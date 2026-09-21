"""Copy the GOLD tables to a cloud Postgres so the public dashboard can read
them without the whole Flink stack running in the cloud (where it has no
free tier). Upsert-only, incremental on updated_at, loops forever.

Run:  python -m src.publish_gold            (needs CLOUD_DATABASE_URL in .env)
      docker compose --profile cloud up -d  (same thing, as a container)
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql

log = logging.getLogger("publish")

# table -> primary key columns. updated_at is the incremental cursor on all.
TABLES = {
    "station_aqi_hourly": ("station", "hour_ts"),
    "station_completeness_24h": ("station", "window_end"),
    "station_pollutant_jumps": ("station", "pollutant", "hour_ts"),
    "station_pollutant_stuck": ("station", "pollutant", "hour_ts"),
}
DDL_FILES = ("sql/002_station_aqi.sql", "sql/003_data_quality.sql", "sql/004_updated_at.sql")


def ensure_schema(cloud):
    """Run the gold DDL idempotently on the cloud DB."""
    root = Path(__file__).resolve().parent.parent
    for f in DDL_FILES:
        ddl = (root / f).read_text()
        ddl = re.sub(r"CREATE TABLE (\w+)", r"CREATE TABLE IF NOT EXISTS \1", ddl)
        ddl = re.sub(r"CREATE INDEX (\w+)", r"CREATE INDEX IF NOT EXISTS \1", ddl)
        cloud.execute(ddl)
    cloud.commit()


def sync_table(local, cloud, table, pk, since):
    cols = [r[0] for r in local.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position", (table,))]
    rows = local.execute(
        sql.SQL("SELECT {} FROM {} WHERE updated_at > %s").format(
            sql.SQL(", ").join(map(sql.Identifier, cols)), sql.Identifier(table)), (since,)).fetchall()
    if not rows:
        return 0
    upsert = sql.SQL("INSERT INTO {t} ({c}) VALUES ({v}) ON CONFLICT ({k}) DO UPDATE SET {u}").format(
        t=sql.Identifier(table),
        c=sql.SQL(", ").join(map(sql.Identifier, cols)),
        v=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
        k=sql.SQL(", ").join(map(sql.Identifier, pk)),
        u=sql.SQL(", ").join(sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in cols if c not in pk),
    )
    with cloud.cursor() as cur:
        cur.executemany(upsert, rows)
    cloud.commit()
    return len(rows)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval", type=int, default=300, help="seconds between syncs")
    ap.add_argument("--backfill-days", type=int, default=7, help="how far back the first sync reaches")
    args = ap.parse_args()
    load_dotenv()
    local_url = os.getenv("DATABASE_URL", "postgresql://airpulse:airpulse@localhost:5432/airpulse")
    cloud_url = os.environ["CLOUD_DATABASE_URL"]

    since = datetime.now(timezone.utc) - timedelta(days=args.backfill_days)
    while True:
        started = datetime.now(timezone.utc)
        try:
            with psycopg.connect(local_url) as local, psycopg.connect(cloud_url) as cloud:
                ensure_schema(cloud)
                counts = {t: sync_table(local, cloud, t, pk, since) for t, pk in TABLES.items()}
            log.info("synced since %s: %s", since.strftime("%H:%M:%S"), counts)
            since = started - timedelta(minutes=2)      # overlap: updates are idempotent
        except Exception:
            log.exception("sync failed; will retry")
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
