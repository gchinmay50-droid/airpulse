"""Replay load generator for the throughput benchmark.

The live CPCB feed is ~0.2 events/sec, which proves nothing about the
pipeline. This synthesises N hours of readings for every real station
(same envelope, same key, same topic layout) and publishes them as fast as
Redpanda accepts, so Flink can be measured at hundreds of times live rate.

Run:  python -m src.loadgen --hours 72 --topic aqi.readings.bench
Then: sh bench/run.sh   (submits the bench Flink job and times it)
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg
from dotenv import load_dotenv

from src.producer import make_producer

log = logging.getLogger("loadgen")
POLLUTANTS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "OZONE", "NH3"]


def stations(conn):
    """Real station identities from the gold table, so the benchmark keys
    partition exactly like production traffic."""
    return conn.execute("""
        SELECT DISTINCT ON (station) station, city, state, latitude, longitude
        FROM station_aqi_hourly WHERE latitude IS NOT NULL ORDER BY station, hour_ts DESC
    """).fetchall()


def synth_rows(stns, hours, start, rng):
    """Yield raw rows shaped exactly like data.gov.in's, hour by hour.

    Each (station, pollutant) is a slow random walk in sub-index space with a
    diurnal bump, so the aggregation has realistic variety; a few slots are
    deliberately frozen or spiked so the quality jobs have something to find.
    """
    level = {(s[0], p): rng.uniform(15, 180) for s in stns for p in POLLUTANTS}
    frozen = set(rng.sample(list(level), k=max(1, len(level) // 50)))     # 2% stuck
    for h in range(hours):
        ts = start + timedelta(hours=h)
        last_update = (ts + timedelta(hours=5, minutes=30)).strftime("%d-%m-%Y %H:%M:%S")  # IST
        diurnal = 1 + 0.25 * math.sin((h % 24) / 24 * 2 * math.pi)
        for s in stns:
            for p in POLLUTANTS:
                k = (s[0], p)
                if k not in frozen:
                    level[k] = min(500, max(0, level[k] + rng.gauss(0, 6)))
                v = round(level[k] * diurnal)
                if rng.random() < 0.0005:      # rare implausible spike
                    v = min(500, v + 220)
                if rng.random() < 0.03:        # NA rate seen in the real feed
                    v = "NA"
                yield {
                    "station": s[0], "city": s[1], "state": s[2],
                    "latitude": str(s[3]), "longitude": str(s[4]),
                    "pollutant_id": p, "last_update": last_update,
                    "min_value": str(v), "max_value": str(v), "avg_value": str(v),
                }, ts


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=72)
    ap.add_argument("--topic", default="aqi.readings.bench")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    load_dotenv()

    with psycopg.connect(os.getenv("DATABASE_URL", "postgresql://airpulse:airpulse@localhost:5432/airpulse")) as conn:
        stns = stations(conn)
    producer = make_producer(os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"))
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)   # far from live data: no key collisions
    rng = random.Random(args.seed)

    n, nbytes, t0 = 0, 0, time.perf_counter()
    for row, ts in synth_rows(stns, args.hours, start, rng):
        value = json.dumps({
            "schema_version": 1, "source": "loadgen",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "event_time": ts.isoformat(), "raw": row,
        }).encode()
        while True:
            try:
                producer.produce(args.topic, key=row["station"], value=value)
                break
            except BufferError:            # local queue full: let it drain
                producer.poll(0.01)
        n += 1; nbytes += len(value)
        if n % 50_000 == 0:
            producer.poll(0)
            log.info("published %d", n)
    producer.flush()
    dt = time.perf_counter() - t0
    result = {"messages": n, "stations": len(stns), "hours": args.hours, "seconds": round(dt, 2),
              "msgs_per_sec": round(n / dt), "mb_per_sec": round(nbytes / dt / 1e6, 1)}
    log.info("done %s", result)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
