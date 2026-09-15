"""
AirPulse sink: consume raw readings, validate, write to Postgres.

The delivery-guarantee story, which is the whole point of this file:

  1. Kafka gives AT-LEAST-ONCE. We turn auto-commit OFF and commit offsets
     only after the database transaction succeeds. If we crash between the
     two, Kafka redelivers the batch on restart.
  2. The database write is IDEMPOTENT: the fingerprint is the primary key,
     so a redelivered row hits ON CONFLICT DO NOTHING.
  3. (1) + (2) = effectively exactly-once, with no distributed transaction.

Batching: we poll up to BATCH messages (or BATCH_WAIT seconds), write them
in ONE transaction, then commit offsets ONCE. Per-message commits would be
~100x slower and no safer.
"""

import argparse
import json
import logging
import os
import time

import psycopg
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

from src.validate import validate, Reject

log = logging.getLogger("sink")

TOPIC = "aqi.readings.raw"
GROUP = "airpulse-postgres-sink"
BATCH = 500
BATCH_WAIT = 2.0   # seconds; flush a partial batch after this long

INSERT = """
INSERT INTO readings
  (station, pollutant, event_time, city, state, latitude, longitude,
   sub_index_min, sub_index_max, sub_index_avg, quality_flags, ingested_at)
VALUES
  (%(station)s, %(pollutant)s, %(event_time)s, %(city)s, %(state)s,
   %(latitude)s, %(longitude)s, %(sub_index_min)s, %(sub_index_max)s, %(sub_index_avg)s,
   %(quality_flags)s, %(ingested_at)s)
ON CONFLICT (station, pollutant, event_time) DO NOTHING
"""


def make_consumer(bootstrap):
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",   # first run: start from the beginning of the topic
        "enable.auto.commit": False,       # WE decide when an offset is safe to commit
    })


def write_batch(conn, rows):
    """One transaction for the whole batch. Returns rows actually inserted
    (duplicates are silently skipped by ON CONFLICT and don't count)."""
    with conn.cursor() as cur:
        cur.executemany(INSERT, rows)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def run(consumer, conn, max_batches=None):
    consumer.subscribe([TOPIC])
    batches = 0
    totals = {"consumed": 0, "rejected": 0, "inserted": 0, "flagged": 0}

    while max_batches is None or batches < max_batches:
        rows, started = [], time.monotonic()

        # ---- collect a batch --------------------------------------------
        while len(rows) < BATCH and time.monotonic() - started < BATCH_WAIT:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("consumer error: %s", msg.error())
                continue
            totals["consumed"] += 1
            try:
                row = validate(json.loads(msg.value()))
            except (Reject, ValueError, KeyError) as e:
                totals["rejected"] += 1
                log.warning("rejected p%s o%s: %s", msg.partition(), msg.offset(), e)
                continue
            if row["quality_flags"]:
                totals["flagged"] += 1
            rows.append(row)

        if not rows:
            if max_batches is not None:
                break          # --once mode and the topic is drained
            continue

        # ---- write, THEN commit offsets ---------------------------------
        inserted = write_batch(conn, rows)
        consumer.commit(asynchronous=False)
        totals["inserted"] += inserted
        batches += 1
        log.info("batch %s: rows=%s inserted=%s (dups skipped=%s)",
                 batches, len(rows), inserted, len(rows) - inserted)

    log.info("done: %s", totals)
    return totals


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="drain what's there, then exit")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv()

    consumer = make_consumer(os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"))
    conn = psycopg.connect(os.getenv("DATABASE_URL", "postgresql://airpulse:airpulse@localhost:5432/airpulse"))
    try:
        run(consumer, conn, max_batches=10_000 if args.once else None)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
