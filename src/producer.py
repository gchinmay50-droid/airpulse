"""
AirPulse producer: poll the CPCB feed, publish one message per reading.

Design decisions (each one is an interview question):

  KEY = station name
      Kafka routes a key to a partition by hash, and a partition is strictly
      ordered. So every reading from one station lands on one partition, in
      order. Downstream windowing per station is then safe.

  VALUE = the raw row, untouched, inside an envelope
      This is a RAW ("bronze") topic. We do not clean, cast or drop anything
      here - "NA" stays "NA", numbers stay strings. If the cleaning logic has
      a bug later, the original data is still in the topic to replay from.
      The envelope adds what the source lacks: a UTC event_time, when WE saw
      it (ingested_at), where it came from, and a schema version.

  event_time vs ingested_at
      event_time  = when the sensor measured (from last_update, IST -> UTC)
      ingested_at = when this producer published it
      The gap between them is the pipeline's latency. Windowing must use
      event_time, or a late-arriving reading lands in the wrong hour.

  Dedup on (station, pollutant, last_update)
      CPCB updates hourly; we poll more often. Without this, every reading is
      published several times. In-memory, so it resets on restart - that is
      fine: the processor dedups too. Layered idempotency, not single point.

  enable.idempotence=True, acks=all
      The broker discards producer retries it already stored, and only acks
      once the write is durable. Together: no duplicates from OUR retries,
      no acknowledged-then-lost messages.
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

from src.cpcb_client import CPCBClient, parse_event_time

log = logging.getLogger("producer")

TOPIC = "aqi.readings.raw"
SCHEMA_VERSION = 1


def build_message(row, ingested_at):
    """Raw row + envelope. Returns (key, value_bytes)."""
    event_time = parse_event_time(row.get("last_update"))
    value = {
        "schema_version": SCHEMA_VERSION,
        "source": "cpcb.data.gov.in",
        "ingested_at": ingested_at.isoformat(),
        "event_time": event_time.isoformat() if event_time else None,
        "raw": row,
    }
    return row.get("station", ""), json.dumps(value).encode("utf-8")


def make_producer(bootstrap):
    return Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,            # batch for up to 50ms - fewer, fuller requests
        "compression.type": "lz4",
    })


class Stats:
    def __init__(self):
        self.delivered = self.failed = 0

    def on_delivery(self, err, msg):
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            log.error("delivery failed key=%s: %s", msg.key(), err)


def poll_once(client, producer, seen):
    """One poll cycle: fetch -> dedup -> publish -> flush. Returns counts."""
    ingested_at = datetime.now(timezone.utc)
    rows = client.fetch_all()

    stats = Stats()
    published = skipped = 0
    for row in rows:
        fingerprint = (row.get("station"), row.get("pollutant_id"), row.get("last_update"))
        if fingerprint in seen:
            skipped += 1
            continue
        key, value = build_message(row, ingested_at)
        producer.produce(TOPIC, key=key, value=value, callback=stats.on_delivery)
        seen.add(fingerprint)
        published += 1
        producer.poll(0)   # let librdkafka run delivery callbacks as we go

    producer.flush(30)     # block until every message is acked (or 30s)
    log.info("cycle: fetched=%s published=%s dup-skipped=%s delivered=%s failed=%s",
             len(rows), published, skipped, stats.delivered, stats.failed)
    return published, stats.failed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="one cycle, then exit")
    parser.add_argument("--interval", type=int, default=600, help="seconds between polls")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv()

    client = CPCBClient(os.getenv("DATAGOV_API_KEY"))
    producer = make_producer(os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"))
    seen = set()

    while True:
        try:
            poll_once(client, producer, seen)
        except Exception as e:
            # A bad cycle must not kill the producer. Log it, wait, try again.
            log.error("cycle failed: %s: %s", type(e).__name__, e)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
