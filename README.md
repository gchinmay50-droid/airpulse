# AirPulse — real-time air quality intelligence for India

![CI](https://github.com/gchinmay50-droid/airpulse/actions/workflows/ci.yml/badge.svg)

A streaming data pipeline that ingests India's live air-quality feed (CPCB via
data.gov.in), processes it with **Apache Flink**, and computes a *trustworthy*
per-station AQI — one that says "insufficient data" instead of inventing a number.

```
CPCB feed ──▶ producer ──▶ Redpanda (Kafka) ──▶ Flink SQL ──▶ Postgres (gold) ──▶ dashboard
 (hourly)     (Python)     aqi.readings.raw      2 jobs        station_aqi_hourly    FastAPI + PWA
                                │                              station_completeness_24h
                                └──▶ validation sink ──▶ Postgres (bronze: readings)
```

## Why this exists

Every AQI app in India shows you a number. Almost none of them tell you:

- **whether that number is valid.** CPCB's National AQI rules require at least
  3 pollutants (one of them PM2.5 or PM10) and 16 of the last 24 hours of data.
  The public feed publishes sub-indices with no station-level AQI and no
  indication of whether those rules hold. AirPulse applies the rules and
  reports *why* when they fail.
- **how far away the station is.** Only ~12% of Indian cities have a monitor.
  The dashboard shows distance and a confidence label rather
  than presenting a station 40 km away as "your air".

## What is running

| Stage | Component | Detail |
|---|---|---|
| Ingest | `src/producer.py` | Polls data.gov.in every 10 min, one Kafka message per (station, pollutant). Idempotent: fingerprint dedup on `(station, pollutant, last_update)`. |
| Bronze | `src/sink.py` + `src/validate.py` | Consumer group with manual commits after each DB transaction. Pure-function validation rules (range 0–500, NA handling, junk rejection) covered by 15 tests. |
| Gold | `flink/sql/station_aqi.sql` | **Job 1** — continuous `GROUP BY (station, hour)` upserted to Postgres; applies the sufficiency rules and records `insufficient_reason`. **Job 2** — 24 h event-time hopping window (slides hourly, 30-min watermark) computing per-station reporting completeness against the 16-hour rule. |
| Serve | `api/main.py` + `api/static/` | FastAPI reading only the gold tables: `/api/health` (freshness), `/api/stations/latest`, `/api/nearest?lat&lon` (live stations only, with distance + confidence label). A dependency-free PWA frontend on top. |
| CI | `.github/workflows/ci.yml` | pytest + `docker compose config` on every push. |

Two Flink jobs use two deliberately different patterns: the headline AQI needs
low latency, so it gets a windowless continuous aggregation where a late reading
is just another update. Completeness is a health metric where being an hour
late is fine, so it gets the windowed pattern that *waits for the watermark*
before emitting — correctness over speed.

## Findings from the data (measured, not assumed)

- The CPCB feed is a **mutating snapshot**: ~3,400 fixed (station, pollutant)
  slots overwritten hourly with no history. Anything overwritten between polls is
  gone from the source forever, so the poll interval is a data-loss boundary.
- Offset pagination over that snapshot **leaks**: 24–45 duplicates and as many
  misses per fetch. Dedup plus frequent polling self-heals across cycles.
- The feed gives **sub-indices (0–500), not concentrations** — PM2.5 exceeds
  PM10 at ~20% of stations, which is impossible for concentrations.
- First Flink run: 490 stations, 442 valid AQIs, 27 rejected for fewer than
  3 pollutants, 21 for having no PM sub-index. A naive `MAX()` would have
  shown a number for all 48.

## Run it

Requirements: Docker Desktop, Python 3.13, a free [data.gov.in API key](https://data.gov.in/).

```bash
cp .env.example .env               # paste your DATAGOV_API_KEY
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt
sh flink/download-jars.sh          # Kafka + JDBC connectors (not committed)
docker compose up -d --build       # Redpanda, Console, Postgres, Flink

# schema
docker exec -i airpulse-postgres psql -U airpulse -d airpulse < sql/001_readings.sql
docker exec -i airpulse-postgres psql -U airpulse -d airpulse < sql/002_station_aqi.sql

# Flink jobs (MSYS_NO_PATHCONV stops Git Bash mangling the container path)
MSYS_NO_PATHCONV=1 docker exec airpulse-jobmanager \
  ./bin/sql-client.sh -f /opt/flink/sql/station_aqi.sql

# ingest
python -m src.producer             # loops every 10 min; --once for a single cycle
python -m src.sink                 # validation sink
python -m uvicorn api.main:app --port 8010   # API + dashboard
```

| UI | URL |
|---|---|
| Redpanda Console | http://localhost:8080 |
| Flink dashboard | http://localhost:8081 |
| AirPulse dashboard + API docs | http://localhost:8010 · http://localhost:8010/docs |
| Postgres | `localhost:5432`, `airpulse`/`airpulse` |

```bash
python -m pytest tests/ -v
```

## Roadmap

- [x] Ingest + Kafka + validation sink + tests + CI
- [x] Flink: station AQI with sufficiency rules; 24 h completeness windows
- [ ] Data-quality layer: stuck sensors, dark stations, implausible jumps
- [x] Dashboard (FastAPI + PWA): city view, "AQI near me" with distance/confidence
- [ ] Cross-source validation against OpenAQ concentrations
- [ ] GCP port: Pub/Sub + Cloud Run + BigQuery
