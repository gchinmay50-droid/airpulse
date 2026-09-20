"""AirPulse read API. Thin by design: it only reads the GOLD tables that Flink
writes (station_aqi_hourly, station_completeness_24h). No AQI maths lives here.

Run:  uvicorn api.main:app --reload --port 8000
Docs: http://localhost:8000/docs
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row

from api.geo import confidence, haversine_km, validate_coords

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airpulse:airpulse@localhost:5432/airpulse")

app = FastAPI(title="AirPulse", version="0.1.0",
              description="Per-station AQI for India, computed with CPCB's sufficiency rules.")


@contextmanager
def db():
    # One short-lived connection per request is plenty at this traffic; a pool
    # is a one-line swap (psycopg_pool) if it ever is not.
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        yield conn


# Latest gold row per station, with the most recent completeness verdict.
# DISTINCT ON is the idiomatic Postgres "top-1 per group".
LATEST_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (station) *
    FROM station_aqi_hourly
    ORDER BY station, hour_ts DESC
),
completeness AS (
    SELECT DISTINCT ON (station) station, hours_reported, meets_16h_rule
    FROM station_completeness_24h
    ORDER BY station, window_end DESC
)
SELECT l.station, l.city, l.state, l.latitude, l.longitude, l.hour_ts,
       l.aqi, l.category, l.dominant, l.pollutants_reporting, l.insufficient_reason,
       l.pm25, l.pm10, l.no2, l.so2, l.co, l.o3, l.nh3,
       c.hours_reported, c.meets_16h_rule
FROM latest l LEFT JOIN completeness c USING (station)
"""


@app.get("/api/health")
def health():
    """Freshness indicator: how stale is the newest hour in the gold layer?"""
    with db() as conn:
        row = conn.execute("""
            SELECT max(hour_ts) AS latest_hour, count(DISTINCT station) AS stations,
                   count(*) FILTER (WHERE aqi IS NOT NULL) AS with_aqi
            FROM station_aqi_hourly
            WHERE hour_ts = (SELECT max(hour_ts) FROM station_aqi_hourly)
        """).fetchone()
    latest = row["latest_hour"]
    age_min = None if latest is None else (datetime.now(timezone.utc) - latest).total_seconds() / 60
    return {
        "latest_hour": latest,
        "age_minutes": None if age_min is None else round(age_min),
        # CPCB publishes ~1h late and we poll every 10 min: <2h is normal.
        "status": "unknown" if age_min is None else ("fresh" if age_min < 120 else "stale"),
        "stations": row["stations"],
        "stations_with_aqi": row["with_aqi"],
    }


@app.get("/api/stations/latest")
def stations_latest(city: str | None = Query(None, description="case-insensitive filter")):
    """Every station's most recent AQI plus whether the 16-of-24-hours rule holds."""
    sql, params = LATEST_SQL, []
    if city:
        sql += " WHERE l.city ILIKE %s"
        params.append(city)
    sql += " ORDER BY l.aqi DESC NULLS LAST, l.station"
    with db() as conn:
        return conn.execute(sql, params).fetchall()


@app.get("/api/stations/{station}/history")
def station_history(station: str, hours: int = Query(48, ge=1, le=24 * 14)):
    with db() as conn:
        rows = conn.execute("""
            SELECT hour_ts, aqi, category, dominant, pollutants_reporting, insufficient_reason,
                   pm25, pm10, no2, so2, co, o3, nh3
            FROM station_aqi_hourly
            WHERE station = %s AND hour_ts > now() - make_interval(hours => %s)
            ORDER BY hour_ts
        """, (station, hours)).fetchall()
    if not rows:
        raise HTTPException(404, f"no data for station {station!r} in the last {hours}h")
    return {"station": station, "hours": hours, "rows": rows}


@app.get("/api/nearest")
def nearest(lat: float, lon: float, limit: int = Query(3, ge=1, le=10)):
    """Nearest stations to a point, each with distance and an honest confidence label.

    Only stations with a reading in the last 3 hours are candidates: a dead
    station 500 m away must never beat a live one 6 km away.
    """
    try:
        validate_coords(lat, lon)
    except ValueError as e:
        raise HTTPException(422, str(e))
    with db() as conn:
        rows = conn.execute(
            LATEST_SQL + """
            WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
              AND l.hour_ts > now() - interval '3 hours'
            """).fetchall()
    for r in rows:
        r["distance_km"] = round(haversine_km(lat, lon, r["latitude"], r["longitude"]), 2)
        r["confidence"], r["confidence_note"] = confidence(r["distance_km"])
    rows.sort(key=lambda r: r["distance_km"])
    return {"query": {"lat": lat, "lon": lon}, "stations": rows[:limit]}


# The PWA frontend. Mounted last so /api/* routes win.
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
