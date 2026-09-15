"""
How many Indian stations are there, how many are actually ALIVE, and how many
could produce a valid CPCB AQI at all?

These three numbers decide the whole design, so we measure instead of assuming.
Raw response is cached to data/ so we stop hammering the API while iterating.
"""

import os
import sys
import json
from collections import Counter
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    sys.exit("No OPENAQ_API_KEY found. Copy .env.example to .env first.")

BASE_URL = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}
CACHE = Path("data/stations_in.local.json")

# The 8 pollutants CPCB's National AQI is defined over. Everything else the
# station reports (temperature, wind, NO, NOx) is context, not AQI input.
CPCB_POLLUTANTS = {"pm25", "pm10", "no2", "so2", "co", "o3", "nh3", "pb"}


def fetch_all_indian_stations():
    """Page through every Indian location. OpenAQ caps limit at 1000/page."""
    stations, page = [], 1
    while True:
        response = requests.get(
            BASE_URL + "/locations",
            headers=HEADERS,
            params={"iso": "IN", "limit": 1000, "page": page},
            timeout=60,
        )
        response.raise_for_status()
        batch = response.json().get("results", [])
        stations.extend(batch)
        print(f"  page {page}: +{len(batch)} (total {len(stations)})")
        # A short page means we've reached the end.
        if len(batch) < 1000:
            return stations
        page += 1


def main():
    if CACHE.exists():
        print(f"Using cached {CACHE}")
        stations = json.loads(CACHE.read_text(encoding="utf-8"))
    else:
        print("Fetching all Indian stations...")
        stations = fetch_all_indian_stations()
        CACHE.parent.mkdir(exist_ok=True)
        CACHE.write_text(json.dumps(stations), encoding="utf-8")
        print(f"Cached to {CACHE}")

    print(f"\n{'='*55}\nTOTAL INDIAN STATIONS: {len(stations)}\n{'='*55}")

    with_coords = 0
    with_last_seen = 0
    aqi_capable = 0
    provider_counts = Counter()
    pollutant_counts = Counter()
    duplicate_sensor_stations = 0

    for station in stations:
        coords = station.get("coordinates") or {}
        if coords.get("latitude") is not None and coords.get("longitude") is not None:
            with_coords += 1

        if station.get("datetimeLast"):
            with_last_seen += 1

        provider_counts[(station.get("provider") or {}).get("name", "?")] += 1

        # Which AQI pollutants does this station measure?
        names = [
            (s.get("parameter") or {}).get("name")
            for s in (station.get("sensors") or [])
        ]
        aqi_names = [n for n in names if n in CPCB_POLLUTANTS]
        unique_aqi = set(aqi_names)

        # Same pollutant reported by more than one sensor at this station.
        if len(aqi_names) > len(unique_aqi):
            duplicate_sensor_stations += 1

        for name in unique_aqi:
            pollutant_counts[name] += 1

        # CPCB rule: need >=3 pollutants, one of which is PM2.5 or PM10.
        if len(unique_aqi) >= 3 and ({"pm25", "pm10"} & unique_aqi):
            aqi_capable += 1

    def pct(n):
        return f"{n:>5} ({n / len(stations) * 100:5.1f}%)"

    print(f"\nhas coordinates          : {pct(with_coords)}")
    print(f"has a 'last seen' time   : {pct(with_last_seen)}")
    print(f"CPCB-AQI capable         : {pct(aqi_capable)}   <-- 3+ pollutants incl PM")
    print(f"has duplicate sensors    : {pct(duplicate_sensor_stations)}")

    print("\nStations measuring each AQI pollutant:")
    for name in sorted(CPCB_POLLUTANTS):
        print(f"  {name:<6} {pollutant_counts.get(name, 0):>5}")

    print("\nTop data providers:")
    for name, count in provider_counts.most_common(8):
        print(f"  {count:>5}  {name}")


if __name__ == "__main__":
    main()
