"""
The question OpenAQ failed: is the data.gov.in CPCB feed ACTUALLY current?

We check timestamps against the clock, look at the raw record shape, and
confirm values are concentrations (not pre-computed indices). If this
source lags like OpenAQ did, the project needs a different plan.
"""

import os
import sys
import json
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("DATAGOV_API_KEY")
if not API_KEY:
    sys.exit("No DATAGOV_API_KEY in .env")

RESOURCE = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
URL = f"https://api.data.gov.in/resource/{RESOURCE}"
IST = timezone(timedelta(hours=5, minutes=30))
CACHE = Path("data/cpcb_live.local.json")

# data.gov.in's gateway returns 502 after 60s to the default "python-requests"
# User-Agent. Any other UA works. Identify ourselves honestly.
HEADERS = {"User-Agent": "AirPulse/0.1 (student data-engineering project)"}


# Status codes worth retrying: the server is struggling, not rejecting us.
# 401/403/404 are NOT here on purpose - retrying a bad key changes nothing.
RETRYABLE = {429, 500, 502, 503, 504}


PAGE = 500  # >1000 rows per call hits data.gov.in's ~60s gateway timeout -> 502


def fetch_page(offset, retries=4):
    """One page, with exponential backoff. Never prints the URL (key inside)."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                URL,
                params={"api-key": API_KEY, "format": "json", "limit": PAGE, "offset": offset},
                headers=HEADERS,
                timeout=90,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code not in RETRYABLE:
                sys.exit(f"Non-retryable HTTP {r.status_code}: {r.text[:300]}")
            print(f"   offset={offset} attempt {attempt}: HTTP {r.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"   offset={offset} attempt {attempt}: {type(e).__name__}")
        if attempt < retries:
            time.sleep(2 ** attempt)
    sys.exit("Gave up: data.gov.in unavailable after retries.")


def fetch_all():
    rows, offset, total = [], 0, None
    while True:
        payload = fetch_page(offset)
        batch = payload.get("records", [])
        total = payload.get("total", total)
        rows.extend(batch)
        print(f"offset={offset:<5} +{len(batch):<4} have {len(rows)}/{total}")
        if len(batch) < PAGE:
            return rows, payload
        offset += PAGE


def parse_ts(s):
    """CPCB writes timestamps as 'DD-MM-YYYY HH:MM:SS' in IST."""
    return datetime.strptime(s, "%d-%m-%Y %H:%M:%S").replace(tzinfo=IST)


def main():
    rows, payload = fetch_all()
    CACHE.parent.mkdir(exist_ok=True)
    CACHE.write_text(json.dumps(rows), encoding="utf-8")

    print("\n=== ONE RAW ROW, COMPLETE ===")
    print(json.dumps(rows[0], indent=2))

    print("\n=== FIELDS PRESENT ===")
    print(sorted(rows[0].keys()))

    # --- The freshness test -------------------------------------------------
    now = datetime.now(timezone.utc)
    ages_h = []
    bad_ts = 0
    for row in rows:
        try:
            ages_h.append((now - parse_ts(row["last_update"])).total_seconds() / 3600)
        except Exception:
            bad_ts += 1

    ages_h.sort()
    print(f"\n=== FRESHNESS ({len(ages_h)} rows, {bad_ts} unparseable) ===")
    print(f"now (IST)        : {now.astimezone(IST):%Y-%m-%d %H:%M}")
    print(f"newest reading   : {ages_h[0]:.2f} h old")
    print(f"median reading   : {ages_h[len(ages_h)//2]:.2f} h old")
    print(f"oldest reading   : {ages_h[-1]/24:.1f} days old")
    buckets = Counter(
        "<2h" if a < 2 else "2-6h" if a < 6 else "6-24h" if a < 24 else "1-7d" if a < 168 else ">7d"
        for a in ages_h
    )
    for k in ["<2h", "2-6h", "6-24h", "1-7d", ">7d"]:
        n = buckets.get(k, 0)
        print(f"  {k:<6} {n:>5}  ({n/len(ages_h)*100:4.1f}%)")

    # --- What's in it -------------------------------------------------------
    stations = {(r.get("station"), r.get("city")) for r in rows}
    print(f"\nunique stations  : {len(stations)}")
    print(f"pollutants       : {Counter(r.get('pollutant_id') for r in rows)}")
    with_geo = sum(1 for r in rows if r.get("latitude") and r.get("longitude"))
    print(f"rows with lat/lon: {with_geo}/{len(rows)}")

    # NA values are how CPCB signals a sensor that reported nothing.
    na = sum(1 for r in rows if str(r.get("avg_value")).strip().upper() in ("NA", "", "NONE"))
    print(f"rows with NA avg : {na}/{len(rows)}  ({na/len(rows)*100:.1f}%)")

    mumbai = sorted({r["station"] for r in rows if "mumbai" in str(r.get("city", "")).lower()})
    print(f"\nMumbai stations ({len(mumbai)}):")
    for s in mumbai:
        print(f"  {s}")


if __name__ == "__main__":
    main()
