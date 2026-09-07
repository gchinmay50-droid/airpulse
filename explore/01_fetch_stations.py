"""
Goal: find out what OpenAQ ACTUALLY returns for Indian monitoring stations.

This is an exploration script, not pipeline code. Its whole job is to print
the real shape of the data so we design against reality instead of guesses.
It gets deleted (or moved to docs) once we know what we're dealing with.
"""

import os
import sys
import json

import requests
from dotenv import load_dotenv

# Reads the .env file sitting next to this project and loads the values
# in it as environment variables. Keeps the secret out of the source code.
load_dotenv()

API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    sys.exit(
        "No OPENAQ_API_KEY found.\n"
        "Fix: copy .env.example to .env and paste your key into it."
    )

BASE_URL = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}


def fetch(path, params=None):
    """One place where every HTTP call goes through.

    Centralising this means the timeout, the auth header and the error
    printing are defined once instead of copy-pasted at each call site.
    """
    response = requests.get(
        BASE_URL + path,
        headers=HEADERS,
        params=params,
        timeout=30,  # never let a request hang forever
    )

    print(f"GET {response.url}  ->  HTTP {response.status_code}")

    if response.status_code != 200:
        # Print the body before raising: the error message from the API is
        # usually the fastest way to understand what we got wrong.
        print("--- response body ---")
        print(response.text[:1500])
        response.raise_for_status()

    return response.json()


def main():
    # iso=IN filters to India. limit=5 keeps the first look small and readable.
    payload = fetch("/locations", {"iso": "IN", "limit": 5})

    # OpenAQ v3 wraps everything as {"meta": {...}, "results": [...]}.
    meta = payload.get("meta", {})
    results = payload.get("results", [])

    print(f"\nmeta: {json.dumps(meta, indent=2)}")
    print(f"stations returned: {len(results)}")

    if not results:
        print("\nNo results. The 'iso' filter may be wrong for v3 -- we adapt.")
        return

    # Print ONE station in full. This is the important part: we need to see
    # every field, especially whether coordinates and raw concentrations
    # (ug/m3) are present, because both differentiators depend on them.
    print("\n=== FIRST STATION, COMPLETE ===")
    print(json.dumps(results[0], indent=2)[:4000])

    # Then a compact summary of the rest, to sanity-check we got real Indian sites.
    print("\n=== SUMMARY ===")
    for station in results:
        coords = station.get("coordinates") or {}
        sensors = station.get("sensors") or []
        parameters = [s.get("parameter", {}).get("name") for s in sensors]
        print(
            f"  id={station.get('id')} "
            f"name={station.get('name')!r} "
            f"lat={coords.get('latitude')} lon={coords.get('longitude')} "
            f"params={parameters}"
        )


if __name__ == "__main__":
    main()
