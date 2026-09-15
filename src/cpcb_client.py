"""
Client for the CPCB real-time AQI feed on data.gov.in.

Everything we learned the hard way in explore/03 lives here, once:
  - default python-requests User-Agent gets 502 after 60s; any other UA works
  - pages > 1000 rows also 502; we page at 500
  - the API is flaky, so retry with exponential backoff on server errors only
  - the API key travels in the URL, so never log a raw URL or exception string
"""

import logging
import time
from datetime import datetime, timezone, timedelta

import requests

log = logging.getLogger(__name__)

RESOURCE_ID = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
HEADERS = {"User-Agent": "AirPulse/0.1 (student data-engineering project)"}
PAGE_SIZE = 500
RETRYABLE = {429, 500, 502, 503, 504}
IST = timezone(timedelta(hours=5, minutes=30))


class CPCBClient:
    def __init__(self, api_key, timeout=90, retries=4):
        if not api_key:
            raise ValueError("CPCBClient needs an api_key")
        self.api_key = api_key
        self.timeout = timeout
        self.retries = retries
        # A Session reuses the TCP connection across pages: faster, and polite.
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_page(self, offset):
        params = {"api-key": self.api_key, "format": "json",
                  "limit": PAGE_SIZE, "offset": offset}
        for attempt in range(1, self.retries + 1):
            try:
                r = self.session.get(URL, params=params, timeout=self.timeout)
                if r.status_code == 200:
                    return r.json()
                if r.status_code not in RETRYABLE:
                    raise RuntimeError(f"CPCB non-retryable HTTP {r.status_code}")
                log.warning("CPCB offset=%s attempt %s: HTTP %s", offset, attempt, r.status_code)
            except requests.exceptions.RequestException as e:
                # type name only - str(e) contains the URL, and the URL contains the key
                log.warning("CPCB offset=%s attempt %s: %s", offset, attempt, type(e).__name__)
            if attempt < self.retries:
                time.sleep(2 ** attempt)
        raise RuntimeError("CPCB feed unavailable after retries")

    def fetch_all(self):
        """Return every row of the feed. One row = one (station, pollutant)."""
        rows, offset = [], 0
        while True:
            payload = self._fetch_page(offset)
            batch = payload.get("records", [])
            rows.extend(batch)
            if len(batch) < PAGE_SIZE:
                log.info("CPCB fetched %s rows (api total=%s)", len(rows), payload.get("total"))
                return rows
            offset += PAGE_SIZE


def parse_event_time(last_update):
    """CPCB writes 'DD-MM-YYYY HH:MM:SS' in IST with no zone marker.
    Return a timezone-aware UTC datetime, or None if unparseable."""
    try:
        return datetime.strptime(last_update, "%d-%m-%Y %H:%M:%S") \
                       .replace(tzinfo=IST).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None
