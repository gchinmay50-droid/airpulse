"""
Data-quality rules for a raw CPCB reading (per-pollutant SUB-INDICES).

Philosophy: FLAG, don't drop. A flagged value is set to NULL so it can never
poison an average, but the row is kept so the data-health page can count
exactly what was wrong and where. The only rows we refuse are ones with no
usable identity (no station / pollutant / parseable timestamp) - they
cannot be stored, keyed, or deduplicated.

These are pure functions: dict in, dict out, no I/O. That is what makes
them testable in milliseconds without Kafka or Postgres running.
"""

from datetime import datetime

# IMPORTANT: this feed carries CPCB SUB-INDICES, not concentrations.
# Proof (2026-09-16): PM2.5 > PM10 at 19.6% of stations, which is physically
# impossible for concentrations; and max values cap at exactly 500 for two
# different pollutants - the top of CPCB's index scale. The dataset is titled
# "Real time Air Quality Index" for a reason.
# A sub-index is defined on [0, 500]. Anything outside is a fault.
SUB_INDEX_MIN = 0.0
SUB_INDEX_MAX = 500.0

KNOWN_POLLUTANTS = {"PM2.5", "PM10", "NO2", "SO2", "CO", "OZONE", "NH3"}


class Reject(Exception):
    """Row has no usable identity and cannot be stored."""


def _to_float(text):
    """'58' -> 58.0; 'NA' / '' / None / garbage -> None."""
    if text is None:
        return None
    s = str(text).strip()
    if not s or s.upper() == "NA":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def validate(message):
    """Turn one raw Kafka message (already JSON-decoded) into a typed, flagged
    row ready for the readings table. Raises Reject if it has no identity."""
    raw = message.get("raw") or {}
    flags = []

    station = (raw.get("station") or "").strip()
    pollutant = (raw.get("pollutant_id") or "").strip()
    event_time = message.get("event_time")
    if not station or not pollutant or not event_time:
        raise Reject("missing station, pollutant or event_time")
    event_time = datetime.fromisoformat(event_time)

    if pollutant not in KNOWN_POLLUTANTS:
        flags.append("UNKNOWN_POLLUTANT")

    # --- values ---------------------------------------------------------
    raw_avg = raw.get("avg_value")
    mn, mx, avg = _to_float(raw.get("min_value")), _to_float(raw.get("max_value")), _to_float(raw_avg)

    if avg is None:
        # Distinguish "sensor reported nothing" from "sensor reported junk".
        s = str(raw_avg).strip().upper() if raw_avg is not None else ""
        flags.append("NA" if (not s or s == "NA") else "UNPARSEABLE")

    if avg is not None and not (SUB_INDEX_MIN <= avg <= SUB_INDEX_MAX):
        flags.append("OUT_OF_INDEX_RANGE")
        avg = None

    if mn is not None and mx is not None and mn > mx:
        flags.append("MIN_GT_MAX")
        mn = mx = None

    if avg is not None and mn is not None and mx is not None and not (mn <= avg <= mx):
        flags.append("AVG_OUT_OF_RANGE")

    # --- location ------------------------------------------------------
    lat, lon = _to_float(raw.get("latitude")), _to_float(raw.get("longitude"))
    if lat is None or lon is None or not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        flags.append("BAD_COORDINATES")
        lat = lon = None

    return {
        "station": station,
        "pollutant": pollutant,
        "event_time": event_time,
        "city": raw.get("city"),
        "state": raw.get("state"),
        "latitude": lat,
        "longitude": lon,
        "sub_index_min": mn,
        "sub_index_max": mx,
        "sub_index_avg": avg,
        "quality_flags": flags,
        "ingested_at": datetime.fromisoformat(message["ingested_at"]),
    }
