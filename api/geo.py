"""Pure geometry helpers for the "AQI near me" feature. No I/O, fully unit-tested.

The confidence label is the honesty differentiator: a station 40 km away is
NOT "your air". Thresholds follow the CPCB siting guidance that a monitor is
representative of roughly a 2-5 km radius in urban areas.
"""
from __future__ import annotations

import math

EARTH_RADIUS_KM = 6371.0088

# (upper bound in km, label, human explanation)
CONFIDENCE_BANDS = (
    (5.0, "representative", "within 5 km - this station reflects your local air"),
    (25.0, "indicative", "5-25 km away - a regional indication, not your street"),
    (math.inf, "not_covered", "over 25 km away - no monitor covers your location"),
)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two WGS-84 points, in kilometres."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def confidence(distance_km: float) -> tuple[str, str]:
    """Map a distance to (label, explanation)."""
    if distance_km < 0 or math.isnan(distance_km):
        raise ValueError(f"distance must be non-negative, got {distance_km}")
    for upper, label, why in CONFIDENCE_BANDS:
        if distance_km < upper:
            return label, why
    raise AssertionError("unreachable: last band is unbounded")


def validate_coords(lat: float, lon: float) -> None:
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        raise ValueError(f"coordinates out of range: lat={lat} lon={lon}")
