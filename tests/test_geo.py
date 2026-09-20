import math

import pytest

from api.geo import confidence, haversine_km, validate_coords

# Reference points (Google Maps): Chembur station vs Bandra station, Mumbai.
CHEMBUR = (19.0522, 72.9005)
BANDRA = (19.0596, 72.8295)


def test_haversine_zero_for_same_point():
    assert haversine_km(*CHEMBUR, *CHEMBUR) == 0.0


def test_haversine_known_distance():
    # ~7.5 km across Mumbai; tolerance covers ellipsoid vs sphere.
    assert haversine_km(*CHEMBUR, *BANDRA) == pytest.approx(7.5, abs=0.2)


def test_haversine_is_symmetric():
    assert haversine_km(*CHEMBUR, *BANDRA) == pytest.approx(haversine_km(*BANDRA, *CHEMBUR))


def test_haversine_mumbai_to_delhi():
    # Mumbai (18.97, 72.83) to Delhi (28.61, 77.21): published ~1150 km.
    assert haversine_km(18.97, 72.83, 28.61, 77.21) == pytest.approx(1150, abs=15)


@pytest.mark.parametrize(
    "km,label",
    [(0, "representative"), (4.99, "representative"), (5.0, "indicative"),
     (24.99, "indicative"), (25.0, "not_covered"), (400, "not_covered")],
)
def test_confidence_bands(km, label):
    assert confidence(km)[0] == label


def test_confidence_rejects_negative_and_nan():
    with pytest.raises(ValueError):
        confidence(-1)
    with pytest.raises(ValueError):
        confidence(math.nan)


@pytest.mark.parametrize("lat,lon", [(91, 0), (-91, 0), (0, 181), (0, -181)])
def test_validate_coords_rejects_out_of_range(lat, lon):
    with pytest.raises(ValueError):
        validate_coords(lat, lon)


def test_validate_coords_accepts_india():
    validate_coords(19.07, 72.87)
