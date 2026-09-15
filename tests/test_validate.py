"""
Each test is one rule, named for the behaviour it pins down. If a future
change breaks a rule, the failing test name tells you which one.
"""

import pytest

from src.validate import validate, Reject


def msg(**raw_overrides):
    """A known-good message; tests override only the field they care about."""
    raw = {
        "station": "Chembur, Mumbai - MPCB", "pollutant_id": "PM2.5",
        "city": "Mumbai", "state": "Maharashtra",
        "latitude": "19.0364585", "longitude": "72.8954371",
        "min_value": "20", "max_value": "60", "avg_value": "40",
        "last_update": "16-09-2026 00:00:00",
    }
    raw.update(raw_overrides)
    return {
        "schema_version": 1, "source": "test",
        "ingested_at": "2026-09-15T19:23:48+00:00",
        "event_time": "2026-09-15T18:30:00+00:00",
        "raw": raw,
    }


def test_clean_row_has_no_flags_and_typed_values():
    row = validate(msg())
    assert row["quality_flags"] == []
    assert row["sub_index_avg"] == 40.0
    assert row["latitude"] == pytest.approx(19.0364585)
    assert row["event_time"].isoformat() == "2026-09-15T18:30:00+00:00"


def test_na_becomes_null_with_NA_flag():
    row = validate(msg(avg_value="NA"))
    assert row["sub_index_avg"] is None
    assert row["quality_flags"] == ["NA"]


def test_garbage_value_is_flagged_unparseable_not_na():
    row = validate(msg(avg_value="12abc"))
    assert row["sub_index_avg"] is None
    assert row["quality_flags"] == ["UNPARSEABLE"]


def test_negative_is_nulled_and_flagged():
    row = validate(msg(avg_value="-5", min_value="-5", max_value="0"))
    assert row["sub_index_avg"] is None
    assert "OUT_OF_INDEX_RANGE" in row["quality_flags"]


def test_above_500_is_nulled_and_flagged():
    row = validate(msg(avg_value="612", max_value="612"))
    assert row["sub_index_avg"] is None
    assert "OUT_OF_INDEX_RANGE" in row["quality_flags"]


def test_index_of_exactly_500_is_valid():
    # 500 is the top of CPCB's scale and DOES occur in the feed. Must be kept.
    row = validate(msg(avg_value="500", max_value="500"))
    assert row["sub_index_avg"] == 500.0
    assert row["quality_flags"] == []


def test_high_co_index_is_valid():
    # Regression: CO=193 was wrongly flagged when we assumed mg/m3 concentrations.
    row = validate(msg(pollutant_id="CO", avg_value="193", min_value="193", max_value="193"))
    assert row["sub_index_avg"] == 193.0
    assert row["quality_flags"] == []


def test_min_greater_than_max_nulls_both():
    row = validate(msg(min_value="80", max_value="20"))
    assert row["sub_index_min"] is None and row["sub_index_max"] is None
    assert "MIN_GT_MAX" in row["quality_flags"]


def test_avg_outside_min_max_is_flagged_but_kept():
    row = validate(msg(min_value="50", max_value="60", avg_value="40"))
    assert row["sub_index_avg"] == 40.0
    assert "AVG_OUT_OF_RANGE" in row["quality_flags"]


def test_bad_coordinates_are_nulled_and_flagged():
    for lat, lon in [("NA", "72.9"), ("19.0", "abc"), ("95", "72.9"), ("19.0", "200")]:
        row = validate(msg(latitude=lat, longitude=lon))
        assert row["latitude"] is None and row["longitude"] is None
        assert "BAD_COORDINATES" in row["quality_flags"]


def test_unknown_pollutant_is_flagged_not_rejected():
    row = validate(msg(pollutant_id="BENZENE"))
    assert row["pollutant"] == "BENZENE"
    assert "UNKNOWN_POLLUTANT" in row["quality_flags"]


@pytest.mark.parametrize("field", ["station", "pollutant_id"])
def test_missing_identity_field_is_rejected(field):
    with pytest.raises(Reject):
        validate(msg(**{field: ""}))


def test_missing_event_time_is_rejected():
    m = msg()
    m["event_time"] = None
    with pytest.raises(Reject):
        validate(m)


def test_multiple_problems_all_reported():
    row = validate(msg(avg_value="-1", min_value="10", max_value="5", latitude="NA"))
    assert set(row["quality_flags"]) >= {"OUT_OF_INDEX_RANGE", "MIN_GT_MAX", "BAD_COORDINATES"}
