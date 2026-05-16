"""
tests.py — Unit tests for the sensor data pipeline.
Run: python -m pytest tests.py -v
"""

import math
import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pipeline import (
    _try_parse_float,
    _try_parse_int,
    _try_parse_date,
    clean_and_validate,
    compute_top_third_average,
    DQStats,
)


# ── Parser tests ──────────────────────────────────────────────────────────────

class TestTryParseFloat:
    def test_valid_float(self):
        assert _try_parse_float("3.14") == pytest.approx(3.14)

    def test_valid_int_string(self):
        assert _try_parse_float("42") == pytest.approx(42.0)

    def test_null_string(self):
        assert _try_parse_float("NULL") is None

    def test_none(self):
        assert _try_parse_float(None) is None

    def test_empty(self):
        assert _try_parse_float("") is None

    def test_alpha(self):
        assert _try_parse_float("ABC") is None

    def test_mixed(self):
        assert _try_parse_float("IIIII") is None

    def test_none_string(self):
        assert _try_parse_float("NONE") is None

    def test_nan_string(self):
        assert _try_parse_float("NAN") is None


class TestTryParseInt:
    def test_valid_int(self):
        assert _try_parse_int("34") == 34

    def test_valid_float_whole(self):
        assert _try_parse_int("34.0") == 34

    def test_float_fractional(self):
        assert _try_parse_int("34.5") is None

    def test_alpha(self):
        assert _try_parse_int("XYZ") is None

    def test_none(self):
        assert _try_parse_int(None) is None

    def test_error_code(self):
        assert _try_parse_int("ERR") is None


class TestTryParseDate:
    def test_full_timestamp(self):
        result = _try_parse_date("2025-06-20 10:39:50.109664")
        assert result is not None
        assert result.startswith("2025-06-20 10:39:50")

    def test_no_microseconds(self):
        result = _try_parse_date("2025-06-20 10:39:50")
        assert result == "2025-06-20 10:39:50"

    def test_hour_minute_only(self):
        result = _try_parse_date("2025-06-24 05:19")
        assert result == "2025-06-24 05:19:00"

    def test_date_only(self):
        result = _try_parse_date("2025-06-24")
        assert result == "2025-06-24 00:00:00"

    def test_invalid(self):
        assert _try_parse_date("NOT-A-DATE") is None

    def test_none(self):
        assert _try_parse_date(None) is None


# ── DQ / clean_and_validate tests ─────────────────────────────────────────────

def make_raw(event_date="2025-06-20 10:39:50.109664",
             sensor_number="34",
             sensor_value="16.59",
             source="test"):
    return {
        "event_date": event_date,
        "sensor_number": sensor_number,
        "sensor_value": sensor_value,
        "_source": source,
    }


class TestCleanAndValidate:
    def test_valid_record(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw()], dq)
        assert len(clean) == 1
        assert clean[0]["sensor_number"] == 34
        assert clean[0]["sensor_value"] == pytest.approx(16.59)

    def test_null_value_dropped(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw(sensor_value="NULL")], dq)
        assert len(clean) == 0
        assert dq.dropped_null_value == 1

    def test_non_numeric_value_dropped(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw(sensor_value="ABC")], dq)
        assert len(clean) == 0
        assert dq.dropped_non_numeric_value == 1

    def test_non_numeric_sensor_number_dropped(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw(sensor_number="XYZ")], dq)
        assert len(clean) == 0
        assert dq.dropped_non_numeric_sensor_number == 1

    def test_sensor_out_of_range_dropped(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw(sensor_number="200")], dq)
        assert len(clean) == 0
        assert dq.dropped_out_of_range_sensor == 1

    def test_sensor_zero_dropped(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw(sensor_number="0")], dq)
        assert len(clean) == 0
        assert dq.dropped_out_of_range_sensor == 1

    def test_bad_date_dropped(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw(event_date="NOT-A-DATE")], dq)
        assert len(clean) == 0
        assert dq.dropped_bad_date == 1

    def test_truncated_timestamp_ok(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw(event_date="2025-06-24 05:19")], dq)
        assert len(clean) == 1

    def test_iiiii_value_dropped(self):
        dq = DQStats()
        clean = clean_and_validate([make_raw(sensor_value="IIIII")], dq)
        assert len(clean) == 0
        assert dq.dropped_non_numeric_value == 1

    def test_multiple_mixed_records(self):
        dq = DQStats()
        records = [
            make_raw(),                         # valid
            make_raw(sensor_value="NULL"),       # dropped – null value
            make_raw(sensor_number="XYZ"),       # dropped – bad sensor#
            make_raw(sensor_value="99.9", sensor_number="100"),  # valid
        ]
        clean = clean_and_validate(records, dq)
        assert len(clean) == 2
        assert dq.dropped_null_value == 1
        assert dq.dropped_non_numeric_sensor_number == 1


# ── Top-1/3 average tests ─────────────────────────────────────────────────────

class TestComputeTopThirdAverage:
    def test_empty(self):
        result = compute_top_third_average([])
        assert result["average"] is None

    def test_single_record(self):
        records = [{"sensor_value": 50.0}]
        result = compute_top_third_average(records)
        assert result["top_third_count"] == 1
        assert result["average"] == pytest.approx(50.0)

    def test_three_records(self):
        # Top 1/3 of 3 = ceil(1) = 1 → max value
        records = [
            {"sensor_value": 10.0},
            {"sensor_value": 50.0},
            {"sensor_value": 90.0},
        ]
        result = compute_top_third_average(records)
        assert result["total_count"] == 3
        assert result["top_third_count"] == 1
        assert result["average"] == pytest.approx(90.0)

    def test_six_records(self):
        # Top 1/3 of 6 = 2 → average of 2 highest
        records = [{"sensor_value": float(v)} for v in [10, 20, 30, 40, 50, 60]]
        result = compute_top_third_average(records)
        assert result["top_third_count"] == 2
        assert result["average"] == pytest.approx(55.0)  # (60+50)/2

    def test_four_records_ceiling(self):
        # ceil(4/3) = 2
        records = [{"sensor_value": float(v)} for v in [10, 20, 80, 100]]
        result = compute_top_third_average(records)
        assert result["top_third_count"] == 2
        assert result["average"] == pytest.approx(90.0)  # (100+80)/2

    def test_known_dataset(self):
        # 9 records → top 3
        vals = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        records = [{"sensor_value": float(v)} for v in vals]
        result = compute_top_third_average(records)
        assert result["top_third_count"] == 3
        assert result["average"] == pytest.approx(8.0)   # (9+8+7)/3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
