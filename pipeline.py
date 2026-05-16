"""
Sensor Data Pipeline
====================
Ingests data from two source dataset folders (CSV and JSON),
combines them, handles data quality issues, and outputs the
average of the top 1/3 sensor values across the combined dataset.

Designed to scale to 500GB+ via PySpark (see spark_pipeline.py).
"""

import os
import sys
import json
import logging
import argparse
import math
from datetime import datetime
from pathlib import Path
import csv

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


# ── Data Quality Constants ────────────────────────────────────────────────────
VALID_SENSOR_NUMBER_MIN = 1
VALID_SENSOR_NUMBER_MAX = 100


# ── Ingestion ─────────────────────────────────────────────────────────────────

def ingest_csv_files(root_dir: str) -> list[dict]:
    """
    Walk data_source_1/<partition>/*.csv and ingest all CSV files.
    Returns a list of raw record dicts with a 'source' tag.
    """
    records = []
    source_path = Path(root_dir) / "data_source_1"
    if not source_path.exists():
        log.warning(f"data_source_1 not found at {source_path}")
        return records

    csv_files = sorted(source_path.rglob("*.csv"))
    log.info(f"Found {len(csv_files)} CSV file(s) in data_source_1")

    for fpath in csv_files:
        log.info(f"  Reading CSV: {fpath}")
        with open(fpath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["_source"] = "data_source_1"
                row["_file"] = str(fpath)
                records.append(dict(row))

    log.info(f"Ingested {len(records)} raw rows from data_source_1 (CSV)")
    return records


def ingest_json_files(root_dir: str) -> list[dict]:
    """
    Walk data_source_2/<partition>/*.json and ingest all JSON files.
    Returns a list of raw record dicts with a 'source' tag.
    """
    records = []
    source_path = Path(root_dir) / "data_source_2"
    if not source_path.exists():
        log.warning(f"data_source_2 not found at {source_path}")
        return records

    json_files = sorted(source_path.rglob("*.json"))
    log.info(f"Found {len(json_files)} JSON file(s) in data_source_2")

    for fpath in json_files:
        log.info(f"  Reading JSON: {fpath}")
        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            log.warning(f"  Skipping {fpath}: expected JSON array, got {type(data)}")
            continue
        for rec in data:
            rec["_source"] = "data_source_2"
            rec["_file"] = str(fpath)
            records.append(dict(rec))

    log.info(f"Ingested {len(records)} raw rows from data_source_2 (JSON)")
    return records


# ── Cleaning / DQ ─────────────────────────────────────────────────────────────

class DQStats:
    def __init__(self):
        self.total_raw = 0
        self.dropped_null_value = 0
        self.dropped_non_numeric_value = 0
        self.dropped_null_sensor_number = 0
        self.dropped_non_numeric_sensor_number = 0
        self.dropped_out_of_range_sensor = 0
        self.dropped_bad_date = 0
        self.clean = 0

    def summary(self) -> str:
        return (
            f"\nData Quality Summary\n"
            f"{'='*40}\n"
            f"  Raw records ingested  : {self.total_raw}\n"
            f"  Dropped – null value  : {self.dropped_null_value}\n"
            f"  Dropped – bad value   : {self.dropped_non_numeric_value}\n"
            f"  Dropped – null sensor#: {self.dropped_null_sensor_number}\n"
            f"  Dropped – bad sensor# : {self.dropped_non_numeric_sensor_number}\n"
            f"  Dropped – sensor OOR  : {self.dropped_out_of_range_sensor}\n"
            f"  Dropped – bad date    : {self.dropped_bad_date}\n"
            f"  Clean records kept    : {self.clean}\n"
            f"{'='*40}"
        )


def _try_parse_float(val) -> float | None:
    """Return float if parseable, None otherwise."""
    if val is None:
        return None
    try:
        result = float(str(val).strip())
        if math.isnan(result) or math.isinf(result):
            return None
        return result
    except (ValueError, TypeError):
        return None


def _try_parse_int(val) -> int | None:
    """Return int if parseable from a numeric string, None otherwise."""
    if val is None:
        return None
    try:
        s = str(val).strip()
        # Reject strings that have non-digit/non-period chars
        f = float(s)
        if f != int(f):
            return None          # e.g. 1.5 is not a valid sensor number
        return int(f)
    except (ValueError, TypeError):
        return None


def _try_parse_date(val) -> str | None:
    """Normalise various timestamp formats to ISO string; None if unparseable."""
    if val is None:
        return None
    s = str(val).strip()
    fmts = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).isoformat(sep=" ")
        except ValueError:
            pass
    return None


def clean_and_validate(raw_records: list[dict], dq: DQStats) -> list[dict]:
    """
    Apply data quality rules and return clean records with typed fields:
      event_date    – normalised ISO timestamp string (nullable, kept)
      sensor_number – int 1-100
      sensor_value  – float
      source        – origin source label
    """
    clean = []

    for rec in raw_records:
        # ── sensor_value ──────────────────────────────────────────────────
        raw_val = rec.get("sensor_value")
        if raw_val is None or str(raw_val).strip() in ("", "NULL", "null", "None", "NaN", "NAN", "nan"):
            dq.dropped_null_value += 1
            log.debug(f"DQ drop (null value): {rec}")
            continue

        val = _try_parse_float(raw_val)
        if val is None:
            dq.dropped_non_numeric_value += 1
            log.debug(f"DQ drop (non-numeric value={raw_val!r}): {rec}")
            continue

        # ── sensor_number ─────────────────────────────────────────────────
        raw_sn = rec.get("sensor_number")
        if raw_sn is None or str(raw_sn).strip() in ("", "NULL", "null", "None"):
            dq.dropped_null_sensor_number += 1
            log.debug(f"DQ drop (null sensor_number): {rec}")
            continue

        sn = _try_parse_int(raw_sn)
        if sn is None:
            dq.dropped_non_numeric_sensor_number += 1
            log.debug(f"DQ drop (non-numeric sensor_number={raw_sn!r}): {rec}")
            continue

        if not (VALID_SENSOR_NUMBER_MIN <= sn <= VALID_SENSOR_NUMBER_MAX):
            dq.dropped_out_of_range_sensor += 1
            log.debug(f"DQ drop (sensor_number OOR={sn}): {rec}")
            continue

        # ── event_date ────────────────────────────────────────────────────
        raw_date = rec.get("event_date")
        parsed_date = _try_parse_date(raw_date) if raw_date else None
        # We allow null dates – do NOT drop. Only warn.
        if raw_date and parsed_date is None:
            dq.dropped_bad_date += 1
            log.debug(f"DQ drop (bad date={raw_date!r}): {rec}")
            continue

        clean.append({
            "event_date": parsed_date,
            "sensor_number": sn,
            "sensor_value": val,
            "source": rec.get("_source", "unknown"),
        })

    dq.clean = len(clean)
    return clean


# ── Analysis ──────────────────────────────────────────────────────────────────

def compute_top_third_average(clean_records: list[dict]) -> dict:
    """
    Sort all sensor values descending, take the top 1/3 (ceiling),
    return the average of those values.
    """
    if not clean_records:
        return {"top_third_count": 0, "total_count": 0, "average": None}

    values = sorted(
        [r["sensor_value"] for r in clean_records],
        reverse=True
    )

    total = len(values)
    top_n = math.ceil(total / 3)          # ceiling so we always include at least 1
    top_values = values[:top_n]
    avg = sum(top_values) / top_n

    return {
        "total_count": total,
        "top_third_count": top_n,
        "threshold_value": top_values[-1],  # lowest value in top 1/3
        "average": avg,
    }


# ── Output ────────────────────────────────────────────────────────────────────

def write_output(result: dict, dq: DQStats, output_path: str):
    out = {
        "run_timestamp": datetime.utcnow().isoformat(),
        "data_quality": {
            "total_raw": dq.total_raw,
            "dropped_null_value": dq.dropped_null_value,
            "dropped_non_numeric_value": dq.dropped_non_numeric_value,
            "dropped_null_sensor_number": dq.dropped_null_sensor_number,
            "dropped_non_numeric_sensor_number": dq.dropped_non_numeric_sensor_number,
            "dropped_out_of_range_sensor": dq.dropped_out_of_range_sensor,
            "dropped_bad_date": dq.dropped_bad_date,
            "clean_records": dq.clean,
        },
        "result": result,
    }
    with open(output_path, "w") as f:
        json.dump(out, f, indent=2)
    log.info(f"Output written to: {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(data_root: str, output_path: str):
    log.info("=" * 60)
    log.info("Sensor Data Pipeline – Starting")
    log.info(f"  Data root : {data_root}")
    log.info(f"  Output    : {output_path}")
    log.info("=" * 60)

    # 1. Ingest
    raw_csv = ingest_csv_files(data_root)
    raw_json = ingest_json_files(data_root)
    all_raw = raw_csv + raw_json

    dq = DQStats()
    dq.total_raw = len(all_raw)
    log.info(f"Total raw records (combined): {dq.total_raw}")

    # 2. Clean & validate
    log.info("Applying data quality rules …")
    clean = clean_and_validate(all_raw, dq)
    log.info(dq.summary())

    # 3. Compute result
    result = compute_top_third_average(clean)

    log.info("\nResult")
    log.info("=" * 40)
    log.info(f"  Total clean records  : {result['total_count']}")
    log.info(f"  Top 1/3 count (⌈N/3⌉): {result['top_third_count']}")
    log.info(f"  Threshold value      : {result.get('threshold_value', 'N/A')}")
    log.info(f"  Average of top 1/3   : {result['average']}")
    log.info("=" * 40)

    # 4. Write output
    write_output(result, dq, output_path)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor data pipeline")
    parser.add_argument(
        "--data-root", default="data/source_data",
        help="Root directory containing data_source_1/ and data_source_2/"
    )
    parser.add_argument(
        "--output", default="output/pipeline_result.json",
        help="Path for JSON output file"
    )
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    run(args.data_root, args.output)
