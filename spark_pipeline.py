"""
spark_pipeline.py — PySpark version of the sensor data pipeline.

Designed for large-scale execution (500 GB+ data) on Databricks,
AWS EMR, Google Dataproc, or any Spark cluster.

Usage (local):
    spark-submit spark_pipeline.py --data-root /path/to/source_data

Usage (Databricks):
    Upload to DBFS or a notebook, set DATA_ROOT to an S3/ADLS/GCS path.
"""

import os
import sys
import math
import argparse
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType,
        TimestampType
    )
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False
    log.warning("PySpark not available – run this on a Spark cluster.")


def build_spark(app_name: str = "SensorPipeline") -> "SparkSession":
    return (
        SparkSession.builder
        .appName(app_name)
        # Adaptive query execution (auto-partitioning on large datasets)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # For local testing
        .config("spark.sql.shuffle.partitions", "auto")
        .getOrCreate()
    )


def ingest_csv(spark, data_root: str):
    """Read all CSVs under data_source_1/ with schema inference."""
    csv_path = os.path.join(data_root, "data_source_1", "**", "*.csv")
    df = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("mode", "PERMISSIVE")    # Don't fail on malformed rows
        .csv(csv_path)
        .withColumn("_source", F.lit("data_source_1"))
    )
    log.info(f"CSV ingest schema: {df.schema.simpleString()}")
    return df


def ingest_json(spark, data_root: str):
    """Read all JSONs under data_source_2/ (array-of-objects format)."""
    json_path = os.path.join(data_root, "data_source_2", "**", "*.json")
    df = (
        spark.read
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .option("mode", "PERMISSIVE")
        .json(json_path)
        .withColumn("_source", F.lit("data_source_2"))
    )
    log.info(f"JSON ingest schema: {df.schema.simpleString()}")
    return df


def normalise_and_union(csv_df, json_df):
    """Rename columns to a common schema and union both sources."""
    # Both sources have: event_date, sensor_number, sensor_value
    common_cols = ["event_date", "sensor_number", "sensor_value", "_source"]

    def select_common(df):
        existing = set(df.columns)
        selected = [c for c in common_cols if c in existing]
        return df.select(*selected)

    return select_common(csv_df).unionByName(select_common(json_df), allowMissingColumns=True)


def apply_dq(df):
    """
    Apply data quality rules using Spark SQL expressions.
    Returns (clean_df, dq_metrics_dict).
    """
    total_raw = df.count()

    # Cast sensor_value to double – coerce non-numeric to null
    df = df.withColumn(
        "sensor_value_cast",
        F.col("sensor_value").cast(DoubleType())
    )

    # Cast sensor_number to int – coerce non-numeric to null
    df = df.withColumn(
        "sensor_number_cast",
        F.col("sensor_number").cast(IntegerType())
    )

    # Identify valid rows
    df = df.withColumn(
        "_valid",
        (
            F.col("sensor_value_cast").isNotNull() &
            ~F.isnan(F.col("sensor_value_cast")) &
            F.col("sensor_number_cast").isNotNull() &
            (F.col("sensor_number_cast") >= 1) &
            (F.col("sensor_number_cast") <= 100)
        )
    )

    # Count drops (materialise once)
    dq_df = df.groupBy("_valid").count().collect()
    invalid_count = sum(r["count"] for r in dq_df if not r["_valid"])
    valid_count   = sum(r["count"] for r in dq_df if r["_valid"])

    log.info(f"DQ: {total_raw} raw → {valid_count} valid, {invalid_count} dropped")

    clean_df = (
        df.filter(F.col("_valid"))
        .select(
            F.to_timestamp("event_date").alias("event_date"),
            F.col("sensor_number_cast").alias("sensor_number"),
            F.col("sensor_value_cast").alias("sensor_value"),
            F.col("_source"),
        )
    )

    return clean_df, {"total_raw": total_raw, "valid": valid_count, "dropped": invalid_count}


def compute_top_third_average(clean_df):
    """
    Compute average of top 1/3 sensor values.

    Strategy for large datasets:
    - Use percentile_approx to find the threshold for the top 1/3 efficiently
    - Filter and average – O(N) without full sort/materialise
    """
    total = clean_df.count()
    if total == 0:
        return {"total_count": 0, "top_third_count": 0, "average": None}

    top_n = math.ceil(total / 3)

    # Compute the 66.67th percentile threshold (values above this are top ~1/3)
    # We use the exact approach: rank with window function for correctness
    from pyspark.sql.window import Window

    # Add a global rank (1 = highest value)
    window = Window.orderBy(F.col("sensor_value").desc())
    ranked = clean_df.withColumn("_rank", F.row_number().over(window))

    top_df = ranked.filter(F.col("_rank") <= top_n)
    result_row = top_df.agg(
        F.count("sensor_value").alias("top_third_count"),
        F.avg("sensor_value").alias("average"),
        F.min("sensor_value").alias("threshold_value"),
    ).collect()[0]

    return {
        "total_count": total,
        "top_third_count": result_row["top_third_count"],
        "threshold_value": result_row["threshold_value"],
        "average": result_row["average"],
    }


def run_spark(data_root: str, output_path: str):
    if not HAS_SPARK:
        log.error("PySpark not installed. Run: pip install pyspark")
        sys.exit(1)

    spark = build_spark()
    log.info(f"Spark version: {spark.version}")

    csv_df  = ingest_csv(spark, data_root)
    json_df = ingest_json(spark, data_root)

    combined = normalise_and_union(csv_df, json_df)
    clean_df, dq_metrics = apply_dq(combined)
    result = compute_top_third_average(clean_df)

    log.info(f"\nResult: {result}")

    import json
    out = {
        "run_timestamp": datetime.utcnow().isoformat(),
        "engine": f"PySpark {spark.version}",
        "data_quality": dq_metrics,
        "result": result,
    }
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(out, f, indent=2)
    log.info(f"Output written: {output_path}")

    spark.stop()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor data pipeline (PySpark)")
    parser.add_argument("--data-root", default="data/source_data")
    parser.add_argument("--output", default="output/spark_result.json")
    args = parser.parse_args()
    run_spark(args.data_root, args.output)
