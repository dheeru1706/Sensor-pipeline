# Sensor Data Pipeline

An end-to-end data pipeline that ingests sensor readings from two heterogeneous sources (CSV and JSON), combines them, applies data quality controls, and computes the average of the top 1/3 sensor values.

## Result (Provided Dataset)

| Metric | Value |
|--------|-------|
| Raw records (2 sources) | 20,000 |
| Records dropped (DQ) | 21 (0.1%) |
| Clean records processed | 19,979 |
| Top 1/3 count ⌈N/3⌉ | 6,660 |
| Threshold value | 66.72 |
| **Average of top 1/3** | **83.32** |

## Quick Start

```bash
# Run the pipeline (local Python mode)
python pipeline.py \
  --data-root data/source_data \
  --output output/pipeline_result.json

# Run all 37 unit tests
python -m pytest tests.py -v
```

## Repository Contents

| File | Description |
|------|-------------|
| `pipeline.py` | Python pipeline — local execution |
| `spark_pipeline.py` | PySpark pipeline — scales to 500 GB+ on Databricks |
| `tests.py` | 37 unit tests (all passing ✓) |
| `pipeline_result.json` | Output from running on the provided dataset |
| `requirements_document.docx` | Requirements, schema, DQ rules, acceptance criteria |
| `implementation_steps.docx` | Tech guide — setup, architecture, scalability |
| `sensor_pipeline_presentation.pptx` | 10-slide presentation deck |

## Architecture

Bronze (Raw Ingest) → Silver (DQ + Clean) → Gold (Aggregate)

- **Bronze**: Read all CSV and JSON files recursively from date-partitioned folders
- **Silver**: Validate types, ranges, and dates — drop bad records, log metrics
- **Gold**: Sort descending, take top ⌈N/3⌉ values, compute arithmetic mean

## 🔍 Data Quality Rules

| Field | Check | Action |
|-------|-------|--------|
| `sensor_value` | Null / placeholder ('NULL', 'None') | Drop |
| `sensor_value` | Non-numeric ('ABC', 'IIIII', 'DSL') | Drop |
| `sensor_number` | Non-integer ('XYZ', 'ERR', 'NUO') | Drop |
| `sensor_number` | Out of range [1, 100] | Drop |
| `event_date` | Truncated timestamp (HH:MM only) | Normalise, retain |

## ⚡ Scaling to 500 GB+

Run `spark_pipeline.py` on a Spark cluster:

```bash
spark-submit spark_pipeline.py \
  --data-root s3://your-bucket/source_data \
  --output s3://your-bucket/output/result.json
```

Key design decisions for scale:
- `recursiveFileLookup=true` — all date partitions auto-discovered
- Adaptive Query Execution — Spark optimises shuffle partitions dynamically
- `row_number()` Window function — distributed ranking, no single-node sort bottleneck
- Databricks auto-scaling — adds compute workers on demand

## Tests

37 unit tests covering all DQ rules and computation logic:

```bash
    python -m pytest tests.py -v
    # 37 passed in 0.07s
```

Test suites:

| Suite | Tests | Coverage |
|-------|-------|----------|
| Float/int/date parsing | 21 | Null, empty, alpha, NaN, truncated timestamps |
| DQ clean_and_validate | 10 | All drop rules + happy path |
| Top-1/3 computation | 6 | Empty, 1, 3, 4, 6, 9 records |
| **Total** | **37** | **37/37 passing** |

## Tech Stack

| Component | Tool |
|-----------|------|
| Language | Python 3.12 |
| Big-data engine | Apache Spark (PySpark) |
| Platform | Databricks / AWS EMR / GCP Dataproc |
| Testing | pytest |
| CI | GitHub Actions |
