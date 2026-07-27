"""Read-only machine-readable Iceberg correctness probe."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recommendation_features.iceberg_tables import create_tables

TABLES = (
    "local.raw.valid_rating_events",
    "local.raw.invalid_rating_events",
    "local.features.user_stream_features",
    "local.features.movie_stream_features",
    "local.features.user_batch_features",
    "local.features.movie_batch_features",
    "local.audit.pipeline_runs",
    "local.audit.parity_results",
)
FEATURE_KEYS = ("entity_id", "window_start", "window_end", "feature_version")


def feature_table_probe(spark: SparkSession, table: str) -> dict[str, Any]:
    frame = spark.table(table)
    total = frame.count()
    unique = frame.select(*FEATURE_KEYS).distinct().count()
    watermark_row = frame.agg(F.max("window_end").alias("maximum_window_end")).first()
    return {
        "rows": total,
        "unique_logical_keys": unique,
        "duplicate_logical_keys": total - unique,
        "maximum_window_end": (
            str(watermark_row["maximum_window_end"])
            if watermark_row and watermark_row["maximum_window_end"] is not None
            else None
        ),
    }


def collect_probe(spark: SparkSession) -> dict[str, Any]:
    create_tables(spark)
    table_counts = {table: spark.table(table).count() for table in TABLES}
    valid = spark.table("local.raw.valid_rating_events")
    valid_rows = table_counts["local.raw.valid_rating_events"]
    valid_unique = valid.select("event_id").distinct().count()
    invalid = spark.table("local.raw.invalid_rating_events")
    invalid_rows = table_counts["local.raw.invalid_rating_events"]
    invalid_unique = invalid.select("invalid_record_id").distinct().count()
    return {
        "status": "pass",
        "table_counts": table_counts,
        "valid_event_uniqueness": {
            "rows": valid_rows,
            "unique_event_ids": valid_unique,
            "duplicate_event_ids": valid_rows - valid_unique,
        },
        "invalid_event_uniqueness": {
            "rows": invalid_rows,
            "unique_invalid_record_ids": invalid_unique,
            "duplicate_invalid_record_ids": invalid_rows - invalid_unique,
        },
        "user_stream_features": feature_table_probe(
            spark, "local.features.user_stream_features"
        ),
        "movie_stream_features": feature_table_probe(
            spark, "local.features.movie_stream_features"
        ),
        "user_batch_features": feature_table_probe(
            spark, "local.features.user_batch_features"
        ),
        "movie_batch_features": feature_table_probe(
            spark, "local.features.movie_batch_features"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("platform-read-only-probe").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = collect_probe(spark)
        rendered = json.dumps(result, indent=2, sort_keys=True)
        print(rendered)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered + "\n", encoding="utf-8")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
