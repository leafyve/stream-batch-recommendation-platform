"""Compare finalised batch and streaming feature windows."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recommendation_features.feature_definitions import AVERAGE_TOLERANCE
from recommendation_features.iceberg_tables import create_tables, merge_dataframe

KEYS = ("entity_id", "window_start", "window_end", "feature_version")


def compare(batch: DataFrame, stream: DataFrame, entity_type: str) -> dict[str, object]:
    watermark_row = stream.agg(F.max("window_end").alias("watermark")).first()
    if watermark_row is None:
        raise RuntimeError(f"{entity_type}: watermark query returned no row")
    watermark = watermark_row["watermark"]
    if watermark is None:
        raise RuntimeError(f"{entity_type}: streaming feature table is empty")
    expected = batch.filter(F.col("window_end") <= F.lit(watermark))
    actual = stream.filter(F.col("window_end") <= F.lit(watermark))
    joined = expected.alias("batch").join(actual.alias("stream"), list(KEYS), "full_outer")
    missing = joined.filter(F.col("stream.entity_id").isNull()).count()
    unexpected = joined.filter(F.col("batch.entity_id").isNull()).count()
    comparable = joined.filter(
        F.col("batch.entity_id").isNotNull() & F.col("stream.entity_id").isNotNull()
    )
    exact = comparable.filter(
        (F.col("batch.rating_count") != F.col("stream.rating_count"))
        | (F.col("batch.minimum_rating") != F.col("stream.minimum_rating"))
        | (F.col("batch.maximum_rating") != F.col("stream.maximum_rating"))
    ).count()
    floating = comparable.filter(
        F.abs(F.col("batch.average_rating") - F.col("stream.average_rating"))
        > AVERAGE_TOLERANCE
    ).count()
    expected_rows, streaming_rows = expected.count(), actual.count()
    failures = missing + unexpected + exact + floating
    denominator = max(expected_rows, 1)
    parity_percentage = max(0.0, 100.0 * (1.0 - failures / denominator))
    return {
        "entity_type": entity_type,
        "watermark": watermark,
        "expected_rows": expected_rows,
        "streaming_rows": streaming_rows,
        "missing_streaming_keys": missing,
        "unexpected_streaming_keys": unexpected,
        "exact_value_mismatches": exact,
        "floating_point_mismatches": floating,
        "parity_percentage": parity_percentage,
        "status": "pass" if failures == 0 and expected_rows == streaming_rows else "fail",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("batch-stream-parity").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)
    results: list[dict[str, object]] = []
    try:
        for entity_type in ("user", "movie"):
            results.append(
                compare(
                    spark.table(f"local.features.{entity_type}_batch_features"),
                    spark.table(f"local.features.{entity_type}_stream_features"),
                    entity_type,
                )
            )
        created = datetime.now(timezone.utc)
        rows = []
        for result in results:
            result_id = str(uuid.uuid4())
            details = json.dumps(result, sort_keys=True, default=str)
            rows.append(
                (
                    result_id,
                    result["entity_type"],
                    result["watermark"],
                    result["expected_rows"],
                    result["streaming_rows"],
                    result["missing_streaming_keys"],
                    result["unexpected_streaming_keys"],
                    result["exact_value_mismatches"],
                    result["floating_point_mismatches"],
                    result["parity_percentage"],
                    result["status"],
                    created.replace(tzinfo=None),
                    details,
                )
            )
        frame = spark.createDataFrame(
            rows,
            """
            result_id string, entity_type string, watermark timestamp,
            expected_rows long, streaming_rows long, missing_streaming_keys long,
            unexpected_streaming_keys long, exact_value_mismatches long,
            floating_point_mismatches long, parity_percentage double, status string,
            created_at timestamp, details_json string
            """,
        )
        merge_dataframe(
            spark,
            frame,
            "local.audit.parity_results",
            ("result_id",),
            view_suffix=str(uuid.uuid4()),
        )
        rendered = json.dumps(results, indent=2, sort_keys=True, default=str)
        print(rendered)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered + "\n", encoding="utf-8")
        return 0 if all(result["status"] == "pass" for result in results) else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
