"""Producer-independent MovieLens batch backfill."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recommendation_features.feature_definitions import build_spark_feature_aggregation
from recommendation_features.iceberg_tables import create_tables, merge_dataframe

FEATURE_KEYS = ("entity_id", "window_start", "window_end", "feature_version")


def git_revision() -> str | None:
    injected = os.getenv("GIT_REVISION", "").strip()
    if injected:
        return injected
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False
        )
    except FileNotFoundError:
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def read_source(spark: SparkSession, path: str, profile: str) -> DataFrame:
    separator = "::" if profile == "ml-1m" else "\t"
    split = F.split(F.col("value"), separator)
    raw = spark.read.text(path)
    parsed = raw.select(
        split[0].cast("long").alias("user_id"),
        split[1].cast("long").alias("movie_id"),
        split[2].cast("double").alias("rating"),
        F.to_timestamp(F.from_unixtime(split[3].cast("long"))).alias("event_time"),
    )
    source = F.lit("movielens-1m" if profile == "ml-1m" else "movielens-100k")
    event_id = F.sha2(
        F.concat_ws(
            "\u001f",
            source,
            F.lit("1"),
            F.col("user_id").cast("string"),
            F.col("movie_id").cast("string"),
            F.format_string("%.1f", F.col("rating")),
            F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ),
        256,
    )
    return (
        parsed.filter(
            (F.col("user_id") > 0)
            & (F.col("movie_id") > 0)
            & F.col("rating").between(0.5, 5.0)
            & ((F.col("rating") * 2) == F.floor(F.col("rating") * 2))
            & F.col("event_time").isNotNull()
        )
        .withColumn("event_id", event_id)
        .withColumn("schema_version", F.lit(1))
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("source", source)
        .withColumn("replay_run_id", F.lit(f"batch-{profile}"))
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-path", required=True)
    parser.add_argument("--profile", choices=["ml-100k", "ml-1m"], required=True)
    parser.add_argument("--checksum")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"batch-backfill-{args.profile}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    try:
        valid = read_source(spark, args.dataset_path, args.profile).cache()
        source_records = spark.read.text(args.dataset_path).count()
        valid_records = valid.count()
        user = build_spark_feature_aggregation(valid, "user")
        movie = build_spark_feature_aggregation(valid, "movie")
        user_rows = merge_dataframe(
            spark,
            user,
            "local.features.user_batch_features",
            FEATURE_KEYS,
            view_suffix=f"user_batch_{run_id}",
        )
        movie_rows = merge_dataframe(
            spark,
            movie,
            "local.features.movie_batch_features",
            FEATURE_KEYS,
            view_suffix=f"movie_batch_{run_id}",
        )
        completed = datetime.now(timezone.utc)
        audit = spark.createDataFrame(
            [
                (
                    run_id,
                    "batch",
                    "success",
                    args.profile,
                    args.checksum,
                    source_records,
                    valid_records,
                    source_records - valid_records,
                    json.dumps(
                        {
                            "window": "7 days",
                            "slide": "1 day",
                            "feature_version": 1,
                            "user_feature_rows": user_rows,
                            "movie_feature_rows": movie_rows,
                        },
                        sort_keys=True,
                    ),
                    git_revision(),
                    started.replace(tzinfo=None),
                    completed.replace(tzinfo=None),
                )
            ],
            """
            run_id string, run_type string, status string, dataset_profile string,
            dataset_checksum string, source_records long, valid_records long,
            invalid_records long, configuration_json string, git_revision string,
            started_at timestamp, completed_at timestamp
            """,
        )
        merge_dataframe(
            spark,
            audit,
            "local.audit.pipeline_runs",
            ("run_id",),
            view_suffix=f"audit_{run_id}",
        )
        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "source_records": source_records,
                    "valid_records": valid_records,
                    "invalid_records": source_records - valid_records,
                    "user_feature_rows": user_rows,
                    "movie_feature_rows": movie_rows,
                    "status": "success",
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
