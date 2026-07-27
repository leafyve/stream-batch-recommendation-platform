"""Four-query Spark Structured Streaming pipeline."""

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from recommendation_features.feature_definitions import build_spark_feature_aggregation
from recommendation_features.iceberg_tables import create_tables, merge_dataframe

BOOTSTRAP = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("RATING_TOPIC", "rating-events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "rating-events-dlq")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/checkpoints")
REPORT_ROOT = Path(os.getenv("REPORT_ROOT", "/reports/runtime"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("schema_version", LongType()),
        StructField("user_id", LongType()),
        StructField("movie_id", LongType()),
        StructField("rating", DoubleType()),
        StructField("event_time", StringType()),
        StructField("ingestion_time", StringType()),
        StructField("source", StringType()),
        StructField("replay_run_id", StringType()),
    ]
)

FEATURE_KEYS = ("entity_id", "window_start", "window_end", "feature_version")
REPORT_LOCK = Lock()
# The approved local REST catalog persists metadata in SQLite. SQLite permits a
# single writer, while foreachBatch callbacks from these four queries may run
# concurrently. Serialize only catalog commits; Kafka reads and feature
# computation remain concurrent.
CATALOG_COMMIT_LOCK = Lock()


def kafka_source(spark: SparkSession, query_name: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("groupIdPrefix", f"recommendation-{query_name}")
        .load()
    )


def parse_and_validate(frame: DataFrame) -> DataFrame:
    parsed = frame.withColumn("payload_text", F.col("value").cast("string")).withColumn(
        "event", F.from_json("payload_text", EVENT_SCHEMA)
    )
    event_time = F.to_timestamp("event.event_time")
    ingestion_time = F.to_timestamp("event.ingestion_time")
    canonical_id = F.sha2(
        F.concat_ws(
            "\u001f",
            F.trim(F.col("event.source")),
            F.col("event.schema_version").cast("string"),
            F.col("event.user_id").cast("string"),
            F.col("event.movie_id").cast("string"),
            F.format_string("%.1f", F.col("event.rating")),
            F.date_format(event_time, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ),
        256,
    )
    candidates = F.array(
        F.when(F.col("event").isNull(), F.lit("invalid JSON")),
        F.when(F.col("event.event_id").isNull(), F.lit("missing field: event_id")),
        F.when(F.col("event.schema_version").isNull(), F.lit("missing field: schema_version")),
        F.when(F.col("event.user_id").isNull(), F.lit("missing field: user_id")),
        F.when(F.col("event.movie_id").isNull(), F.lit("missing field: movie_id")),
        F.when(F.col("event.rating").isNull(), F.lit("missing field: rating")),
        F.when(F.col("event.event_time").isNull(), F.lit("missing field: event_time")),
        F.when(F.col("event.ingestion_time").isNull(), F.lit("missing field: ingestion_time")),
        F.when(F.col("event.source").isNull(), F.lit("missing field: source")),
        F.when(F.col("event.replay_run_id").isNull(), F.lit("missing field: replay_run_id")),
        F.when(F.col("event.schema_version") != 1, F.lit("unsupported schema version")),
        F.when(
            (F.col("event.user_id") <= 0) | (F.col("event.movie_id") <= 0),
            F.lit("user_id and movie_id must be positive"),
        ),
        F.when(
            (F.col("event.rating") < 0.5)
            | (F.col("event.rating") > 5.0)
            | ((F.col("event.rating") * 2) != F.floor(F.col("event.rating") * 2)),
            F.lit("rating must be 0.5 through 5.0 in half-star increments"),
        ),
        F.when(event_time.isNull(), F.lit("invalid event_time")),
        F.when(ingestion_time.isNull(), F.lit("invalid ingestion_time")),
        F.when(F.length(F.trim(F.col("event.source"))) == 0, F.lit("source must be non-empty")),
        F.when(
            F.length(F.trim(F.col("event.replay_run_id"))) == 0,
            F.lit("replay_run_id must be non-empty"),
        ),
        F.when(
            F.col("event.event_id").isNotNull() & (F.col("event.event_id") != canonical_id),
            F.lit("event_id does not match canonical event fields"),
        ),
    )
    return (
        parsed.withColumn(
            "validation_errors", F.filter(candidates, lambda value: value.isNotNull())
        )
        .withColumn("is_valid", F.size("validation_errors") == 0)
        .withColumn("parsed_event_time", event_time)
        .withColumn("parsed_ingestion_time", ingestion_time)
    )


def valid_rows(frame: DataFrame) -> DataFrame:
    return frame.filter("is_valid").select(
        F.col("event.event_id").alias("event_id"),
        F.col("event.schema_version").cast("int").alias("schema_version"),
        F.col("event.user_id").alias("user_id"),
        F.col("event.movie_id").alias("movie_id"),
        F.col("event.rating").alias("rating"),
        F.col("parsed_event_time").alias("event_time"),
        F.col("parsed_ingestion_time").alias("ingestion_time"),
        F.trim(F.col("event.source")).alias("source"),
        F.trim(F.col("event.replay_run_id")).alias("replay_run_id"),
        F.col("key").alias("kafka_key"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )


def invalid_rows(frame: DataFrame) -> DataFrame:
    return frame.filter("NOT is_valid").select(
        F.sha2(
            F.concat_ws(
                ":",
                F.col("topic"),
                F.col("partition").cast("string"),
                F.col("offset").cast("string"),
            ),
            256,
        ).alias("invalid_record_id"),
        F.col("value").alias("raw_payload"),
        "validation_errors",
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("key").alias("kafka_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingestion_time"),
    )


def write_metric(payload: dict[str, object]) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    target = REPORT_ROOT / "streaming_batches.jsonl"
    with REPORT_LOCK, target.open("a", encoding="utf-8") as output:
        output.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def merge_sink(
    spark: SparkSession, query_name: str, table: str, keys: tuple[str, ...]
) -> Callable[[DataFrame, int], None]:
    def sink(frame: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        status = "success"
        error: str | None = None
        output_rows = 0
        frame.persist()
        try:
            with CATALOG_COMMIT_LOCK:
                output_rows = merge_dataframe(
                    spark,
                    frame,
                    table,
                    keys,
                    view_suffix=f"{query_name}_{batch_id}",
                )
        except Exception as exc:
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            raise
        finally:
            frame.unpersist()
            write_metric(
                {
                    "query_name": query_name,
                    "batch_id": batch_id,
                    "output_rows": output_rows,
                    "processing_seconds": time.monotonic() - started,
                    "merge_status": status,
                    "error": error,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                }
            )

    return sink


def invalid_sink(spark: SparkSession) -> Callable[[DataFrame, int], None]:
    def sink(frame: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        status = "success"
        error: str | None = None
        output_rows = 0
        frame.persist()
        try:
            with CATALOG_COMMIT_LOCK:
                output_rows = merge_dataframe(
                    spark,
                    frame,
                    "local.raw.invalid_rating_events",
                    ("invalid_record_id",),
                    view_suffix=f"raw_invalid_{batch_id}",
                )
            if output_rows:
                (
                    frame.select(
                        F.col("invalid_record_id").cast("binary").alias("key"),
                        F.to_json(
                            F.struct(
                                F.base64("raw_payload").alias("raw_payload_base64"),
                                "validation_errors",
                                "source_topic",
                                "source_partition",
                                "source_offset",
                                "kafka_timestamp",
                                "ingestion_time",
                                "invalid_record_id",
                            )
                        )
                        .cast("binary")
                        .alias("value"),
                    )
                    .write.format("kafka")
                    .option("kafka.bootstrap.servers", BOOTSTRAP)
                    .option("topic", DLQ_TOPIC)
                    .save()
                )
        except Exception as exc:
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            raise
        finally:
            frame.unpersist()
            write_metric(
                {
                    "query_name": "raw_invalid_ingestion",
                    "batch_id": batch_id,
                    "output_rows": output_rows,
                    "processing_seconds": time.monotonic() - started,
                    "merge_status": status,
                    "error": error,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "dlq_duplicate_limitation": (
                        "A retry after Iceberg commit may duplicate Kafka DLQ output; "
                        "deduplicate by invalid_record_id."
                    ),
                }
            )

    return sink


class ProgressRecorder(StreamingQueryListener):
    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        write_metric({"listener_event": "started", "details": str(event)})

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        progress = json.loads(event.progress.json)
        write_metric({"listener_event": "progress", **progress})

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        write_metric({"listener_event": "terminated", "details": str(event)})

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        write_metric({"listener_event": "idle", "details": str(event)})


def start_query(
    frame: DataFrame,
    *,
    name: str,
    checkpoint: str,
    sink: Callable[[DataFrame, int], None],
) -> StreamingQuery:
    return (
        frame.writeStream.queryName(name)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{checkpoint}")
        .foreachBatch(sink)
        .start()
    )


def main() -> int:
    spark = SparkSession.builder.appName("stream-batch-recommendation-features").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressRecorder())
    create_tables(spark)

    raw_valid_frame = valid_rows(parse_and_validate(kafka_source(spark, "raw-valid")))
    raw_invalid_frame = invalid_rows(parse_and_validate(kafka_source(spark, "raw-invalid")))
    user_frame = valid_rows(parse_and_validate(kafka_source(spark, "user-features")))
    movie_frame = valid_rows(parse_and_validate(kafka_source(spark, "movie-features")))

    queries = [
        start_query(
            raw_valid_frame,
            name="raw_valid_ingestion",
            checkpoint="raw_valid",
            sink=merge_sink(
                spark,
                "raw_valid_ingestion",
                "local.raw.valid_rating_events",
                ("event_id",),
            ),
        ),
        start_query(
            raw_invalid_frame,
            name="raw_invalid_ingestion",
            checkpoint="raw_invalid",
            sink=invalid_sink(spark),
        ),
        start_query(
            build_spark_feature_aggregation(user_frame, "user"),
            name="user_feature_aggregation",
            checkpoint="user_features",
            sink=merge_sink(
                spark,
                "user_feature_aggregation",
                "local.features.user_stream_features",
                FEATURE_KEYS,
            ),
        ),
        start_query(
            build_spark_feature_aggregation(movie_frame, "movie"),
            name="movie_feature_aggregation",
            checkpoint="movie_features",
            sink=merge_sink(
                spark,
                "movie_feature_aggregation",
                "local.features.movie_stream_features",
                FEATURE_KEYS,
            ),
        ),
    ]
    try:
        spark.streams.awaitAnyTermination()
        for query in queries:
            if query.exception():
                raise RuntimeError(f"{query.name} terminated: {query.exception()}")
    except KeyboardInterrupt:
        for query in queries:
            query.stop()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
