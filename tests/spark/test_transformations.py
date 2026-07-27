"""Practical Spark contracts executed in the pinned project image."""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from pyspark.sql import Row, SparkSession

from recommendation_features.feature_definitions import build_spark_feature_aggregation
from recommendation_features.schemas import RatingEvent
from recommendation_features.streaming_job import invalid_rows, parse_and_validate, valid_rows


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("spark-transformation-contracts")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def kafka_row(payload: bytes, *, offset: int = 0) -> Row:
    return Row(
        key=b"1",
        value=payload,
        topic="rating-events",
        partition=0,
        offset=offset,
        timestamp=datetime(2026, 1, 1),
    )


def valid_payload(*, rating: float = 4.0, schema_version: int = 1) -> bytes:
    event = RatingEvent.create(
        user_id=1,
        movie_id=2,
        rating=rating,
        event_time="2026-01-02T00:00:00Z",
        ingestion_time="2026-02-01T00:00:00Z",
        source="fixture",
        replay_run_id="spark-contract",
        schema_version=schema_version,
    )
    return event.to_json()


@pytest.mark.spark
def test_valid_and_malformed_events_are_separated(spark: SparkSession) -> None:
    frame = spark.createDataFrame([kafka_row(valid_payload()), kafka_row(b"{", offset=1)])
    parsed = parse_and_validate(frame)

    accepted = valid_rows(parsed).collect()
    rejected = invalid_rows(parsed).collect()

    assert len(accepted) == 1
    assert len(rejected) == 1
    assert rejected[0].invalid_record_id
    assert "missing field: event_id" in rejected[0].validation_errors
    assert rejected[0].raw_payload == bytearray(b"{")


@pytest.mark.spark
def test_invalid_rating_and_schema_version_are_rejected(spark: SparkSession) -> None:
    payload = json.loads(valid_payload())
    payload["rating"] = 4.2
    unsupported = json.loads(valid_payload())
    unsupported["schema_version"] = 2
    frame = spark.createDataFrame(
        [
            kafka_row(json.dumps(payload).encode()),
            kafka_row(json.dumps(unsupported).encode(), offset=1),
        ]
    )

    errors = [row.validation_errors for row in invalid_rows(parse_and_validate(frame)).collect()]

    assert any("rating must be 0.5 through 5.0 in half-star increments" in row for row in errors)
    assert any("unsupported schema version" in row for row in errors)


@pytest.mark.spark
def test_batch_feature_expression_deduplicates_and_uses_utc_windows(
    spark: SparkSession,
) -> None:
    rows = [
        Row(
            event_id="same",
            user_id=1,
            movie_id=10,
            rating=4.0,
            event_time=datetime(2026, 1, 2, 12),
        ),
        Row(
            event_id="same",
            user_id=1,
            movie_id=10,
            rating=4.0,
            event_time=datetime(2026, 1, 2, 12),
        ),
        Row(
            event_id="other",
            user_id=1,
            movie_id=11,
            rating=2.0,
            event_time=datetime(2026, 1, 3, 12),
        ),
    ]

    features = build_spark_feature_aggregation(spark.createDataFrame(rows), "user")
    containing_both = features.filter(
        "window_start = timestamp'2026-01-02 00:00:00' "
        "AND window_end = timestamp'2026-01-09 00:00:00'"
    ).first()

    assert containing_both is not None
    assert containing_both.rating_count == 2
    assert containing_both.average_rating == 3.0
    assert containing_both.minimum_rating == 2.0
    assert containing_both.maximum_rating == 4.0
