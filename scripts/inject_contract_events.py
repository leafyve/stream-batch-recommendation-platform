"""Publish malformed, invalid, and deliberately late contract-test events."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Message, Producer

from recommendation_features.schemas import RatingEvent

TOPIC = "rating-events"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--advance-watermark-only",
        action="store_true",
        help="publish one future-dated valid marker so earlier feature windows finalize",
    )
    args = parser.parse_args()
    producer = Producer(
        {
            "bootstrap.servers": "localhost:19092",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    failures: list[str] = []

    def delivered(error: KafkaError | None, _message: Message) -> None:
        if error is not None:
            failures.append(str(error))

    if args.advance_watermark_only:
        marker = RatingEvent.create(
            user_id=9_000_000,
            movie_id=9_000_000,
            rating=4.0,
            event_time="2035-01-01T00:00:00Z",
            ingestion_time=datetime.now(timezone.utc),
            source="contract-test",
            replay_run_id="watermark-finalization-marker",
        )
        producer.produce(
            TOPIC,
            key=b"contract-watermark-marker",
            value=marker.to_json(),
            on_delivery=delivered,
        )
        remaining = producer.flush(30)
        if failures or remaining:
            raise RuntimeError(f"delivery failures={failures!r} remaining={remaining}")
        print(
            json.dumps(
                {
                    "status": "pass",
                    "published": 1,
                    "watermark_finalization_marker": 1,
                    "marker_event_id": marker.event_id,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    invalid_rating = {
        "event_id": "0" * 64,
        "schema_version": 1,
        "user_id": 9_000_001,
        "movie_id": 9_000_001,
        "rating": 4.2,
        "event_time": "1998-04-22T00:00:00Z",
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "source": "contract-test",
        "replay_run_id": "invalid-rating",
    }
    deliberately_late = RatingEvent.create(
        user_id=9_000_002,
        movie_id=9_000_002,
        rating=4.0,
        event_time="1990-01-01T00:00:00Z",
        ingestion_time=datetime.now(timezone.utc),
        source="contract-test",
        replay_run_id="deliberately-late",
    )
    payloads = (
        b"{",
        json.dumps(invalid_rating, sort_keys=True).encode("utf-8"),
        deliberately_late.to_json(),
    )
    for index, payload in enumerate(payloads):
        producer.produce(
            TOPIC,
            key=f"contract-{index}".encode(),
            value=payload,
            on_delivery=delivered,
        )
    remaining = producer.flush(30)
    if failures or remaining:
        raise RuntimeError(f"delivery failures={failures!r} remaining={remaining}")
    print(
        json.dumps(
            {
                "status": "pass",
                "published": len(payloads),
                "malformed": 1,
                "invalid_rating": 1,
                "deliberately_late_valid": 1,
                "late_event_id": deliberately_late.event_id,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
