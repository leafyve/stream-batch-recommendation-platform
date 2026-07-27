"""Publish malformed, invalid, and deliberately late contract-test events."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Message, Producer

from recommendation_features.schemas import RatingEvent

TOPIC = "rating-events"


def main() -> int:
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
