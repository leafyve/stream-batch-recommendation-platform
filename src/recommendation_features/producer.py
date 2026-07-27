"""Deterministic MovieLens replay producer for Redpanda."""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from recommendation_features.config import ConfigurationError, load_yaml, make_producer_config
from recommendation_features.logging import configure_logging, get_logger
from recommendation_features.schemas import RatingEvent, parse_utc

STOP_REQUESTED = Event()


class DatasetError(ValueError):
    """The source dataset is missing or malformed."""


class DeliveryFailure(RuntimeError):
    """One or more records were not acknowledged by the broker."""


@dataclass(frozen=True)
class SourceRating:
    user_id: int
    movie_id: int
    rating: float
    event_time: datetime


@dataclass
class DeliveryStats:
    attempted: int = 0
    acknowledged: int = 0
    failed: int = 0
    injected_duplicates: int = 0
    reordered_events: int = 0
    started_monotonic: float = 0.0
    ended_monotonic: float = 0.0

    def as_dict(self) -> dict[str, int | float]:
        elapsed = max(self.ended_monotonic - self.started_monotonic, 0.0)
        return {
            "attempted": self.attempted,
            "acknowledged": self.acknowledged,
            "failed": self.failed,
            "injected_duplicates": self.injected_duplicates,
            "reordered_events": self.reordered_events,
            "elapsed_seconds": elapsed,
            "actual_events_per_second": self.acknowledged / elapsed if elapsed else 0.0,
        }


def read_movielens(path: Path) -> Iterator[SourceRating]:
    """Read MovieLens 100K u.data or 1M ratings.dat."""
    if not path.is_file():
        raise DatasetError(f"dataset file does not exist: {path}")
    separator = "::" if path.name == "ratings.dat" else "\t"
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            fields = line.rstrip("\n").split(separator)
            if len(fields) != 4:
                raise DatasetError(f"{path}:{line_number}: expected four fields")
            try:
                user_id, movie_id = int(fields[0]), int(fields[1])
                rating, timestamp = float(fields[2]), int(fields[3])
            except ValueError as exc:
                raise DatasetError(f"{path}:{line_number}: invalid numeric field") from exc
            yield SourceRating(
                user_id=user_id,
                movie_id=movie_id,
                rating=rating,
                event_time=datetime.fromtimestamp(timestamp, tz=timezone.utc),
            )


def build_events(
    records: Iterable[SourceRating],
    *,
    source: str,
    replay_run_id: str,
    max_records: int | None,
    start_record: int,
    ingestion_time: datetime | None = None,
) -> list[RatingEvent]:
    """Create schema-v1 events in stable event-time order."""
    ordered = sorted(records, key=lambda row: (row.event_time, row.user_id, row.movie_id))
    selected = ordered[start_record:]
    if max_records is not None:
        selected = selected[:max_records]
    ingested = ingestion_time or datetime.now(timezone.utc)
    return [
        RatingEvent.create(
            user_id=row.user_id,
            movie_id=row.movie_id,
            rating=row.rating,
            event_time=row.event_time,
            ingestion_time=ingested,
            source=source,
            replay_run_id=replay_run_id,
        )
        for row in selected
    ]


def inject_delivery_modes(
    events: Sequence[RatingEvent],
    *,
    duplicate_percentage: float,
    out_of_order_percentage: float,
    delay_seconds: int,
    seed: int,
) -> tuple[list[RatingEvent], int, int]:
    """Inject duplicates and event-time-delayed delivery without wall-clock sleeping."""
    rng = random.Random(seed)
    base = list(events)
    duplicate_count = round(len(base) * duplicate_percentage / 100.0)
    requested_reordered = round(len(base) * out_of_order_percentage / 100.0)
    duplicates = rng.sample(base, min(duplicate_count, len(base))) if base else []
    combined = base + duplicates
    reordered_count = 0
    if requested_reordered > 0 and len(combined) > 1:
        maximum_event_time = max(parse_utc(event.event_time) for event in combined)
        eligible = [
            index
            for index, event in enumerate(combined)
            if (maximum_event_time - parse_utc(event.event_time)).total_seconds()
            >= delay_seconds
        ]
        positions = sorted(rng.sample(eligible, min(requested_reordered, len(eligible))))
        reordered_count = len(positions)
        selected = set(positions)
        combined = [
            scheduled[3]
            for scheduled in sorted(
                (
                    parse_utc(event.event_time)
                    + (
                        timedelta(seconds=delay_seconds)
                        if index in selected
                        else timedelta(0)
                    ),
                    index in selected,
                    index,
                    event,
                )
                for index, event in enumerate(combined)
            )
        ]
    return combined, len(duplicates), reordered_count


def kafka_producer(bootstrap_servers: str, batch_size: int) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 250,
            "retry.backoff.max.ms": 4000,
            "batch.num.messages": batch_size,
            "client.id": "movielens-replay-producer",
        }
    )


def publish(
    producer: Producer,
    events: Sequence[RatingEvent],
    *,
    topic: str,
    events_per_second: float,
    stats: DeliveryStats,
) -> None:
    """Publish with bounded local queue retries and checked callbacks."""
    logger = get_logger(component="producer", topic=topic)

    def delivered(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            stats.failed += 1
            logger.error("delivery_failed", error=str(error), event_partition=message.partition())
        else:
            stats.acknowledged += 1

    interval = 1.0 / events_per_second
    stats.started_monotonic = time.monotonic()
    for event in events:
        if STOP_REQUESTED.is_set():
            break
        deadline = stats.started_monotonic + (stats.attempted * interval)
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        for attempt in range(6):
            try:
                producer.produce(
                    topic,
                    key=str(event.user_id).encode("utf-8"),
                    value=event.to_json(),
                    on_delivery=delivered,
                )
                stats.attempted += 1
                producer.poll(0)
                break
            except BufferError as exc:
                if attempt == 5:
                    message = "producer queue remained full after five retries"
                    raise DeliveryFailure(message) from exc
                producer.poll(min(0.25 * (2**attempt), 4.0))
            except KafkaException as exc:
                if exc.args and isinstance(exc.args[0], KafkaError) and exc.args[0].retriable():
                    if attempt == 5:
                        raise DeliveryFailure("transient broker retries exhausted") from exc
                    time.sleep(min(0.25 * (2**attempt), 4.0))
                    continue
                raise DeliveryFailure("permanent Kafka delivery configuration error") from exc
    remaining = producer.flush(30.0)
    stats.ended_monotonic = time.monotonic()
    if remaining:
        stats.failed += remaining
        raise DeliveryFailure(f"{remaining} queued messages were not flushed")
    if stats.failed:
        raise DeliveryFailure(f"{stats.failed} broker acknowledgements failed")


def arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-path", type=Path, required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--topic")
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--events-per-second", type=float)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--start-record", type=int, default=0)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--duplicate-percentage", type=float)
    parser.add_argument("--out-of-order-percentage", type=float)
    parser.add_argument("--out-of-order-delay-seconds", type=int)
    parser.add_argument("--replay-run-id", required=True)
    parser.add_argument("--random-seed", type=int)
    parser.add_argument("--statistics-path", type=Path)
    return parser.parse_args(argv)


def _request_stop(_signum: int, _frame: object) -> None:
    STOP_REQUESTED.set()


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = arguments(argv)
    if args.start_record < 0:
        print("start_record must not be negative", file=sys.stderr)
        return 2
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    try:
        config = make_producer_config(
            load_yaml(),
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            events_per_second=args.events_per_second,
            batch_size=args.batch_size,
            max_records=args.max_records,
            duplicate_percentage=args.duplicate_percentage,
            out_of_order_percentage=args.out_of_order_percentage,
            out_of_order_delay_seconds=args.out_of_order_delay_seconds,
            random_seed=args.random_seed,
        )
        events = build_events(
            read_movielens(args.dataset_path),
            source=args.source,
            replay_run_id=args.replay_run_id,
            max_records=config.max_records,
            start_record=args.start_record,
        )
        delivery, duplicates, reordered = inject_delivery_modes(
            events,
            duplicate_percentage=config.duplicate_percentage,
            out_of_order_percentage=config.out_of_order_percentage,
            delay_seconds=config.out_of_order_delay_seconds,
            seed=config.random_seed,
        )
        stats = DeliveryStats(
            injected_duplicates=duplicates,
            reordered_events=reordered,
        )
        publish(
            kafka_producer(config.bootstrap_servers, config.batch_size),
            delivery,
            topic=config.topic,
            events_per_second=config.events_per_second,
            stats=stats,
        )
    except (ConfigurationError, DatasetError, DeliveryFailure, ValueError) as exc:
        get_logger(component="producer").error(
            "producer_failed", error_type=type(exc).__name__, error=str(exc)
        )
        return 1
    result = {
        **stats.as_dict(),
        "source_records": len(events),
        "replay_run_id": args.replay_run_id,
        "topic": config.topic,
        "random_seed": config.random_seed,
        "stopped_by_signal": STOP_REQUESTED.is_set(),
    }
    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered)
    if args.statistics_path:
        args.statistics_path.parent.mkdir(parents=True, exist_ok=True)
        args.statistics_path.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
