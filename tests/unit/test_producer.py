from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from recommendation_features import producer as producer_module
from recommendation_features.producer import (
    DatasetError,
    DeliveryStats,
    SourceRating,
    arguments,
    build_events,
    inject_delivery_modes,
    main,
    publish,
    read_movielens,
)


def source(second: int, user: int = 1, movie: int = 2) -> SourceRating:
    return SourceRating(user, movie, 4.0, datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC))


def test_read_movielens_100k_and_1m(tmp_path: Path) -> None:
    hundred = tmp_path / "u.data"
    hundred.write_text("1\t2\t4\t100\n", encoding="utf-8")
    million = tmp_path / "ratings.dat"
    million.write_text("3::4::4.5::200\n", encoding="utf-8")
    assert next(read_movielens(hundred)).user_id == 1
    assert next(read_movielens(million)).rating == 4.5


def test_read_movielens_reports_missing_and_bad_rows(tmp_path: Path) -> None:
    with pytest.raises(DatasetError, match="does not exist"):
        list(read_movielens(tmp_path / "none"))
    bad = tmp_path / "u.data"
    bad.write_text("1\t2\n", encoding="utf-8")
    with pytest.raises(DatasetError, match="four fields"):
        list(read_movielens(bad))
    bad.write_text("x\t2\t4\t100\n", encoding="utf-8")
    with pytest.raises(DatasetError, match="numeric"):
        list(read_movielens(bad))


def test_build_events_sorts_slices_and_uses_fixed_ingestion_time() -> None:
    ingested = datetime(2026, 2, 1, tzinfo=UTC)
    result = build_events(
        [source(3), source(1), source(2)],
        source="fixture",
        replay_run_id="run",
        start_record=1,
        max_records=1,
        ingestion_time=ingested,
    )
    assert len(result) == 1
    assert result[0].event_time == "2026-01-01T00:00:02Z"
    assert result[0].ingestion_time == "2026-02-01T00:00:00Z"


def test_injection_is_deterministic_and_duplicates_keep_identity() -> None:
    events = build_events(
        [source(index, user=index + 1) for index in range(10)],
        source="fixture",
        replay_run_id="run",
        start_record=0,
        max_records=None,
        ingestion_time=datetime(2026, 2, 1, tzinfo=UTC),
    )
    first = inject_delivery_modes(
        events,
        duplicate_percentage=20,
        out_of_order_percentage=30,
        delay_seconds=0,
        seed=42,
    )
    second = inject_delivery_modes(
        events,
        duplicate_percentage=20,
        out_of_order_percentage=30,
        delay_seconds=0,
        seed=42,
    )
    assert first == second
    delivery, duplicates, reordered = first
    assert len(delivery) == 12
    assert duplicates == 2
    assert reordered == 3
    assert len({event.event_id for event in delivery}) == 10


def test_empty_injection_and_required_arguments() -> None:
    result = inject_delivery_modes(
        [],
        duplicate_percentage=50,
        out_of_order_percentage=50,
        delay_seconds=0,
        seed=1,
    )
    assert result == ([], 0, 0)
    parsed = arguments(
        [
            "--dataset-path",
            "data",
            "--source",
            "source",
            "--replay-run-id",
            "run",
        ]
    )
    assert parsed.dataset_path == Path("data")
    assert parsed.start_record == 0


class RecordingProducer:
    def __init__(self, *, remaining: int = 0) -> None:
        self.remaining = remaining
        self.records: list[tuple[str, bytes, bytes]] = []

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        on_delivery: Any,
    ) -> None:
        self.records.append((topic, key, value))
        on_delivery(None, object())

    def poll(self, _timeout: float) -> None:
        return None

    def flush(self, _timeout: float) -> int:
        return self.remaining


def test_publish_records_checked_acknowledgements(monkeypatch: pytest.MonkeyPatch) -> None:
    event = build_events(
        [source(0)],
        source="fixture",
        replay_run_id="run",
        start_record=0,
        max_records=None,
        ingestion_time=datetime(2026, 2, 1, tzinfo=UTC),
    )[0]
    clock = iter([100.0, 100.0, 100.5])
    monkeypatch.setattr(producer_module.time, "monotonic", lambda: next(clock))
    broker = RecordingProducer()
    stats = DeliveryStats()

    publish(
        broker,  # type: ignore[arg-type]
        [event],
        topic="ratings",
        events_per_second=100.0,
        stats=stats,
    )

    assert stats.as_dict()["acknowledged"] == 1
    assert broker.records == [("ratings", b"1", event.to_json())]
    assert stats.as_dict()["actual_events_per_second"] == 2.0


def test_main_writes_statistics_and_rejects_negative_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = tmp_path / "u.data"
    dataset.write_text("1\t2\t4\t100\n", encoding="utf-8")
    statistics = tmp_path / "metrics" / "producer.json"

    def fake_publish(
        _producer: object,
        events: list[object],
        *,
        topic: str,
        events_per_second: float,
        stats: DeliveryStats,
    ) -> None:
        assert topic == "rating-events"
        assert events_per_second > 0
        stats.attempted = len(events)
        stats.acknowledged = len(events)
        stats.started_monotonic = 1.0
        stats.ended_monotonic = 2.0

    monkeypatch.setattr(producer_module, "kafka_producer", lambda *_args: object())
    monkeypatch.setattr(producer_module, "publish", fake_publish)
    producer_module.STOP_REQUESTED.clear()
    common = [
        "--dataset-path",
        str(dataset),
        "--source",
        "fixture",
        "--replay-run-id",
        "unit-run",
        "--max-records",
        "1",
    ]

    assert main([*common, "--statistics-path", str(statistics)]) == 0
    assert '"acknowledged": 1' in statistics.read_text(encoding="utf-8")
    assert main([*common, "--start-record", "-1"]) == 2
