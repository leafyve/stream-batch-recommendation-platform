from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from recommendation_features.schemas import (
    EventValidationError,
    RatingEvent,
    deterministic_event_id,
    normalise_rating,
    parse_utc,
)
from recommendation_features.validation import validate_payload

NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def event(**overrides: object) -> RatingEvent:
    values: dict[str, object] = {
        "user_id": 1,
        "movie_id": 2,
        "rating": 4.5,
        "event_time": "2026-01-01T00:00:00Z",
        "ingestion_time": NOW,
        "source": "synthetic-test",
        "replay_run_id": "run-a",
    }
    values.update(overrides)
    return RatingEvent.create(**values)  # type: ignore[arg-type]


def test_valid_event_round_trip_is_accepted() -> None:
    original = event()
    result = validate_payload(original.to_json())
    assert result.valid
    assert result.event == original
    assert result.errors == ()


def test_invalid_json_and_non_object_are_data_errors() -> None:
    assert validate_payload(b"{").errors == (
        "invalid JSON: Expecting property name enclosed in double quotes",
    )
    assert validate_payload("[]").errors == ("JSON payload must be an object",)
    assert validate_payload(b"\xff").errors == ("payload is not valid UTF-8",)


def test_invalid_rating_is_rejected() -> None:
    with pytest.raises(ValueError, match="half-star"):
        event(rating=4.2)
    raw = json.loads(event().to_json())
    raw["rating"] = 6.0
    result = validate_payload(json.dumps(raw))
    assert not result.valid
    assert "rating must be between 0.5 and 5.0" in result.errors


def test_unsupported_schema_version_and_invalid_ids_are_rejected() -> None:
    with pytest.raises(EventValidationError) as captured:
        event(schema_version=2, user_id=0)
    assert "unsupported schema_version: 2" in captured.value.errors
    assert "user_id must be a positive signed 64-bit integer" in captured.value.errors


def test_event_id_is_stable_across_replays_and_ingestion_times() -> None:
    first = event(replay_run_id="first", ingestion_time="2026-01-02T00:00:00Z")
    second = event(replay_run_id="second", ingestion_time="2026-02-03T00:00:00Z")
    assert first.event_id == second.event_id
    assert first.event_id == deterministic_event_id(
        source="synthetic-test",
        schema_version=1,
        user_id=1,
        movie_id=2,
        rating="4.50",
        event_time="2026-01-01T00:00:00+00:00",
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0.5, "0.5"), (5, "5.0"), ("3.50", "3.5")],
)
def test_rating_canonicalisation(value: object, expected: str) -> None:
    assert normalise_rating(value) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize("value", ["0", "5.1", "nan-text"])
def test_bad_rating_text(value: str) -> None:
    with pytest.raises(ValueError):
        normalise_rating(value)


def test_timestamps_require_offsets_and_normalise_to_utc() -> None:
    assert parse_utc("2026-01-01T08:00:00+08:00") == datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="UTC offset"):
        parse_utc("2026-01-01T00:00:00")


def test_mapping_reports_missing_fields_and_type_errors() -> None:
    with pytest.raises(EventValidationError, match="missing field"):
        RatingEvent.from_mapping({"event_id": "x"})
    raw = json.loads(event().to_json())
    raw["user_id"] = "not-an-id"
    with pytest.raises(EventValidationError, match="field types"):
        RatingEvent.from_mapping(raw)


def test_mapping_rejects_tampered_identity_and_empty_metadata() -> None:
    raw = json.loads(event().to_json())
    raw["event_id"] = "0" * 64
    raw["source"] = " "
    raw["replay_run_id"] = ""
    with pytest.raises(EventValidationError) as captured:
        RatingEvent.from_mapping(raw)
    assert "source must be non-empty" in captured.value.errors
    assert "replay_run_id must be non-empty" in captured.value.errors
    assert "event_id does not match canonical event fields" not in captured.value.errors
