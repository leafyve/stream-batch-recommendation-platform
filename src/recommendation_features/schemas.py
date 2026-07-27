"""Versioned rating-event contract and deterministic identity."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

SCHEMA_VERSION = 1
MAX_SIGNED_INT64 = 2**63 - 1


class EventValidationError(ValueError):
    """Raised when a rating event violates schema v1."""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = errors


def parse_utc(value: str | datetime) -> datetime:
    """Parse a timestamp and normalise it to timezone-aware UTC."""
    if isinstance(value, datetime):
        parsed = value
    else:
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError("timestamp is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def utc_iso(value: str | datetime) -> str:
    """Return canonical millisecond-free UTC text."""
    return parse_utc(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def normalise_rating(value: float | int | str | Decimal) -> str:
    """Return a stable one-decimal rating representation."""
    try:
        rating = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("rating must be numeric") from exc
    if rating < Decimal("0.5") or rating > Decimal("5.0"):
        raise ValueError("rating must be between 0.5 and 5.0")
    if rating * 2 != (rating * 2).to_integral_value():
        raise ValueError("rating must use half-star increments")
    return f"{rating.quantize(Decimal('0.0'))}"


def deterministic_event_id(
    *,
    source: str,
    schema_version: int,
    user_id: int,
    movie_id: int,
    rating: float | int | str | Decimal,
    event_time: str | datetime,
) -> str:
    """Hash the canonical source-rating identity, excluding replay metadata."""
    canonical = "\x1f".join(
        (
            source.strip(),
            str(schema_version),
            str(user_id),
            str(movie_id),
            normalise_rating(rating),
            utc_iso(event_time),
        )
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RatingEvent:
    event_id: str
    schema_version: int
    user_id: int
    movie_id: int
    rating: float
    event_time: str
    ingestion_time: str
    source: str
    replay_run_id: str

    @classmethod
    def create(
        cls,
        *,
        user_id: int,
        movie_id: int,
        rating: float,
        event_time: str | datetime,
        ingestion_time: str | datetime,
        source: str,
        replay_run_id: str,
        schema_version: int = SCHEMA_VERSION,
    ) -> RatingEvent:
        event = cls(
            event_id=deterministic_event_id(
                source=source,
                schema_version=schema_version,
                user_id=user_id,
                movie_id=movie_id,
                rating=rating,
                event_time=event_time,
            ),
            schema_version=schema_version,
            user_id=user_id,
            movie_id=movie_id,
            rating=float(normalise_rating(rating)),
            event_time=utc_iso(event_time),
            ingestion_time=utc_iso(ingestion_time),
            source=source.strip(),
            replay_run_id=replay_run_id.strip(),
        )
        event.validate()
        return event

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> RatingEvent:
        required = (
            "event_id",
            "schema_version",
            "user_id",
            "movie_id",
            "rating",
            "event_time",
            "ingestion_time",
            "source",
            "replay_run_id",
        )
        missing = [field for field in required if field not in value]
        if missing:
            raise EventValidationError([f"missing field: {field}" for field in missing])
        try:
            event = cls(
                event_id=str(value["event_id"]),
                schema_version=int(value["schema_version"]),
                user_id=int(value["user_id"]),
                movie_id=int(value["movie_id"]),
                rating=float(value["rating"]),
                event_time=str(value["event_time"]),
                ingestion_time=str(value["ingestion_time"]),
                source=str(value["source"]),
                replay_run_id=str(value["replay_run_id"]),
            )
        except (TypeError, ValueError) as exc:
            raise EventValidationError(["field types are invalid"]) from exc
        event.validate()
        return replace(
            event,
            rating=float(normalise_rating(event.rating)),
            event_time=utc_iso(event.event_time),
            ingestion_time=utc_iso(event.ingestion_time),
            source=event.source.strip(),
            replay_run_id=event.replay_run_id.strip(),
        )

    def validate(self) -> None:
        errors: list[str] = []
        if self.schema_version != SCHEMA_VERSION:
            errors.append(f"unsupported schema_version: {self.schema_version}")
        for field_name, identifier in (("user_id", self.user_id), ("movie_id", self.movie_id)):
            if identifier <= 0 or identifier > MAX_SIGNED_INT64:
                errors.append(f"{field_name} must be a positive signed 64-bit integer")
        try:
            normalise_rating(self.rating)
        except ValueError as exc:
            errors.append(str(exc))
        for field_name, timestamp_value in (
            ("event_time", self.event_time),
            ("ingestion_time", self.ingestion_time),
        ):
            try:
                parse_utc(timestamp_value)
            except ValueError as exc:
                errors.append(f"{field_name}: {exc}")
        if not self.source.strip():
            errors.append("source must be non-empty")
        if not self.replay_run_id.strip():
            errors.append("replay_run_id must be non-empty")
        if len(self.event_id) != 64:
            errors.append("event_id must be a SHA-256 hex digest")
        else:
            try:
                int(self.event_id, 16)
            except ValueError:
                errors.append("event_id must be hexadecimal")
        if not errors:
            expected = deterministic_event_id(
                source=self.source,
                schema_version=self.schema_version,
                user_id=self.user_id,
                movie_id=self.movie_id,
                rating=self.rating,
                event_time=self.event_time,
            )
            if self.event_id != expected:
                errors.append("event_id does not match canonical event fields")
        if errors:
            raise EventValidationError(errors)

    def to_json(self) -> bytes:
        """Serialize to stable compact UTF-8 JSON."""
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode("utf-8")
