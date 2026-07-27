"""Pure-Python validation used by producer, batch, and tests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from recommendation_features.schemas import EventValidationError, RatingEvent


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    event: RatingEvent | None
    errors: tuple[str, ...]


def validate_payload(payload: bytes | str) -> ValidationResult:
    """Parse and validate one event without throwing for data-quality errors."""
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    except UnicodeDecodeError:
        return ValidationResult(False, None, ("payload is not valid UTF-8",))
    try:
        value: Any = json.loads(text)
    except json.JSONDecodeError as exc:
        return ValidationResult(False, None, (f"invalid JSON: {exc.msg}",))
    if not isinstance(value, dict):
        return ValidationResult(False, None, ("JSON payload must be an object",))
    try:
        event = RatingEvent.from_mapping(value)
    except EventValidationError as exc:
        return ValidationResult(False, None, tuple(exc.errors))
    return ValidationResult(True, event, ())

