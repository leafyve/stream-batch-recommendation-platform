"""Validated configuration loading with explicit precedence."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = ROOT / "configs" / "pipeline.yaml"


class ConfigurationError(ValueError):
    """Raised when pipeline configuration is missing or invalid."""


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str
    topic: str
    events_per_second: float
    batch_size: int
    max_records: int | None
    duplicate_percentage: float
    out_of_order_percentage: float
    out_of_order_delay_seconds: int
    random_seed: int


def load_yaml(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load the repository YAML configuration."""
    if not path.is_file():
        raise ConfigurationError(f"Configuration file does not exist: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ConfigurationError("Pipeline configuration must be a mapping")
    return raw


def choose(
    cli_value: Any,
    env_name: str,
    file_value: Any,
    default: Any,
    *,
    environ: Mapping[str, str] | None = None,
) -> Any:
    """Apply CLI, environment, file, built-in precedence."""
    if cli_value is not None:
        return cli_value
    source = os.environ if environ is None else environ
    if env_name in source:
        return source[env_name]
    if file_value is not None:
        return file_value
    return default


def _percentage(value: float, name: str) -> float:
    if not 0.0 <= value <= 100.0:
        raise ConfigurationError(f"{name} must be between 0 and 100")
    return value


def make_producer_config(
    file_config: Mapping[str, Any],
    *,
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    events_per_second: float | None = None,
    batch_size: int | None = None,
    max_records: int | None = None,
    duplicate_percentage: float | None = None,
    out_of_order_percentage: float | None = None,
    out_of_order_delay_seconds: int | None = None,
    random_seed: int | None = None,
    environ: Mapping[str, str] | None = None,
) -> ProducerConfig:
    """Resolve and validate producer settings."""
    producer = file_config.get("producer", {})
    topics = file_config.get("topics", {})
    if not isinstance(producer, Mapping) or not isinstance(topics, Mapping):
        raise ConfigurationError("producer and topics sections must be mappings")

    resolved = ProducerConfig(
        bootstrap_servers=str(
            choose(
                bootstrap_servers,
                "REDPANDA_BOOTSTRAP_SERVERS",
                producer.get("bootstrap_servers"),
                "redpanda:9092",
                environ=environ,
            )
        ),
        topic=str(
            choose(topic, "RATING_TOPIC", topics.get("ratings"), "rating-events", environ=environ)
        ),
        events_per_second=float(
            choose(
                events_per_second,
                "EVENTS_PER_SECOND",
                producer.get("events_per_second"),
                100.0,
                environ=environ,
            )
        ),
        batch_size=int(
            choose(
                batch_size,
                "PRODUCER_BATCH_SIZE",
                producer.get("batch_size"),
                100,
                environ=environ,
            )
        ),
        max_records=(
            int(
                choose(
                    max_records,
                    "MAX_RECORDS",
                    producer.get("max_records"),
                    1000,
                    environ=environ,
                )
            )
            if choose(
                max_records,
                "MAX_RECORDS",
                producer.get("max_records"),
                1000,
                environ=environ,
            )
            is not None
            else None
        ),
        duplicate_percentage=float(
            choose(
                duplicate_percentage,
                "DUPLICATE_PERCENTAGE",
                producer.get("duplicate_percentage"),
                0.0,
                environ=environ,
            )
        ),
        out_of_order_percentage=float(
            choose(
                out_of_order_percentage,
                "OUT_OF_ORDER_PERCENTAGE",
                producer.get("out_of_order_percentage"),
                0.0,
                environ=environ,
            )
        ),
        out_of_order_delay_seconds=int(
            choose(
                out_of_order_delay_seconds,
                "OUT_OF_ORDER_DELAY_SECONDS",
                producer.get("out_of_order_delay_seconds"),
                7200,
                environ=environ,
            )
        ),
        random_seed=int(
            choose(
                random_seed,
                "RANDOM_SEED",
                producer.get("random_seed"),
                169849,
                environ=environ,
            )
        ),
    )
    if not resolved.bootstrap_servers.strip() or not resolved.topic.strip():
        raise ConfigurationError("bootstrap_servers and topic must be non-empty")
    if resolved.events_per_second <= 0 or resolved.batch_size <= 0:
        raise ConfigurationError("events_per_second and batch_size must be positive")
    if resolved.max_records is not None and resolved.max_records <= 0:
        raise ConfigurationError("max_records must be positive when specified")
    if resolved.out_of_order_delay_seconds < 0:
        raise ConfigurationError("out_of_order_delay_seconds must not be negative")
    _percentage(resolved.duplicate_percentage, "duplicate_percentage")
    _percentage(resolved.out_of_order_percentage, "out_of_order_percentage")
    return resolved
