from __future__ import annotations

from pathlib import Path

import pytest

from recommendation_features.config import (
    ConfigurationError,
    choose,
    load_yaml,
    make_producer_config,
)


def base() -> dict[str, object]:
    return {
        "producer": {
            "bootstrap_servers": "file:9092",
            "events_per_second": 10,
            "batch_size": 20,
            "max_records": 30,
            "duplicate_percentage": 1,
            "out_of_order_percentage": 2,
            "out_of_order_delay_seconds": 3,
            "random_seed": 4,
        },
        "topics": {"ratings": "file-topic"},
    }


def test_choose_precedence() -> None:
    assert choose("cli", "VALUE", "file", "default", environ={"VALUE": "env"}) == "cli"
    assert choose(None, "VALUE", "file", "default", environ={"VALUE": "env"}) == "env"
    assert choose(None, "VALUE", "file", "default", environ={}) == "file"
    assert choose(None, "VALUE", None, "default", environ={}) == "default"


def test_producer_config_resolves_and_casts_environment() -> None:
    result = make_producer_config(
        base(),
        max_records=None,
        environ={"EVENTS_PER_SECOND": "42.5", "RATING_TOPIC": "environment-topic"},
    )
    assert result.events_per_second == 42.5
    assert result.topic == "environment-topic"
    assert result.random_seed == 4


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"events_per_second": 0}, "positive"),
        ({"batch_size": 0}, "positive"),
        ({"max_records": -1}, "max_records"),
        ({"duplicate_percentage": 101}, "between 0 and 100"),
        ({"out_of_order_percentage": -1}, "between 0 and 100"),
        ({"out_of_order_delay_seconds": -1}, "must not be negative"),
        ({"bootstrap_servers": " "}, "non-empty"),
    ],
)
def test_invalid_producer_settings(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        make_producer_config(base(), environ={}, **overrides)  # type: ignore[arg-type]


def test_bad_yaml_sources_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="does not exist"):
        load_yaml(tmp_path / "missing.yaml")
    target = tmp_path / "bad.yaml"
    target.write_text("- list\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="mapping"):
        load_yaml(target)


def test_bad_sections_are_rejected() -> None:
    with pytest.raises(ConfigurationError, match="sections"):
        make_producer_config({"producer": [], "topics": []}, environ={})

