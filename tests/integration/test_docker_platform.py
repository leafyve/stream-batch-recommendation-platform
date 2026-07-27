from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def test_compose_services_are_healthy_and_topics_are_configured() -> None:
    run(["docker", "compose", "config", "--quiet"])
    services = {
        row["Service"]: row
        for row in (
            json.loads(line)
            for line in run(
                ["docker", "compose", "ps", "--format", "json"]
            ).stdout.splitlines()
            if line.strip()
        )
    }
    required = {
        "redpanda",
        "console",
        "minio",
        "iceberg-rest",
        "spark-master",
        "spark-worker",
    }
    assert required <= services.keys()
    for name in required:
        assert services[name]["State"] == "running"
        assert services[name].get("Health", "healthy") == "healthy"

    for topic in ("rating-events", "rating-events-dlq"):
        payload = json.loads(
            run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "redpanda",
                    "rpk",
                    "topic",
                    "describe",
                    topic,
                    "-p",
                    "--format",
                    "json",
                ]
            ).stdout
        )[0]
        assert payload["summary"]["partitions"] == 3
        configs = {row["key"]: row["value"] for row in payload["configs"]}
        assert configs["cleanup.policy"] == "delete"
        assert configs["retention.ms"] == "604800000"


def test_all_iceberg_tables_are_readable_and_logically_unique() -> None:
    run(
        [
            sys.executable,
            "scripts/task.py",
            "probe",
            "--output",
            "integration-test-probe.json",
        ]
    )
    payload = cast(
        dict[str, Any],
        json.loads(
            (ROOT / "reports" / "runtime" / "integration-test-probe.json").read_text(
                encoding="utf-8"
            )
        ),
    )
    assert payload["status"] == "pass"
    assert len(payload["table_counts"]) == 8
    assert payload["valid_event_uniqueness"]["duplicate_event_ids"] == 0
    assert payload["invalid_event_uniqueness"]["duplicate_invalid_record_ids"] == 0
    for table in (
        "user_stream_features",
        "movie_stream_features",
        "user_batch_features",
        "movie_batch_features",
    ):
        assert payload[table]["duplicate_logical_keys"] == 0
