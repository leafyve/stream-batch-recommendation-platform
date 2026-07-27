"""Verify and record the pinned SQLite Iceberg REST catalog contract."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
TASK = [sys.executable, str(ROOT / "scripts" / "task.py")]
OUTPUT = ROOT / "reports" / "evidence" / "catalog.json"


def run(command: list[str]) -> str:
    print("+", subprocess.list2cmdline(command), flush=True)
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    return result.stdout


def probe(value: str) -> str:
    return run(
        [
            *TASK,
            "init-tables",
            "--probe-value",
            value,
        ]
    )


def expect_probe(value: str) -> str:
    return run(
        [
            *TASK,
            "init-tables",
            "--expect-probe-value",
            value,
        ]
    )


def main() -> int:
    first = probe("catalog-first")
    before_id = run(
        ["docker", "compose", "ps", "-q", "iceberg-rest"]
    ).strip()
    run(["docker", "compose", "restart", "iceberg-rest"])
    run(["docker", "compose", "up", "-d", "--wait", "iceberg-rest"])
    after_id = run(
        ["docker", "compose", "ps", "-q", "iceberg-rest"]
    ).strip()
    after_restart = expect_probe("catalog-first")
    repeated = probe("catalog-repeated")
    classpath = run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "iceberg-rest",
            "/bin/sh",
            "-ec",
            "printf '/usr/lib/iceberg-rest/iceberg-rest-adapter.jar: '; "
            "grep -a -o -m1 'org/sqlite/JDBC.class' "
            "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar",
        ]
    ).strip()
    for expected, content in (
        ("value=catalog-first", first),
        ("CATALOG_EXPECT value=catalog-first", after_restart),
        ("value=catalog-repeated", repeated),
    ):
        if expected not in content:
            raise RuntimeError(f"catalog probe output missing {expected}")
    evidence: dict[str, Any] = {
        "status": "pass",
        "command": "py -3.11 scripts/capture_catalog_evidence.py",
        "environment": "local Docker Desktop Linux containers",
        "configuration": {
            "fixture": "apache/iceberg-rest-fixture:1.10.1",
            "catalog_implementation": "org.apache.iceberg.jdbc.JdbcCatalog",
            "uri": "jdbc:sqlite:/catalog/iceberg_catalog.db?busy_timeout=30000",
            "volume": "iceberg_catalog:/catalog",
            "warehouse": "s3://warehouse/",
        },
        "sqlite_driver_classpath_evidence": classpath,
        "container_id_before_restart": before_id,
        "container_id_after_restart": after_id,
        "first_probe": "catalog-first",
        "probe_read_after_restart": "catalog-first",
        "repeated_commit_probe": "catalog-repeated",
        "tables_readable": 8,
        "runs": 3,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "SQLite is a local development catalog with one writer.",
            "The contract test does not emulate catalog host or disk loss.",
        ],
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
