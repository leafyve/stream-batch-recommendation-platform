"""Create compact evidence for validation, DLQ, and late-event contracts."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "reports" / "runtime"
OUTPUT = ROOT / "reports" / "evidence" / "contracts.json"
PROGRESS = RUNTIME / "streaming_batches.jsonl"


def load_json(name: str) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads((RUNTIME / name).read_text(encoding="utf-8")))


def dlq_partitions() -> list[dict[str, Any]]:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "redpanda",
            "rpk",
            "topic",
            "describe",
            "rating-events-dlq",
            "-p",
            "--format",
            "json",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload: list[dict[str, Any]] = json.loads(result.stdout)
    return [
        {
            "partition": int(row["partition"]),
            "log_start_offset": int(row["log_start_offset"]),
            "high_watermark": int(row["high_watermark"]),
        }
        for row in payload[0]["partitions"]
    ]


def late_event_progress() -> dict[str, dict[str, Any]]:
    matches: dict[str, dict[str, Any]] = {}
    for line in PROGRESS.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = row.get("name")
        if (
            name not in {"user_feature_aggregation", "movie_feature_aggregation"}
            or row.get("listener_event") != "progress"
            or row.get("numInputRows") != 3
        ):
            continue
        dropped = sum(
            int(operator.get("numRowsDroppedByWatermark", 0))
            for operator in row.get("stateOperators", [])
        )
        if dropped:
            matches[str(name)] = {
                "batch_id": int(row["batchId"]),
                "event_time": row.get("eventTime", {}),
                "num_input_rows": int(row["numInputRows"]),
                "num_rows_dropped_by_watermark": dropped,
                "source_end_offset": row["sources"][0]["endOffset"],
            }
    return matches


def main() -> int:
    before = load_json("contract-probe-before.json")
    after = load_json("contract-probe-after.json")
    partitions = dlq_partitions()
    late_progress = late_event_progress()
    before_counts = before["table_counts"]
    after_counts = after["table_counts"]
    evidence = {
        "status": "pass",
        "command": (
            "py -3.11 scripts/inject_contract_events.py; "
            "py -3.11 scripts/capture_contract_evidence.py"
        ),
        "environment": "local Docker Desktop Linux containers",
        "dataset": {
            "name": (
                "MovieLens 1M plus one watermark-finalization marker and "
                "three synthetic schema-contract records"
            ),
            "md5": "c4d9eecfca2ab87c1945afe126590906",
        },
        "configuration": {
            "watermark_finalization_marker_records": 1,
            "published_records": 3,
            "malformed_json_records": 1,
            "invalid_rating_records": 1,
            "deliberately_late_valid_records": 1,
            "late_event_time_utc": "1990-01-01T00:00:00Z",
        },
        "before_table_counts": before_counts,
        "after_table_counts": after_counts,
        "observed": {
            "new_valid_rows": (
                after_counts["local.raw.valid_rating_events"]
                - before_counts["local.raw.valid_rating_events"]
            ),
            "new_invalid_rows": (
                after_counts["local.raw.invalid_rating_events"]
                - before_counts["local.raw.invalid_rating_events"]
            ),
            "unique_invalid_record_ids": after["invalid_event_uniqueness"][
                "unique_invalid_record_ids"
            ],
            "duplicate_invalid_record_ids": after["invalid_event_uniqueness"][
                "duplicate_invalid_record_ids"
            ],
            "user_feature_row_change": (
                after_counts["local.features.user_stream_features"]
                - before_counts["local.features.user_stream_features"]
            ),
            "movie_feature_row_change": (
                after_counts["local.features.movie_stream_features"]
                - before_counts["local.features.movie_stream_features"]
            ),
            "late_event_query_progress": late_progress,
            "dlq_partitions": partitions,
            "dlq_messages": sum(
                row["high_watermark"] - row["log_start_offset"] for row in partitions
            ),
        },
        "catalog_contention_regression": {
            "diagnosis": "Concurrent REST commits returned SQLITE_BUSY from the SQLite catalog.",
            "fix": (
                "Catalog commits are serialized by the stream driver and the JDBC URL uses "
                "busy_timeout=30000 so the REST fixture waits for SQLite's single writer."
            ),
            "checkpoint_retry_output_rows": 2,
        },
        "runs": 1,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "Synthetic contract records verify routing behavior, not arbitrary corrupt payloads.",
            "Kafka DLQ output is at-least-once; consumers must deduplicate by invalid_record_id.",
        ],
    }
    if (
        evidence["observed"]["new_valid_rows"] != 1
        or evidence["observed"]["new_invalid_rows"] != 2
        or evidence["observed"]["dlq_messages"] != 2
        or evidence["observed"]["user_feature_row_change"] != 0
        or evidence["observed"]["movie_feature_row_change"] != 0
        or set(late_progress)
        != {"user_feature_aggregation", "movie_feature_aggregation"}
    ):
        raise RuntimeError("contract evidence does not satisfy the required assertions")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
