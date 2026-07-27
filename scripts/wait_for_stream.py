"""Wait for every named query to commit the current Redpanda high-watermarks."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PROGRESS = ROOT / "reports" / "runtime" / "streaming_batches.jsonl"
QUERY_NAMES = {
    "raw_valid_ingestion",
    "raw_invalid_ingestion",
    "user_feature_aggregation",
    "movie_feature_aggregation",
}


def high_watermarks() -> dict[str, int]:
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
            "rating-events",
            "-p",
            "--format",
            "json",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    return {
        str(row["partition"]): int(row["high_watermark"])
        for row in payload[0]["partitions"]
    }


def latest_offsets() -> dict[str, dict[str, int]]:
    latest: dict[str, dict[str, int]] = {}
    if not PROGRESS.is_file():
        return latest
    for line in PROGRESS.read_text(encoding="utf-8").splitlines():
        try:
            row: dict[str, Any] = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = row.get("name")
        if name not in QUERY_NAMES or row.get("listener_event") != "progress":
            continue
        sources = row.get("sources", [])
        if not sources:
            continue
        end_offset = sources[0].get("endOffset", {}).get("rating-events", {})
        latest[str(name)] = {str(key): int(value) for key, value in end_offset.items()}
    return latest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    args = parser.parse_args()
    target = high_watermarks()
    deadline = time.monotonic() + args.timeout_seconds
    while time.monotonic() < deadline:
        observed = latest_offsets()
        caught_up = {
            name: offsets == target for name, offsets in observed.items() if name in QUERY_NAMES
        }
        if set(observed) == QUERY_NAMES and all(caught_up.values()):
            print(
                json.dumps(
                    {
                        "status": "pass",
                        "target_high_watermarks": target,
                        "query_end_offsets": observed,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        container = subprocess.run(
            [
                "docker",
                "inspect",
                "-f",
                "{{.State.Status}}:{{.RestartCount}}",
                "tiktok-se-intern-streaming-job-1",
            ],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        print(
            f"waiting container={container} target={target} "
            f"caught_up={caught_up}",
            flush=True,
        )
        if container.startswith("exited"):
            break
        time.sleep(args.poll_seconds)
    print(
        json.dumps(
            {
                "status": "fail",
                "target_high_watermarks": target,
                "query_end_offsets": latest_offsets(),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
