"""Automate the complete MovieLens 100K process-restart recovery proof."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
TASK = [sys.executable, str(ROOT / "scripts" / "task.py")]
PROGRESS = ROOT / "reports" / "runtime" / "streaming_batches.jsonl"
EVIDENCE = ROOT / "reports" / "evidence" / "recovery.json"
QUERY_NAMES = {
    "raw_valid_ingestion",
    "raw_invalid_ingestion",
    "user_feature_aggregation",
    "movie_feature_aggregation",
}


def run(command: list[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
    print("+", subprocess.list2cmdline(command), flush=True)
    return subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=capture,
    )


def task(*arguments: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return run([*TASK, *arguments], capture=capture)


def latest_query_progress() -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if not PROGRESS.is_file():
        return latest
    for line in PROGRESS.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = row.get("name")
        if name in QUERY_NAMES and row.get("listener_event") == "progress":
            latest[str(name)] = row
    return latest


def started_event_count() -> int:
    if not PROGRESS.is_file():
        return 0
    count = 0
    for line in PROGRESS.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("listener_event") == "started":
            count += 1
    return count


def wait_for_query_start(
    previous_started_count: int,
    *,
    timeout_seconds: int = 240,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        state = run(
            [
                "docker",
                "inspect",
                "-f",
                "{{.State.Status}}:{{.RestartCount}}",
                "tiktok-se-intern-streaming-job-1",
            ],
            capture=True,
        ).stdout.strip()
        if state.startswith("running") and started_event_count() >= previous_started_count + 4:
            return
        print(f"waiting for restarted query runs container={state}", flush=True)
        time.sleep(2)
    raise RuntimeError("restarted streaming queries did not start before timeout")


def produce_segment(
    *,
    start: int,
    count: int,
    run_id: str,
    statistics_name: str,
) -> dict[str, Any]:
    task(
        "produce-dev",
        "--profile",
        "ml-100k",
        "--start-record",
        str(start),
        "--max-records",
        str(count),
        "--events-per-second",
        "3000",
        "--duplicate-percentage",
        "5",
        "--out-of-order-percentage",
        "10",
        "--out-of-order-delay-seconds",
        "7200",
        "--replay-run-id",
        run_id,
        "--statistics-name",
        statistics_name,
    )
    return json.loads(
        (ROOT / "reports" / "runtime" / statistics_name).read_text(encoding="utf-8")
    )


def inspect_container() -> dict[str, Any]:
    output = run(
        [
            "docker",
            "inspect",
            "-f",
            '{"id":"{{.Id}}","started_at":"{{.State.StartedAt}}",'
            '"status":"{{.State.Status}}","restart_count":{{.RestartCount}}}',
            "tiktok-se-intern-streaming-job-1",
        ],
        capture=True,
    ).stdout
    return json.loads(output)


def main() -> int:
    started_at = datetime.now(timezone.utc)
    run(["docker", "compose", "--profile", "jobs", "build"])
    task("up")
    task("create-topics")
    task("init-tables")
    task("run-stream")

    segment_one = produce_segment(
        start=0,
        count=25_000,
        run_id="recovery-segment-one",
        statistics_name="recovery-segment-one.json",
    )
    run([sys.executable, str(ROOT / "scripts" / "wait_for_stream.py")])
    before_progress = latest_query_progress()
    previous_run_ids = {
        name: str(before_progress[name]["runId"]) for name in sorted(QUERY_NAMES)
    }
    previous_started_count = started_event_count()
    checkpoint_files = run(
        [
            "docker",
            "compose",
            "--profile",
            "jobs",
            "exec",
            "-T",
            "streaming-job",
            "find",
            "/checkpoints",
            "-type",
            "f",
        ],
        capture=True,
    ).stdout.splitlines()
    before_container = inspect_container()

    restart_started = time.monotonic()
    run(["docker", "kill", "--signal=SIGKILL", "tiktok-se-intern-streaming-job-1"])
    run(["docker", "compose", "--profile", "jobs", "up", "-d", "streaming-job"])
    wait_for_query_start(previous_started_count)
    recovery_seconds = time.monotonic() - restart_started
    after_container = inspect_container()

    segment_two = produce_segment(
        start=25_000,
        count=75_000,
        run_id="recovery-segment-two",
        statistics_name="recovery-segment-two.json",
    )
    run([sys.executable, str(ROOT / "scripts" / "wait_for_stream.py")])
    restarted_progress = latest_query_progress()
    if any(
        restarted_progress[name].get("runId") == previous_run_ids[name]
        for name in QUERY_NAMES
    ):
        raise RuntimeError("one or more queries did not resume under a new run ID")
    run(["docker", "compose", "--profile", "jobs", "stop", "-t", "60", "streaming-job"])

    task("run-batch", "--profile", "ml-100k")
    task("run-batch", "--profile", "ml-100k")
    parity = task("parity", capture=True)
    print(parity.stdout)
    parity_results = json.loads(
        (ROOT / "reports" / "runtime" / "parity.json").read_text(encoding="utf-8")
    )
    task("probe", "--output", "recovery-probe-before-storage-restart.json")
    probe_before = json.loads(
        (
            ROOT
            / "reports"
            / "runtime"
            / "recovery-probe-before-storage-restart.json"
        ).read_text(encoding="utf-8")
    )

    run(["docker", "compose", "restart", "minio", "iceberg-rest"])
    run(["docker", "compose", "up", "-d", "--wait", "minio", "iceberg-rest"])
    task("probe", "--output", "recovery-probe-after-storage-restart.json")
    probe_after = json.loads(
        (
            ROOT
            / "reports"
            / "runtime"
            / "recovery-probe-after-storage-restart.json"
        ).read_text(encoding="utf-8")
    )
    if probe_before["table_counts"] != probe_after["table_counts"]:
        raise RuntimeError("table counts changed across MinIO/catalog restart")

    evidence = {
        "status": "pass",
        "command": "py -3.11 scripts/task.py recovery-test",
        "environment": "local Docker Desktop Linux containers",
        "dataset": {
            "name": "ml-100k",
            "md5": "0e33842e24a9c977be4e0107933c0723",
            "source_records": 100_000,
        },
        "configuration": {
            "segment_one_records": 25_000,
            "segment_two_records": 75_000,
            "target_events_per_second": 3000,
            "duplicate_percentage": 5,
            "out_of_order_percentage": 10,
            "out_of_order_delay_seconds": 7200,
        },
        "segments": [segment_one, segment_two],
        "checkpoint_file_count_before_kill": len(checkpoint_files),
        "query_run_ids_before_kill": previous_run_ids,
        "query_run_ids_after_restart": {
            name: row["runId"] for name, row in restarted_progress.items()
        },
        "streaming_container_before": before_container,
        "streaming_container_after": after_container,
        "process_restart_recovery_seconds": recovery_seconds,
        "probe_before_storage_restart": probe_before,
        "probe_after_storage_restart": probe_after,
        "storage_restart_counts_preserved": True,
        "parity_results": parity_results,
        "runs": 1,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "started_at_utc": started_at.isoformat(),
        "limitations": [
            "This proves one streaming-process/container restart only.",
            "It does not prove broker failure, cluster failover, or high availability.",
        ],
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
