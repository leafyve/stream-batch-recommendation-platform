"""Run the reproducible MovieLens 1M local Docker benchmark."""

from __future__ import annotations

import json
import math
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import psutil  # type: ignore[import-untyped]
import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]
TASK = [sys.executable, str(ROOT / "scripts" / "task.py")]
WAIT = [sys.executable, str(ROOT / "scripts" / "wait_for_stream.py")]
RUNTIME = ROOT / "reports" / "runtime"
PROGRESS = RUNTIME / "streaming_batches.jsonl"
EVIDENCE = ROOT / "reports" / "evidence" / "benchmark.json"
SOURCE_RECORDS = 1_000_209
DISCOVERY_RATES = (500.0, 1500.0, 3000.0)
TRIAL_SOURCE_RECORDS = 10_000
MEASURED_TRIALS = 3
QUERY_NAMES = {
    "raw_valid_ingestion",
    "raw_invalid_ingestion",
    "user_feature_aggregation",
    "movie_feature_aggregation",
}


def run(
    command: list[str], *, capture: bool = False
) -> subprocess.CompletedProcess[str]:
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


def progress_line_count() -> int:
    if not PROGRESS.is_file():
        return 0
    with PROGRESS.open(encoding="utf-8") as source:
        return sum(1 for _line in source)


def progress_since(start_line: int) -> list[dict[str, Any]]:
    if not PROGRESS.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(PROGRESS.read_text(encoding="utf-8").splitlines()):
        if index < start_line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("listener_event") == "progress" and row.get("name") in QUERY_NAMES:
            rows.append(row)
    return rows


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile_value
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize_spark(rows: list[dict[str, Any]]) -> dict[str, Any]:
    input_rates = [
        float(row["inputRowsPerSecond"])
        for row in rows
        if float(row.get("inputRowsPerSecond", 0.0)) > 0
    ]
    processed_rates = [
        float(row["processedRowsPerSecond"])
        for row in rows
        if float(row.get("processedRowsPerSecond", 0.0)) > 0
    ]
    durations = [
        float(row.get("durationMs", {}).get("triggerExecution", 0.0))
        for row in rows
        if float(row.get("durationMs", {}).get("triggerExecution", 0.0)) > 0
    ]
    return {
        "progress_records": len(rows),
        "input_rows_per_second_median": (
            statistics.median(input_rates) if input_rates else 0.0
        ),
        "processed_rows_per_second_median": (
            statistics.median(processed_rates) if processed_rates else 0.0
        ),
        "micro_batch_duration_ms_p50": percentile(durations, 0.50),
        "micro_batch_duration_ms_p95": percentile(durations, 0.95),
    }


def latest_offsets() -> dict[str, dict[str, int]]:
    latest: dict[str, dict[str, int]] = {}
    if not PROGRESS.is_file():
        return latest
    for line in PROGRESS.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = row.get("name")
        if name not in QUERY_NAMES or row.get("listener_event") != "progress":
            continue
        sources = row.get("sources", [])
        if not sources:
            continue
        offsets = sources[0].get("endOffset", {}).get("rating-events", {})
        latest[str(name)] = {str(key): int(value) for key, value in offsets.items()}
    return latest


def high_watermarks() -> dict[str, int]:
    result = run(
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
        capture=True,
    )
    payload: list[dict[str, Any]] = json.loads(result.stdout)
    return {
        str(row["partition"]): int(row["high_watermark"])
        for row in payload[0]["partitions"]
    }


def backlog_at_acknowledgement() -> dict[str, int]:
    target = high_watermarks()
    observed = latest_offsets()
    return {
        name: sum(
            max(0, high_watermark - observed_offsets.get(partition, 0))
            for partition, high_watermark in target.items()
        )
        for name, observed_offsets in observed.items()
    }


def producer_statistics(name: str) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads((RUNTIME / name).read_text(encoding="utf-8")),
    )


def run_trial(
    *,
    phase: str,
    trial: int,
    start_record: int,
    source_records: int,
    target_rate: float,
    duplicate_percentage: float = 0.0,
    out_of_order_percentage: float = 0.0,
) -> dict[str, Any]:
    statistics_name = f"benchmark-{phase}-{trial}.json"
    first_progress_line = progress_line_count()
    acknowledged_at = time.monotonic()
    task(
        "produce-dev",
        "--profile",
        "ml-1m",
        "--start-record",
        str(start_record),
        "--max-records",
        str(source_records),
        "--events-per-second",
        str(target_rate),
        "--duplicate-percentage",
        str(duplicate_percentage),
        "--out-of-order-percentage",
        str(out_of_order_percentage),
        "--out-of-order-delay-seconds",
        "7200",
        "--replay-run-id",
        f"benchmark-{phase}-{trial}",
        "--statistics-name",
        statistics_name,
    )
    acknowledged_at = time.monotonic()
    backlog = backlog_at_acknowledgement()
    run([*WAIT, "--timeout-seconds", "1800"])
    caught_up_at = time.monotonic()
    rows = progress_since(first_progress_line)
    producer = producer_statistics(statistics_name)
    catch_up_seconds = caught_up_at - acknowledged_at
    return {
        "phase": phase,
        "trial": trial,
        "start_record": start_record,
        "source_records": source_records,
        "target_events_per_second": target_rate,
        "actual_events_per_second": producer["actual_events_per_second"],
        "attempted": producer["attempted"],
        "acknowledged": producer["acknowledged"],
        "injected_duplicates": producer["injected_duplicates"],
        "reordered_events": producer["reordered_events"],
        "backlog_at_last_producer_ack": backlog,
        "maximum_query_backlog_at_last_producer_ack": max(backlog.values(), default=0),
        "last_ack_to_all_queries_committed_seconds": catch_up_seconds,
        "spark": summarize_spark(rows),
    }


def assert_empty_platform() -> None:
    task("probe", "--output", "benchmark-empty-probe.json")
    probe = json.loads((RUNTIME / "benchmark-empty-probe.json").read_text(encoding="utf-8"))
    nonempty = {
        table: count for table, count in probe["table_counts"].items() if int(count) != 0
    }
    if nonempty:
        raise RuntimeError(
            "benchmark requires empty persistent volumes; run guarded clean first: "
            f"{nonempty}"
        )


def docker_resources() -> dict[str, Any]:
    info = json.loads(
        run(["docker", "info", "--format", "{{json .}}"], capture=True).stdout
    )
    stats_output = run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            '{"name":"{{.Name}}","cpu":"{{.CPUPerc}}","memory":"{{.MemUsage}}"}',
        ],
        capture=True,
    ).stdout
    stats = [json.loads(line) for line in stats_output.splitlines() if line.strip()]
    limits: dict[str, dict[str, int]] = {}
    for service in (
        "redpanda",
        "iceberg-rest",
        "minio",
        "spark-master",
        "spark-worker",
    ):
        container_id = run(
            ["docker", "compose", "ps", "-q", service], capture=True
        ).stdout.strip()
        inspection = json.loads(
            run(["docker", "inspect", container_id], capture=True).stdout
        )[0]
        limits[service] = {
            "nano_cpus": int(inspection["HostConfig"].get("NanoCpus", 0)),
            "memory_bytes": int(inspection["HostConfig"].get("Memory", 0)),
        }
    return {
        "docker_cpus": int(info["NCPU"]),
        "docker_memory_bytes": int(info["MemTotal"]),
        "docker_server_version": str(info["ServerVersion"]),
        "container_snapshot": stats,
        "container_limits": limits,
    }


def main() -> int:
    started_at = datetime.now(timezone.utc)
    task("up")
    task("create-topics")
    task("init-tables")
    assert_empty_platform()
    task("run-stream")

    next_record = 0
    discovery: list[dict[str, Any]] = []
    for index, rate in enumerate(DISCOVERY_RATES, start=1):
        result = run_trial(
            phase="discovery",
            trial=index,
            start_record=next_record,
            source_records=TRIAL_SOURCE_RECORDS,
            target_rate=rate,
        )
        next_record += TRIAL_SOURCE_RECORDS
        result["controlled_backlog"] = (
            result["last_ack_to_all_queries_committed_seconds"] <= 120.0
            and result["maximum_query_backlog_at_last_producer_ack"]
            <= TRIAL_SOURCE_RECORDS
        )
        discovery.append(result)

    sustainable = [
        row
        for row in discovery
        if row["controlled_backlog"]
        and row["actual_events_per_second"]
        >= row["target_events_per_second"] * 0.90
    ]
    if not sustainable:
        raise RuntimeError("no discovery rate met the controlled-backlog criterion")
    ceiling = float(max(row["target_events_per_second"] for row in sustainable))
    measured_rates = {
        "low": ceiling * 0.25,
        "medium": ceiling * 0.50,
        "high": ceiling,
    }

    measured: dict[str, list[dict[str, Any]]] = {}
    for label, rate in measured_rates.items():
        trials: list[dict[str, Any]] = []
        for trial in range(1, MEASURED_TRIALS + 1):
            trials.append(
                run_trial(
                    phase=label,
                    trial=trial,
                    start_record=next_record,
                    source_records=TRIAL_SOURCE_RECORDS,
                    target_rate=rate,
                )
            )
            next_record += TRIAL_SOURCE_RECORDS
        measured[label] = trials

    remaining = SOURCE_RECORDS - next_record
    if remaining <= 0:
        raise RuntimeError("benchmark trial allocation exhausted the MovieLens 1M dataset")
    full_remainder = run_trial(
        phase="full-dataset-remainder",
        trial=1,
        start_record=next_record,
        source_records=remaining,
        target_rate=ceiling,
        duplicate_percentage=2.0,
        out_of_order_percentage=5.0,
    )
    next_record += remaining
    if next_record != SOURCE_RECORDS:
        raise RuntimeError(f"benchmark used {next_record} source rows, expected {SOURCE_RECORDS}")

    run(["docker", "compose", "--profile", "jobs", "stop", "-t", "60", "streaming-job"])
    task("run-batch", "--profile", "ml-1m")
    task("parity")
    task("probe", "--output", "benchmark-final-probe.json")
    parity = json.loads((RUNTIME / "parity.json").read_text(encoding="utf-8"))
    probe = json.loads((RUNTIME / "benchmark-final-probe.json").read_text(encoding="utf-8"))
    raw_count = int(probe["table_counts"]["local.raw.valid_rating_events"])
    if raw_count != SOURCE_RECORDS:
        raise RuntimeError(f"deduplicated raw count {raw_count}, expected {SOURCE_RECORDS}")
    if any(result["status"] != "pass" for result in parity):
        raise RuntimeError("MovieLens 1M batch-stream parity did not pass")

    all_measured_trials = [trial for trials in measured.values() for trial in trials]
    all_rows = progress_since(0)
    recovery = json.loads(
        (ROOT / "reports" / "evidence" / "recovery.json").read_text(encoding="utf-8")
    )
    pipeline = yaml.safe_load((ROOT / "configs" / "pipeline.yaml").read_text(encoding="utf-8"))
    host_memory = psutil.virtual_memory()
    evidence = {
        "status": "pass",
        "command": (
            "py -3.11 scripts/task.py clean --volumes "
            "--confirm DELETE_PERSISTENT_DATA; "
            "py -3.11 scripts/task.py benchmark"
        ),
        "environment": "local Docker Desktop Linux containers",
        "git_revision": run(
            ["git", "rev-parse", "HEAD"],
            capture=True,
        ).stdout.strip(),
        "dataset": {
            "name": "MovieLens 1M",
            "md5": "c4d9eecfca2ab87c1945afe126590906",
            "source_records": SOURCE_RECORDS,
            "source_records_used": next_record,
        },
        "configuration": {
            "discovery_rates": list(DISCOVERY_RATES),
            "discovery_records_per_rate": TRIAL_SOURCE_RECORDS,
            "highest_sustainable_rate": ceiling,
            "measured_rates": measured_rates,
            "measured_trials_per_rate": MEASURED_TRIALS,
            "measured_records_per_trial": TRIAL_SOURCE_RECORDS,
            "full_remainder_duplicate_percentage": 2.0,
            "full_remainder_out_of_order_percentage": 5.0,
            "out_of_order_delay_seconds": 7200,
            "spark": pipeline["streaming"],
        },
        "discovery": discovery,
        "measured_trials": measured,
        "measured_summary": {
            label: {
                "target_events_per_second": measured_rates[label],
                "producer_actual_events_per_second_p50": statistics.median(
                    trial["actual_events_per_second"] for trial in trials
                ),
                "spark_input_rows_per_second_p50": statistics.median(
                    trial["spark"]["input_rows_per_second_median"] for trial in trials
                ),
                "spark_processed_rows_per_second_p50": statistics.median(
                    trial["spark"]["processed_rows_per_second_median"] for trial in trials
                ),
                "micro_batch_duration_ms_p50": statistics.median(
                    trial["spark"]["micro_batch_duration_ms_p50"] for trial in trials
                ),
                "micro_batch_duration_ms_p95": statistics.median(
                    trial["spark"]["micro_batch_duration_ms_p95"] for trial in trials
                ),
                "last_ack_to_commit_seconds_p50": statistics.median(
                    trial["last_ack_to_all_queries_committed_seconds"] for trial in trials
                ),
                "runs": len(trials),
            }
            for label, trials in measured.items()
        },
        "full_dataset_remainder": full_remainder,
        "overall_spark": summarize_spark(all_rows),
        "outcomes": {
            "producer_attempted_messages": sum(
                trial["attempted"] for trial in [*discovery, *all_measured_trials]
            )
            + full_remainder["attempted"],
            "injected_duplicates": full_remainder["injected_duplicates"],
            "reordered_events": full_remainder["reordered_events"],
            "deduplicated_valid_rows": raw_count,
            "invalid_rows": probe["table_counts"]["local.raw.invalid_rating_events"],
            "table_counts": probe["table_counts"],
            "parity": parity,
            "process_restart_recovery_seconds_from_100k_correctness_run": recovery[
                "process_restart_recovery_seconds"
            ],
        },
        "resources": {
            "host_logical_processors": psutil.cpu_count(logical=True),
            "host_physical_memory_bytes": host_memory.total,
            **docker_resources(),
        },
        "runs": {
            "discovery": len(discovery),
            "measured_per_rate": MEASURED_TRIALS,
            "full_dataset": 1,
        },
        "started_at_utc": started_at.isoformat(),
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "This is a single-machine local Docker benchmark, not a cluster capacity claim.",
            (
                "Last-ack-to-commit latency measures the last producer acknowledgement until "
                "all four Spark queries commit the corresponding high-watermarks."
            ),
            (
                "The recovery duration is reused from the separate MovieLens 100K correctness "
                "run; the benchmark does not inject another process kill."
            ),
        ],
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
