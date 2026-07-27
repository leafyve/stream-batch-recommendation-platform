"""Cross-platform task runner for the complete local workflow."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENV = ROOT / ".venv"
VENV_PYTHON = VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
COMPOSE = ["docker", "compose"]


class TaskError(RuntimeError):
    """A task failed with actionable command evidence."""


def run(command: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+", subprocess.list2cmdline(list(command)), flush=True)
    return subprocess.run(
        list(command),
        cwd=ROOT,
        check=check,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def python_command(*arguments: str) -> list[str]:
    if not VENV_PYTHON.exists():
        raise TaskError("Run `setup` first; .venv is missing")
    return [str(VENV_PYTHON), *arguments]


def setup(_: argparse.Namespace) -> None:
    if not VENV_PYTHON.exists():
        run([sys.executable, "-m", "venv", str(VENV)])
    lock = ROOT / "requirements.lock"
    if not lock.exists():
        raise TaskError("requirements.lock is missing; dependency resolution is not reproducible")
    run(python_command("-m", "pip", "install", "--require-hashes", "-r", str(lock)))
    run(python_command("-m", "pip", "install", "--no-deps", "-e", str(ROOT)))


def up(_: argparse.Namespace) -> None:
    run([*COMPOSE, "up", "-d", "--build", "--wait"])


def down(_: argparse.Namespace) -> None:
    run([*COMPOSE, "--profile", "jobs", "down", "--remove-orphans"])


def create_topics(_: argparse.Namespace) -> None:
    topics = (
        ("rating-events", "3"),
        ("rating-events-dlq", "3"),
    )
    listed = subprocess.run(
        [*COMPOSE, "exec", "-T", "redpanda", "rpk", "topic", "list"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    for topic, partitions in topics:
        if topic not in listed.split():
            run(
                [
                    *COMPOSE,
                    "exec",
                    "-T",
                    "redpanda",
                    "rpk",
                    "topic",
                    "create",
                    topic,
                    f"--partitions={partitions}",
                    "--replicas=1",
                    "--topic-config=retention.ms=604800000",
                    "--topic-config=cleanup.policy=delete",
                ]
            )
        run(
            [
                *COMPOSE,
                "exec",
                "-T",
                "redpanda",
                "rpk",
                "topic",
                "describe",
                topic,
            ]
        )


def spark_job(module: str, *arguments: str) -> None:
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    run(
        [
            *COMPOSE,
            "--profile",
            "jobs",
            "run",
            "--rm",
            "--use-aliases",
            "-e",
            f"GIT_REVISION={revision}",
            "streaming-job",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--conf",
            "spark.driver.host=streaming-job",
            "--conf",
            "spark.driver.bindAddress=0.0.0.0",
            "--conf",
            "spark.cores.max=2",
            "--conf",
            "spark.executor.cores=2",
            f"/opt/app/jobs/{module}.py",
            *arguments,
        ]
    )


def init_tables(args: argparse.Namespace) -> None:
    arguments = ["--probe-value", args.probe_value] if args.probe_value else []
    spark_job("iceberg_tables", *arguments)


def download_data(args: argparse.Namespace) -> None:
    run(python_command(str(ROOT / "scripts" / "download_movielens.py"), "--profile", args.profile))


def test(_: argparse.Namespace) -> None:
    run(python_command("-m", "ruff", "check", "."))
    run(python_command("-m", "mypy", "src"))
    run(
        python_command(
            "-m",
            "pytest",
            "-m",
            "not spark and not integration and not e2e and not recovery and not benchmark",
            "--cov",
            "recommendation_features",
            "--cov-branch",
            "--cov-report=term-missing",
        )
    )


def spark_test(_: argparse.Namespace) -> None:
    run([*COMPOSE, "build", "spark-master"])
    run(
        [
            *COMPOSE,
            "--profile",
            "jobs",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "python3",
            "streaming-job",
            "-m",
            "pytest",
            "/opt/app/tests/spark",
            "-v",
            "-o",
            "cache_dir=/tmp/pytest-cache",
        ]
    )


def integration_test(_: argparse.Namespace) -> None:
    run(python_command("-m", "pytest", "-m", "integration or e2e", "-v"))


def run_stream(_: argparse.Namespace) -> None:
    run([*COMPOSE, "--profile", "jobs", "up", "-d", "streaming-job"])


def produce_dev(args: argparse.Namespace) -> None:
    filename = "u.data" if args.profile == "ml-100k" else "ratings.dat"
    source = "movielens-100k" if args.profile == "ml-100k" else "movielens-1m"
    run(
        [
            *COMPOSE,
            "--profile",
            "jobs",
            "run",
            "--rm",
            "producer",
            "--dataset-path",
            f"/data/raw/{args.profile}/{filename}",
            "--source",
            source,
            "--replay-run-id",
            args.replay_run_id,
            "--start-record",
            str(args.start_record),
            "--max-records",
            str(args.max_records),
            "--events-per-second",
            str(args.events_per_second),
            "--duplicate-percentage",
            str(args.duplicate_percentage),
            "--out-of-order-percentage",
            str(args.out_of_order_percentage),
            "--out-of-order-delay-seconds",
            str(args.out_of_order_delay_seconds),
            "--statistics-path",
            f"/reports/runtime/{args.statistics_name}",
        ]
    )


def run_batch(args: argparse.Namespace) -> None:
    checksum = (
        "0e33842e24a9c977be4e0107933c0723"
        if args.profile == "ml-100k"
        else "c4d9eecfca2ab87c1945afe126590906"
    )
    spark_job(
        "batch_job",
        "--dataset-path",
        f"/data/raw/{args.profile}/{'u.data' if args.profile == 'ml-100k' else 'ratings.dat'}",
        "--profile",
        args.profile,
        "--checksum",
        checksum,
    )


def parity(_: argparse.Namespace) -> None:
    spark_job("parity", "--output", "/reports/runtime/parity.json")


def probe(args: argparse.Namespace) -> None:
    arguments = ["--output", f"/reports/runtime/{args.output}"] if args.output else []
    spark_job("platform_probe", *arguments)


def recovery_test(_: argparse.Namespace) -> None:
    run(python_command(str(ROOT / "scripts" / "recovery_demo.py")))


def benchmark(_: argparse.Namespace) -> None:
    run(python_command(str(ROOT / "scripts" / "benchmark.py")))


def report(_: argparse.Namespace) -> None:
    run(python_command(str(ROOT / "scripts" / "generate_report.py")))


def clean(args: argparse.Namespace) -> None:
    down(args)
    for path in (ROOT / "reports" / "runtime", ROOT / "logs"):
        if path.exists():
            shutil.rmtree(path)
            print(f"removed transient path: {path}")
    if args.volumes:
        if args.confirm != "DELETE_PERSISTENT_DATA":
            raise TaskError(
                "--volumes requires --confirm DELETE_PERSISTENT_DATA; persistent data was preserved"
            )
        run([*COMPOSE, "--profile", "jobs", "down", "--volumes", "--remove-orphans"])


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    commands = {
        "setup": setup,
        "up": up,
        "download-data": download_data,
        "create-topics": create_topics,
        "init-tables": init_tables,
        "test": test,
        "spark-test": spark_test,
        "integration-test": integration_test,
        "run-stream": run_stream,
        "produce-dev": produce_dev,
        "run-batch": run_batch,
        "parity": parity,
        "probe": probe,
        "recovery-test": recovery_test,
        "benchmark": benchmark,
        "report": report,
        "down": down,
        "clean": clean,
    }
    for name, function in commands.items():
        command = sub.add_parser(name)
        command.set_defaults(function=function)
        if name == "download-data":
            command.add_argument("--profile", choices=["ml-100k", "ml-1m"], default="ml-100k")
        elif name == "produce-dev":
            command.add_argument("--profile", choices=["ml-100k", "ml-1m"], default="ml-100k")
            command.add_argument("--start-record", type=int, default=0)
            command.add_argument("--max-records", type=int, default=1000)
            command.add_argument("--events-per-second", type=float, default=100.0)
            command.add_argument("--duplicate-percentage", type=float, default=0.0)
            command.add_argument("--out-of-order-percentage", type=float, default=0.0)
            command.add_argument("--out-of-order-delay-seconds", type=int, default=7200)
            command.add_argument("--replay-run-id", default="development")
            command.add_argument("--statistics-name", default="producer-development.json")
        elif name == "run-batch":
            command.add_argument("--profile", choices=["ml-100k", "ml-1m"], default="ml-100k")
        elif name == "probe":
            command.add_argument("--output", default="platform-probe.json")
        elif name == "init-tables":
            command.add_argument("--probe-value")
        elif name == "clean":
            command.add_argument("--volumes", action="store_true")
            command.add_argument("--confirm")
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        args.function(args)
    except (subprocess.CalledProcessError, TaskError) as exc:
        print(f"task failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
