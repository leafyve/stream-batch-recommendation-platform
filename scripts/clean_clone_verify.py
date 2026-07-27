"""Run the complete verification workflow from a fresh temporary clone."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "reports" / "evidence" / "verification.json"
REQUIRED_COMMANDS = {
    "setup",
    "up",
    "download-data",
    "create-topics",
    "init-tables",
    "test",
    "integration-test",
    "run-stream",
    "produce-dev",
    "run-batch",
    "parity",
    "recovery-test",
    "benchmark",
    "report",
    "down",
    "clean",
}


def junit_counts(path: Path) -> tuple[int, int]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall(".//testsuite"))
    return (
        sum(int(suite.attrib.get("tests", 0)) for suite in suites),
        sum(
            int(suite.attrib.get("failures", 0)) + int(suite.attrib.get("errors", 0))
            for suite in suites
        ),
    )


def main() -> int:
    started_at = datetime.now(timezone.utc)
    temporary_root = Path(tempfile.mkdtemp(prefix="stream-batch-clean-clone-"))
    clone = temporary_root / "tiktok_SE_intern"
    results: list[dict[str, Any]] = []
    quality: dict[str, Any] = {}

    def run(
        command: list[str],
        *,
        cwd: Path = clone,
        capture: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        rendered = subprocess.list2cmdline(command)
        print("+", rendered, flush=True)
        before = time.monotonic()
        result = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=capture,
        )
        elapsed = time.monotonic() - before
        results.append(
            {
                "command": rendered,
                "exit_code": result.returncode,
                "elapsed_seconds": elapsed,
                "status": "pass" if result.returncode == 0 else "fail",
            }
        )
        if capture:
            print(result.stdout)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"clean-clone command failed ({result.returncode}): {rendered}")
        return result

    status = "fail"
    error: str | None = None
    try:
        run(["git", "clone", "--no-local", str(ROOT), str(clone)], cwd=temporary_root)
        python = [sys.executable, "scripts/task.py"]
        run([*python, "setup"])
        run([*python, "download-data", "--profile", "ml-100k"])
        run([*python, "download-data", "--profile", "ml-1m"])
        run(["docker", "compose", "config", "--quiet"])
        help_output = run([sys.executable, "scripts/task.py", "--help"], capture=True).stdout
        missing_commands = sorted(
            command for command in REQUIRED_COMMANDS if command not in help_output
        )
        if missing_commands:
            raise RuntimeError(f"README task interface is missing: {missing_commands}")
        run([*python, "test"])
        unit_tests, unit_failures = junit_counts(
            clone / "reports" / "runtime" / "unit-junit.xml"
        )
        coverage = json.loads(
            (clone / "reports" / "runtime" / "coverage.json").read_text(encoding="utf-8")
        )
        quality["unit_tests"] = unit_tests
        quality["unit_failures"] = unit_failures
        quality["branch_coverage_percent"] = float(
            coverage["totals"]["percent_covered"]
        )
        run([*python, "spark-test"])
        spark_tests, spark_failures = junit_counts(
            clone / "reports" / "runtime" / "spark-junit.xml"
        )
        quality["spark_tests"] = spark_tests
        quality["spark_failures"] = spark_failures

        run(
            [
                *python,
                "clean",
                "--volumes",
                "--confirm",
                "DELETE_PERSISTENT_DATA",
            ]
        )
        run([*python, "recovery-test"])

        run(
            [
                *python,
                "clean",
                "--volumes",
                "--confirm",
                "DELETE_PERSISTENT_DATA",
            ]
        )
        run([*python, "benchmark"])
        run([*python, "probe", "--output", "contract-probe-before.json"])
        run([*python, "run-stream"])
        clone_python = str(clone / ".venv" / "Scripts" / "python.exe")
        run([clone_python, "scripts/inject_contract_events.py"])
        run([clone_python, "scripts/wait_for_stream.py"])
        run(["docker", "compose", "--profile", "jobs", "stop", "-t", "60", "streaming-job"])
        run([*python, "probe", "--output", "contract-probe-after.json"])
        run([clone_python, "scripts/capture_contract_evidence.py"])
        run([clone_python, "scripts/capture_catalog_evidence.py"])
        run([clone_python, "scripts/capture_build_evidence.py"])
        run([*python, "integration-test"])
        integration_tests, integration_failures = junit_counts(
            clone / "reports" / "runtime" / "integration-junit.xml"
        )
        quality["integration_and_e2e_tests"] = integration_tests
        quality["integration_and_e2e_failures"] = integration_failures
        run([*python, "report"])
        for name in (
            "benchmark.json",
            "build.json",
            "catalog.json",
            "contracts.json",
            "recovery.json",
        ):
            shutil.copy2(clone / "reports" / "evidence" / name, OUTPUT.parent / name)
        shutil.copy2(clone / "README.md", ROOT / "README.md")
        run([str(clone / ".venv" / "Scripts" / "python.exe"), "scripts/secret_scan.py"])
        run(
            [
                str(clone / ".venv" / "Scripts" / "python.exe"),
                "scripts/check_committed_files.py",
            ]
        )
        run(["docker", "compose", "--profile", "jobs", "down", "--remove-orphans"])
        clone_status = run(["git", "status", "--short"], capture=True).stdout.splitlines()
        status = "pass"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        clone_status = []
        raise
    finally:
        evidence = {
            "status": status,
            "error": error,
            "command": "py -3.11 scripts/clean_clone_verify.py",
            "environment": "fresh local clone; Docker Desktop Linux containers",
            "source_repository": str(ROOT),
            "verification_clone": str(clone),
            "source_commit": subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip(),
            "commands": results,
            "quality_results": quality,
            "clone_git_status_after_generated_evidence": clone_status,
            "runs": 1,
            "started_at_utc": started_at.isoformat(),
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "limitations": [
                (
                    "Docker image layer cache may be reused, but containers and named "
                    "volumes are fresh."
                ),
                "The temporary verification clone is retained for inspection.",
            ],
        }
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
