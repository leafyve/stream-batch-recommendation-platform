"""Render verified README metrics from committed evidence JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
EVIDENCE = ROOT / "reports" / "evidence"
START = "<!-- VERIFIED_RESULTS_START -->"
END = "<!-- VERIFIED_RESULTS_END -->"


def load(name: str) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads((EVIDENCE / name).read_text(encoding="utf-8")),
    )


def main() -> int:
    recovery = load("recovery.json")
    contracts = load("contracts.json")
    benchmark = load("benchmark.json")
    summary = benchmark["measured_summary"]
    high = summary["high"]
    parity = benchmark["outcomes"]["parity"]
    block = "\n".join(
        [
            START,
            "### Verified local results",
            "",
            "| Evidence | Measured result |",
            "|---|---:|",
            (
                f"| MovieLens 1M source rows | "
                f"{benchmark['dataset']['source_records_used']:,} |"
            ),
            (
                f"| Highest sustainable producer target | "
                f"{benchmark['configuration']['highest_sustainable_rate']:,.0f} events/s |"
            ),
            (
                f"| High-rate producer actual p50, 3 trials | "
                f"{high['producer_actual_events_per_second_p50']:,.1f} events/s |"
            ),
            (
                f"| High-rate Spark processed-rate p50 | "
                f"{high['spark_processed_rows_per_second_p50']:,.1f} rows/s |"
            ),
            (
                f"| High-rate micro-batch duration p50 / p95 | "
                f"{high['micro_batch_duration_ms_p50']:,.0f} / "
                f"{high['micro_batch_duration_ms_p95']:,.0f} ms |"
            ),
            (
                f"| Injected duplicate messages / deduplicated valid rows | "
                f"{benchmark['outcomes']['injected_duplicates']:,} / "
                f"{benchmark['outcomes']['deduplicated_valid_rows']:,} |"
            ),
            (
                f"| Reordered events | "
                f"{benchmark['outcomes']['reordered_events']:,} |"
            ),
            (
                f"| User / movie finalised-window parity | "
                f"{parity[0]['parity_percentage']:.1f}% / "
                f"{parity[1]['parity_percentage']:.1f}% |"
            ),
            (
                f"| Streaming-process restart recovery | "
                f"{recovery['process_restart_recovery_seconds']:.3f} s |"
            ),
            (
                f"| Invalid records / DLQ messages in contract test | "
                f"{contracts['observed']['new_invalid_rows']} / "
                f"{contracts['observed']['dlq_messages']} |"
            ),
            "",
            (
                "All values above are rendered from `reports/evidence/*.json`. "
                "They are local Docker measurements, not production capacity claims."
            ),
            END,
        ]
    )
    source = README.read_text(encoding="utf-8")
    if START not in source or END not in source:
        raise RuntimeError("README verified-results markers are missing")
    prefix, remainder = source.split(START, 1)
    _old, suffix = remainder.split(END, 1)
    README.write_text(prefix + block + suffix, encoding="utf-8")
    print("README verified results rendered from reports/evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
