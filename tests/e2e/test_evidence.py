from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "reports" / "evidence"


def evidence(name: str) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads((EVIDENCE / name).read_text(encoding="utf-8")),
    )


@pytest.mark.e2e
def test_contract_evidence_proves_invalid_dlq_and_late_event_behavior() -> None:
    payload = evidence("contracts.json")
    observed = payload["observed"]
    assert payload["status"] == "pass"
    assert observed["new_valid_rows"] == 1
    assert observed["new_invalid_rows"] == 2
    assert observed["dlq_messages"] == 2
    assert observed["duplicate_invalid_record_ids"] == 0
    assert observed["user_feature_row_change"] == 0
    assert observed["movie_feature_row_change"] == 0
    for query in ("user_feature_aggregation", "movie_feature_aggregation"):
        assert (
            observed["late_event_query_progress"][query][
                "num_rows_dropped_by_watermark"
            ]
            == 1
        )


@pytest.mark.e2e
def test_catalog_evidence_proves_sqlite_driver_restart_and_repeated_commits() -> None:
    payload = evidence("catalog.json")
    assert payload["status"] == "pass"
    assert "org/sqlite/JDBC.class" in payload["sqlite_driver_classpath_evidence"]
    assert payload["probe_read_after_restart"] == payload["first_probe"]
    assert payload["repeated_commit_probe"] == "catalog-repeated"
    assert payload["tables_readable"] == 8


@pytest.mark.e2e
@pytest.mark.recovery
def test_recovery_evidence_proves_checkpoint_resume_and_storage_persistence() -> None:
    payload = evidence("recovery.json")
    assert payload["status"] == "pass"
    assert payload["checkpoint_file_count_before_kill"] > 0
    assert payload["query_run_ids_before_kill"] != payload["query_run_ids_after_restart"]
    assert payload["storage_restart_counts_preserved"] is True
    assert (
        payload["probe_after_storage_restart"]["valid_event_uniqueness"][
            "duplicate_event_ids"
        ]
        == 0
    )
    assert all(row["status"] == "pass" for row in payload["parity_results"])


@pytest.mark.e2e
@pytest.mark.benchmark
def test_benchmark_evidence_uses_full_movielens_1m_and_three_trials_per_rate() -> None:
    payload = evidence("benchmark.json")
    assert payload["status"] == "pass"
    assert payload["dataset"]["source_records"] == 1_000_209
    assert payload["dataset"]["source_records_used"] == 1_000_209
    assert payload["runs"]["measured_per_rate"] >= 3
    assert payload["outcomes"]["deduplicated_valid_rows"] == 1_000_209
    assert payload["outcomes"]["injected_duplicates"] > 0
    assert all(row["status"] == "pass" for row in payload["outcomes"]["parity"])
