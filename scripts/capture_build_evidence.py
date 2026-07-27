"""Record registry pins and locally built image identities."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "reports" / "evidence" / "build.json"


def inspect(image: str) -> dict[str, Any]:
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return cast(list[dict[str, Any]], json.loads(result.stdout))[0]


def service_image_id(service: str) -> str | None:
    container_id = subprocess.run(
        ["docker", "compose", "ps", "--all", "-q", service],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not container_id:
        return None
    return subprocess.run(
        ["docker", "inspect", "--format", "{{.Image}}", container_id],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def main() -> int:
    repository_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    local_specs: dict[str, dict[str, Any]] = {
        "minio": {
            "image": "tiktok-se-intern-minio:RELEASE.2025-10-15T17-29-55Z",
            "source_release": "RELEASE.2025-10-15T17-29-55Z",
            "source_commit": "9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a",
            "base_image_digest": (
                "golang:1.24.9-bookworm@sha256:"
                "737b40b61ce956d738bed59f18ba854d8d67e7a4c4fa63f64437f4c70247ac5b"
            ),
            "runtime_base_digest": (
                "python:3.11.9-slim-bookworm@sha256:"
                "8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317"
            ),
            "build_command": "docker compose build minio",
            "build_arguments": {
                "MINIO_RELEASE": "RELEASE.2025-10-15T17-29-55Z",
                "MINIO_COMMIT": "9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a",
            },
        },
        "spark": {
            "image": "tiktok-se-intern-spark:local",
            "source_release": "Spark image 3.5.8-scala2.12-java17-python3-ubuntu",
            "source_commit": None,
            "base_image_digest": (
                "apache/spark:3.5.8-scala2.12-java17-python3-ubuntu@sha256:"
                "dbe5f3b3926b01fa0b818ae8463c2994bfd2945d8db96d2337e23dc62d6fa73d"
            ),
            "build_command": "docker compose build spark-master",
            "build_arguments": {},
        },
        "producer": {
            "image": "tiktok-se-intern-producer:local",
            "source_release": "repository source",
            "source_commit": None,
            "base_image_digest": (
                "python:3.11.9-slim-bookworm@sha256:"
                "8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317"
            ),
            "build_command": "docker compose --profile jobs build producer",
            "build_arguments": {},
        },
    }
    local_images: dict[str, Any] = {}
    for name, specification in local_specs.items():
        metadata = inspect(str(specification["image"]))
        local_images[name] = {
            **specification,
            "local_image_id": metadata["Id"],
            "repo_digests": metadata.get("RepoDigests", []),
        }
    evidence = {
        "status": "pass",
        "command": "py -3.11 scripts/capture_build_evidence.py",
        "environment": "local Docker Desktop Linux containers",
        "git_revision": repository_revision,
        "registry_images": {
            "redpanda": (
                "redpandadata/redpanda:v26.1.13@sha256:"
                "ae0a858eddd0538dacbba5696f9be1f590de1fe51283964afc85f5510fa8f32e"
            ),
            "console": (
                "redpandadata/console:v3.8.0@sha256:"
                "4aa45603b7b9c57b7b8073289be4f880edb929599a8d3e17260e98d7ee7bcdd7"
            ),
            "iceberg_rest_fixture": (
                "apache/iceberg-rest-fixture:1.10.1@sha256:"
                "f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f"
            ),
            "minio_mc": (
                "minio/mc:RELEASE.2025-08-13T08-35-41Z@sha256:"
                "a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727"
            ),
        },
        "local_images": local_images,
        "compose_service_image_ids": {
            service: service_image_id(service)
            for service in (
                "minio",
                "spark-master",
                "spark-worker",
                "streaming-job",
            )
        },
        "runs": 1,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "Local image IDs are host-local content identities, not invented registry digests.",
            "A rebuild can change metadata and therefore the local image ID.",
        ],
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
