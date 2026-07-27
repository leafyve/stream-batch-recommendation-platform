"""Download official MovieLens data with checksum and provenance verification."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOWNLOADS = ROOT / "data" / "downloads"
RAW = ROOT / "data" / "raw"
PROVENANCE = ROOT / "data" / "provenance"
SCRIPT_VERSION = "1"

PROFILES = {
    "ml-100k": (
        "https://files.grouplens.org/datasets/movielens/ml-100k.zip",
        "0e33842e24a9c977be4e0107933c0723",
    ),
    "ml-1m": (
        "https://files.grouplens.org/datasets/movielens/ml-1m.zip",
        "c4d9eecfca2ab87c1945afe126590906",
    ),
}


def git_revision() -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def safe_extract(archive: Path, destination: Path) -> None:
    destination_root = destination.resolve()
    with zipfile.ZipFile(archive) as source:
        for member in source.infolist():
            target = (destination / member.filename).resolve()
            if destination_root not in target.parents and target != destination_root:
                raise RuntimeError(f"unsafe archive member: {member.filename}")
        source.extractall(destination)


def download(profile: str) -> dict[str, object]:
    url, expected = PROFILES[profile]
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)
    PROVENANCE.mkdir(parents=True, exist_ok=True)
    archive = DOWNLOADS / f"{profile}.zip"
    with urllib.request.urlopen(url, timeout=120) as response:
        archive.write_bytes(response.read())
    actual = md5(archive)
    if actual != expected:
        archive.unlink(missing_ok=True)
        raise RuntimeError(f"{profile} checksum mismatch: expected {expected}, received {actual}")
    safe_extract(archive, RAW)
    extracted = RAW / profile
    readmes = sorted(path.name for path in extracted.glob("README*"))
    if not readmes:
        raise RuntimeError(f"{profile} did not contain a README or license notice")
    evidence: dict[str, object] = {
        "source_url": url,
        "dataset_name": profile,
        "checksum_algorithm": "md5",
        "expected_checksum": expected,
        "actual_checksum": actual,
        "checksum_result": "pass",
        "retrieval_utc": datetime.now(UTC).isoformat(),
        "script_version": SCRIPT_VERSION,
        "git_revision": git_revision(),
        "license_files": readmes,
        "raw_records_committed": False,
    }
    (PROVENANCE / f"{profile}.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=sorted(PROFILES), required=True)
    args = parser.parse_args()
    print(json.dumps(download(args.profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
