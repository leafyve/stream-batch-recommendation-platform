"""Report unexpectedly large committed files."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIMIT_BYTES = 2_000_000


def main() -> int:
    paths = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    large: list[str] = []
    for raw_path in paths:
        if not raw_path:
            continue
        relative = raw_path.decode("utf-8")
        path = ROOT / relative
        if path.is_file() and path.stat().st_size > LIMIT_BYTES:
            large.append(f"{relative}: {path.stat().st_size} bytes")
    if large:
        print("\n".join(large))
        return 1
    print(f"large-file scan passed: no tracked file exceeds {LIMIT_BYTES} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
