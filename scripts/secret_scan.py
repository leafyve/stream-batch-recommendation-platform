"""Fail if tracked files contain common credential patterns."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    "generic assigned secret": re.compile(
        r"(?i)\b(?:password|secret|token)\s*[:=]\s*['\"][^$][^'\"]{11,}['\"]"
    ),
}


def main() -> int:
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    findings: list[str] = []
    for raw_path in tracked:
        if not raw_path:
            continue
        relative = raw_path.decode("utf-8")
        path = ROOT / relative
        if not path.is_file() or path.stat().st_size > 2_000_000:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in PATTERNS.items():
            for match in pattern.finditer(content):
                line = content.count("\n", 0, match.start()) + 1
                findings.append(f"{relative}:{line}: possible {label}")
    if findings:
        print("\n".join(findings))
        return 1
    print(f"secret scan passed: {len(tracked) - 1} tracked paths inspected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
