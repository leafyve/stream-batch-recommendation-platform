from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from scripts.download_movielens import md5, safe_extract


def test_md5_and_safe_extract(tmp_path: Path) -> None:
    archive = tmp_path / "safe.zip"
    with zipfile.ZipFile(archive, "w") as target:
        target.writestr("ml/README", "license")
    assert md5(archive)
    output = tmp_path / "output"
    safe_extract(archive, output)
    assert (output / "ml" / "README").read_text(encoding="utf-8") == "license"


def test_safe_extract_rejects_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive, "w") as target:
        target.writestr("../escape", "bad")
    with pytest.raises(RuntimeError, match="unsafe"):
        safe_extract(archive, tmp_path / "output")
