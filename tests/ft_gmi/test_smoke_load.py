from __future__ import annotations

import importlib
import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    """
    Ensures repo-local src/ is importable when running tests from repo root.
    Safe for CI & local runs.
    """
    repo_root = Path(__file__).resolve().parents[2]
    src_path = repo_root / "src"
    if src_path.exists():
        p = str(src_path)
        if p not in sys.path:
            sys.path.insert(0, p)


def test_ft_gmi_imports() -> None:
    _ensure_src_on_path()
    importlib.import_module("ft_gmi")