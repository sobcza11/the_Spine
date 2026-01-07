from __future__ import annotations

import importlib
import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    src_path = repo_root / "src"
    if src_path.exists():
        p = str(src_path)
        if p not in sys.path:
            sys.path.insert(0, p)


def test_pillar1_entrypoints_resolve() -> None:
    """
    Non-behavioral smoke: validates callable wiring exists.
    Does NOT execute data pulls.
    """
    _ensure_src_on_path()

    score_mod = importlib.import_module("ft_gmi.pillars.pillar1.score_pillar1")
    assert hasattr(score_mod, "score_pillar1"), "Missing score_pillar1() entrypoint"

    reg_mod = importlib.import_module("ft_gmi.pillars.pillar1.metric_registry")
    assert reg_mod is not None