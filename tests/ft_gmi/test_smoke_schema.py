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


def test_pillar1_output_schema_present() -> None:
    """
    Contract smoke:
    - schema module imports, and
    - exposes some schema surface (constant OR callable OR any ALL_CAPS container).
    """
    _ensure_src_on_path()
    mod = importlib.import_module("ft_gmi.pillars.pillar1.pillar1_output_schema")

    # 1) common constant names
    candidates = ["OUTPUT_SCHEMA", "PILLAR1_OUTPUT_SCHEMA", "SCHEMA", "OUTPUT_COLUMNS", "OUTPUT_COLS"]
    for name in candidates:
        if hasattr(mod, name):
            v = getattr(mod, name)
            assert isinstance(v, (dict, list, tuple)), f"{name} must be dict/list/tuple; got {type(v)}"
            return

    # 2) common function names
    fn_candidates = ["get_schema", "get_output_schema", "output_schema", "schema"]
    for name in fn_candidates:
        if hasattr(mod, name) and callable(getattr(mod, name)):
            out = getattr(mod, name)()
            assert isinstance(out, (dict, list, tuple)), f"{name}() must return dict/list/tuple; got {type(out)}"
            return

    # 3) fallback: any ALL_CAPS container in module
    for name in dir(mod):
        if name.isupper():
            v = getattr(mod, name)
            if isinstance(v, (dict, list, tuple)):
                return

    raise AssertionError(
        "No schema surface found. Add a schema constant or schema getter in pillar1_output_schema."
    )


