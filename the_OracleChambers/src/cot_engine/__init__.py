from pathlib import Path

# Resolve package root (…/the_OracleChambers)
PACKAGE_ROOT = Path(__file__).resolve().parents[1]

# Canonical data locations
DATA_ROOT = PACKAGE_ROOT / "data"
COT_ROOT = DATA_ROOT / "cot"
COT_XLS_DIR = COT_ROOT / "xls"
LOG_DIR = PACKAGE_ROOT / "log"

# Ensure required directories exist
COT_XLS_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

__all__ = [
    "PACKAGE_ROOT",
    "DATA_ROOT",
    "COT_ROOT",
    "COT_XLS_DIR",
    "LOG_DIR",
]
