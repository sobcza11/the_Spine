from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


def check_file_health(path: str | Path, max_age_hours: int = 48) -> dict:
    p = Path(path)

    result = {
        "path": str(p),
        "exists": p.exists(),
        "size_bytes": None,
        "modified_utc": None,
        "age_hours": None,
        "status": "missing",
    }

    if not p.exists():
        return result

    stat = p.stat()
    modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - modified).total_seconds() / 3600

    result.update({
        "size_bytes": stat.st_size,
        "modified_utc": modified.isoformat(),
        "age_hours": round(age_hours, 2),
        "status": "ok" if age_hours <= max_age_hours and stat.st_size > 0 else "stale_or_empty",
    })

    return result


def validate_required_columns(df: pd.DataFrame, required_cols: list[str]) -> dict:
    missing = sorted(set(required_cols) - set(df.columns))

    return {
        "required_cols": required_cols,
        "missing_cols": missing,
        "status": "ok" if not missing else "schema_error",
    }


def write_runtime_health_report(checks: list[dict], out_path: str | Path) -> None:
    out = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "overall_status": "ok" if all(c.get("status") == "ok" for c in checks) else "attention_required",
    }

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
        