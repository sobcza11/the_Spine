from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


def build_survivability_memory_system_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_survivability_intelligence_steps_6_10_v1.parquet"
    )

    df = pd.read_parquet(src)

    memory_cols = [
        c for c in df.columns
        if "memory" in c.lower()
        or "persistence" in c.lower()
        or "drift" in c.lower()
    ]

    payload = {
        "component": "survivability_memory_system_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "memory_columns": memory_cols,
        "memory_column_count": len(memory_cols),
        "status": "survivability_memory_system_ready"
    }

    out = (
        root
        / "_offline_site"
        / "finstate_payloads"
        / "survivability_memory_system_v1.json"
    )

    out.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    print("Survivability Memory System complete")
    print("Memory columns:", len(memory_cols))
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_survivability_memory_system_v1()
