from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


def build_recursive_pressure_conditioning_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_survivability_intelligence_steps_6_10_v1.parquet"
    )

    if not src.exists():
        raise FileNotFoundError(src)

    df = pd.read_parquet(src)

    conditioning_cols = [
        c for c in df.columns
        if "conditioning" in c.lower()
        or "coupling" in c.lower()
        or "pressure" in c.lower()
        or "systemicity" in c.lower()
    ]

    payload = {
        "component": "recursive_pressure_conditioning_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "conditioning_columns": conditioning_cols,
        "conditioning_column_count": len(conditioning_cols),
        "status": "recursive_pressure_conditioning_ready"
    }

    out = (
        root
        / "_offline_site"
        / "finstate_payloads"
        / "recursive_pressure_conditioning_v1.json"
    )

    out.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    print("Recursive Pressure Conditioning complete")
    print("Conditioning columns:", len(conditioning_cols))
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_recursive_pressure_conditioning_v1()
