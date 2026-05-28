from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


def build_quarterly_deterioration_propagation_v1():

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

    pressure_cols = [
        c for c in df.columns
        if "pressure" in c.lower()
        or "fragility" in c.lower()
        or "survivability" in c.lower()
    ]

    payload = {
        "component": "quarterly_deterioration_propagation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "pressure_columns": pressure_cols,
        "pressure_column_count": len(pressure_cols),
        "status": "quarterly_deterioration_propagation_ready"
    }

    out = (
        root
        / "_offline_site"
        / "finstate_payloads"
        / "quarterly_deterioration_propagation_v1.json"
    )

    out.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    print("Quarterly Deterioration Propagation complete")
    print("Pressure columns:", len(pressure_cols))
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_quarterly_deterioration_propagation_v1()
