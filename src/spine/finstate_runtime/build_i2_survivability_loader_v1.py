from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


def build_i2_survivability_loader_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_recursive_propagation_steps_6_10_v1.parquet"
    )

    if not src.exists():
        raise FileNotFoundError(src)

    df = pd.read_parquet(src)

    payload = {
        "component": "i2_survivability_loader_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "dates": int(df["statement_date"].nunique()),
        "columns": sorted(df.columns.tolist()),
        "status": "i2_survivability_loader_ready"
    }

    out = (
        root
        / "_offline_site"
        / "finstate_payloads"
        / "i2_survivability_loader_v1.json"
    )

    out.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    print("I2 Survivability Loader complete")
    print("Rows:", payload["rows"])
    print("Symbols:", payload["symbols"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_i2_survivability_loader_v1()
