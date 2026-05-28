from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


def build_corporate_contagion_engine_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_recursive_propagation_steps_6_10_v1.parquet"
    )

    df = pd.read_parquet(src)

    contagion_cols = [
        c for c in df.columns
        if "contag" in c.lower()
        or "systemic" in c.lower()
        or "recursive" in c.lower()
    ]

    payload = {
        "component": "corporate_contagion_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "contagion_columns": contagion_cols,
        "contagion_column_count": len(contagion_cols),
        "status": "corporate_contagion_engine_ready"
    }

    out = (
        root
        / "_offline_site"
        / "finstate_payloads"
        / "corporate_contagion_engine_v1.json"
    )

    out.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    print("Corporate Contagion Engine complete")
    print("Contagion columns:", len(contagion_cols))
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_corporate_contagion_engine_v1()
