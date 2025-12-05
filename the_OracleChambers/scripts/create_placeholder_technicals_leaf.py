from pathlib import Path
from datetime import date

import duckdb
import pandas as pd

ROOT_TECH = Path("data/processed/p_Tech_US")
ROOT_TECH.mkdir(parents=True, exist_ok=True)

out_path = ROOT_TECH / "technical_leaf.parquet"

# Simple one-row placeholder: neutral technical regime on a reference date
df = pd.DataFrame(
    [
        {
            "date": date(2025, 1, 1),
            "liquidity_score": 0.0,
            "credit_stress_score": 0.0,
            "vol_regime_score": 0.0,
            "breadth_score": 0.0,
            "fx_risk_score": 0.0,
            "metals_signal_score": 0.0,
            "overall_technical_regime": 0.0,
        }
    ]
)

con = duckdb.connect()
con.execute(f"COPY df TO '{out_path.as_posix()}' (FORMAT 'parquet');")
con.close()

print("Placeholder technical_leaf written to:", out_path)
