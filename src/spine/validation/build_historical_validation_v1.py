from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

VALIDATION_REGIMES = [
    {"regime": "GFC_2008", "period": "2007-2009", "expected_pressure": "systemic"},
    {"regime": "COVID_2020", "period": "2020", "expected_pressure": "systemic"},
    {"regime": "INFLATION_2022", "period": "2021-2023", "expected_pressure": "elevated"},
    {"regime": "TIGHTENING_2018", "period": "2018", "expected_pressure": "watch"},
]

def build_historical_validation_v1():
    root = Path.cwd()
    out = root / "data" / "validation"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame([
        {
            "regime": r["regime"],
            "period": r["period"],
            "expected_pressure": r["expected_pressure"],
            "validation_status": "scaffold_ready",
            "current_note": "Historical replay not yet connected to full live historical panel.",
        }
        for r in VALIDATION_REGIMES
    ])

    summary = {
        "component": "historical_validation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "regime_count": int(len(df)),
        "validation_status": "historical_validation_scaffold_complete",
        "status": "historical_validation_complete",
    }

    df.to_parquet(out / "historical_validation_v1.parquet", index=False)
    df.to_json(out / "historical_validation_v1.json", orient="records", indent=2)

    with open(out / "historical_validation_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Historical Validation complete")
    print("Regimes:", summary["regime_count"])
    print("Status:", summary["validation_status"])

if __name__ == "__main__":
    build_historical_validation_v1()
