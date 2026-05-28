from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


def build_i2_quarterly_registry_v1():

    root = Path.cwd()

    panel_path = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_statement_panel_v1.parquet"
    )

    out = root / "data" / "i2_quarterly"

    panel = pd.read_parquet(panel_path)

    registry = (
        panel
        .groupby("symbol")
        .agg(
            min_date=("statement_date", "min"),
            max_date=("statement_date", "max"),
            quarter_count=("statement_date", "count"),
            reporting_confidence=("reporting_confidence", "mean")
        )
        .reset_index()
    )

    registry["annual_only_reporter"] = (
        registry["quarter_count"] <= 4
    )

    registry.to_parquet(
        out / "i2_quarterly_registry_v1.parquet",
        index=False
    )

    registry.head(500).to_json(
        out / "i2_quarterly_registry_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    summary = {
        "component": "i2_quarterly_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "symbols": int(registry["symbol"].nunique()),
        "annual_only_reporters": int(
            registry["annual_only_reporter"].sum()
        ),
        "avg_quarters": round(
            float(registry["quarter_count"].mean()),
            2
        ),
        "status": "quarterly_registry_complete"
    }

    with open(
        out / "i2_quarterly_registry_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    print("I2 Quarterly Registry complete")
    print("Symbols:", summary["symbols"])
    print("Annual-only:", summary["annual_only_reporters"])
    print("Avg quarters:", summary["avg_quarters"])


if __name__ == "__main__":
    build_i2_quarterly_registry_v1()
