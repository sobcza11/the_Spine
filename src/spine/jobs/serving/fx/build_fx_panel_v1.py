from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path.cwd()

FX_ZT_PATH = REPO_ROOT / "data" / "serving" / "fx" / "fx_zt_v1.parquet"
OUTPUT_PATH = REPO_ROOT / "data" / "serving" / "fx" / "fx_panel_v1.json"


def safe_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def main() -> None:
    df = pd.read_parquet(FX_ZT_PATH).copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    latest = df.tail(1).iloc[0]

    series = [
        {
            "date": row["date"].strftime("%Y-%m-%d"),
            "value": safe_float(row["fx_zt"]),
        }
        for _, row in df.tail(50).iterrows()
    ]

    panel = {
        "panel_version": "v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "PASS",
        "layout": {
            "title": "FX | OC",
            "top_left": {
                "name": "FX Zₜ",
                "value": safe_float(latest["fx_zt"]),
                "date": latest["date"].strftime("%Y-%m-%d"),
                "components": {
                    "cb_divergence": safe_float(latest["cb_norm"]),
                    "rates_alignment": safe_float(latest["rates_norm"]),
                    "fx_pressure": safe_float(latest["fx_norm"]),
                    "commodity_linkage": safe_float(latest["wti_norm"]),
                },
            },
            "top_right": {
                "name": "RESERVED — FX Graph Area",
                "series": series,
            },
            "bottom_left": {
                "name": "RBL (OC) | FX Reading Between the Lines",
                "text": (
                    "FX Zₜ is built from approved local inputs: CB divergence, "
                    "rates alignment, FX pressure, and commodity linkage. "
                    "This is a UI composite only and is not an IsoVector signal."
                ),
            },
            "bottom_right": {
                "name": "n/a",
                "status": "reserved",
            },
        },
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(panel, f, indent=2)

    print(f"FX panel written: {OUTPUT_PATH}")
    print(f"Panel status: {panel['status']}")
    print(f"FX Z_t latest: {panel['layout']['top_left']['value']}")


if __name__ == "__main__":
    main()
    