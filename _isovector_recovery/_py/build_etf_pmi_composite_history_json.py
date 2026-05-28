from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


OUTPUT_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\serving\equities\etf_pmi_composite.json"
)

DATE_CANDIDATES = ["date", "as_of_date", "month"]
COMPOSITE_CANDIDATES = [
    "composite_signal",
    "pmi_composite_signal",
    "signal",
    "value",
    "composite",
]
BIAS_CANDIDATES = ["bias", "direction", "composite_bias"]
STATE_CANDIDATES = ["state", "regime", "composite_state"]


def first_existing(columns: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in columns:
            return c
    return None


def derive_bias(value: float) -> str:
    if value > 0:
        return "Positive"
    if value < 0:
        return "Negative"
    return "Neutral"


def derive_state(value: float) -> str:
    if value > 0:
        return "Expansion"
    if value < 0:
        return "Contraction"
    return "Neutral"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Full path to etf_pmi_composite.parquet")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_path}")

    df = pd.read_parquet(input_path).copy()
    cols = list(df.columns)

    date_col = first_existing(cols, DATE_CANDIDATES)
    composite_col = first_existing(cols, COMPOSITE_CANDIDATES)
    bias_col = first_existing(cols, BIAS_CANDIDATES)
    state_col = first_existing(cols, STATE_CANDIDATES)

    if date_col is None or composite_col is None:
        raise KeyError(
            f"Could not find required columns. Found columns: {cols}"
        )

    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
    df[composite_col] = pd.to_numeric(df[composite_col], errors="coerce")
    df = df.dropna(subset=[date_col, composite_col]).sort_values(date_col)

    records: list[dict] = []
    for _, row in df.iterrows():
        value = float(row[composite_col])
        records.append(
            {
                "date": row[date_col],
                "composite_signal": round(value, 6),
                "bias": str(row[bias_col]) if bias_col and pd.notna(row[bias_col]) else derive_bias(value),
                "state": str(row[state_col]) if state_col and pd.notna(row[state_col]) else derive_state(value),
            }
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print(f"Wrote {len(records)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

    