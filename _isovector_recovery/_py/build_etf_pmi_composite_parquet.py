from __future__ import annotations

from pathlib import Path

import pandas as pd


ISM_PMI_FILE = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\raw\ism_pmi_transp.xlsx"
)

OUTPUT_PARQUET_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\serving\equities\etf_pmi_composite.parquet"
)

OUTPUT_JSON_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\serving\equities\etf_pmi_composite.json"
)


def find_date_col(df: pd.DataFrame) -> str:
    candidates = ["date", "Date", "dates", "Dates", "month", "Month", "as_of_date"]
    for col in candidates:
        if col in df.columns:
            return col
    raise KeyError(f"No date column found. Columns: {list(df.columns)}")


def load_signal_sheet(sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(ISM_PMI_FILE, sheet_name=sheet_name).copy()

    date_col = find_date_col(df)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    value_cols = [
        c for c in df.columns
        if c != date_col and c.endswith("_pmi")
    ]

    if not value_cols:
        raise KeyError(
            f"No *_pmi columns found in sheet '{sheet_name}'. Columns: {list(df.columns)}"
        )

    for col in value_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    out = pd.DataFrame({
        "date": df[date_col],
        "value": df[value_cols].mean(axis=1, skipna=True)
    })

    out = out.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)
    return out


def derive_bias(value: float) -> str:
    if value >= 50:
        return "Positive"
    return "Negative"


def derive_state(value: float) -> str:
    if value >= 50:
        return "Expansion"
    return "Contraction"


def main() -> None:
    if not ISM_PMI_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {ISM_PMI_FILE}")

    manu = load_signal_sheet("manu_pmi")
    serv = load_signal_sheet("serv_pmi")

    merged = manu.merge(serv, on="date", how="outer", suffixes=("_manu", "_serv"))
    merged = merged.sort_values("date").reset_index(drop=True)

    merged["composite_signal"] = merged[["value_manu", "value_serv"]].mean(axis=1, skipna=True)
    merged = merged.dropna(subset=["composite_signal"]).copy()

    merged["bias"] = merged["composite_signal"].apply(derive_bias)
    merged["state"] = merged["composite_signal"].apply(derive_state)
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")

    out = merged[["date", "composite_signal", "bias", "state"]].copy()

    OUTPUT_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PARQUET_PATH, index=False)
    out.to_json(OUTPUT_JSON_PATH, orient="records", indent=2)

    print(f"Wrote parquet -> {OUTPUT_PARQUET_PATH}")
    print(f"Wrote json    -> {OUTPUT_JSON_PATH}")
    print(out.tail(12).to_string(index=False))


if __name__ == "__main__":
    main()
