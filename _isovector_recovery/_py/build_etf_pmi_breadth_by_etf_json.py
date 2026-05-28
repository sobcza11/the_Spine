from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ISM_PMI_FILE = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\raw\ism_pmi_transp.xlsx"
)

OUTPUT_JSON_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\serving\equities\etf_pmi_breadth_by_etf.json"
)

ETF_INDUSTRY_MAP = {
    "SPY": [
        "Chemical Products_pmi",
        "Computer & Electronic Products_pmi",
        "Electrical Equipment, Appliances & Components_pmi",
        "Fabricated Metal Products_pmi",
        "Food, Beverage & Tobacco Products_pmi",
        "Machinery_pmi",
        "Transportation Equipment_pmi",
        "Primary Metals_pmi",
        "Paper Products_pmi",
        "Plastics & Rubber Products_pmi"
    ],
    "QQQ": [
        "Computer & Electronic Products_pmi",
        "Electrical Equipment, Appliances & Components_pmi",
        "Machinery_pmi"
    ],
    "DIA": [
        "Chemical Products_pmi",
        "Fabricated Metal Products_pmi",
        "Machinery_pmi",
        "Transportation Equipment_pmi",
        "Primary Metals_pmi",
        "Paper Products_pmi"
    ],
    "ITOT": [
        "Chemical Products_pmi",
        "Computer & Electronic Products_pmi",
        "Electrical Equipment, Appliances & Components_pmi",
        "Fabricated Metal Products_pmi",
        "Food, Beverage & Tobacco Products_pmi",
        "Machinery_pmi",
        "Transportation Equipment_pmi",
        "Primary Metals_pmi",
        "Paper Products_pmi",
        "Plastics & Rubber Products_pmi",
        "Furniture & Related Products_pmi",
        "Wood Products_pmi"
    ],
    "MDY": [
        "Fabricated Metal Products_pmi",
        "Machinery_pmi",
        "Transportation Equipment_pmi",
        "Furniture & Related Products_pmi",
        "Wood Products_pmi",
        "Plastics & Rubber Products_pmi",
        "Electrical Equipment, Appliances & Components_pmi"
    ],
    "IWM": [
        "Fabricated Metal Products_pmi",
        "Machinery_pmi",
        "Furniture & Related Products_pmi",
        "Wood Products_pmi",
        "Plastics & Rubber Products_pmi",
        "Printing & Related Support Activities_pmi",
        "Textile Mills_pmi",
        "Apparel, Leather & Allied Products_pmi"
    ],
}


def find_date_col(df: pd.DataFrame) -> str:
    for col in ["date", "Date", "dates", "Dates", "month", "Month", "as_of_date"]:
        if col in df.columns:
            return col
    raise KeyError(f"No date column found. Columns: {list(df.columns)}")


def load_sheet(sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(ISM_PMI_FILE, sheet_name=sheet_name).copy()
    date_col = find_date_col(df)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={date_col: "date"})
    return df


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
    manu = load_sheet("manu_pmi")
    serv = load_sheet("serv_pmi")

    all_records: list[dict] = []

    for etf, industry_cols in ETF_INDUSTRY_MAP.items():
        manu_cols = [c for c in industry_cols if c in manu.columns]
        serv_cols = [c for c in industry_cols if c in serv.columns]

        manu_part = manu[["date"] + manu_cols].copy()
        serv_part = serv[["date"] + serv_cols].copy()

        for col in manu_cols:
            manu_part[col] = pd.to_numeric(manu_part[col], errors="coerce")
        for col in serv_cols:
            serv_part[col] = pd.to_numeric(serv_part[col], errors="coerce")

        manu_signal = pd.DataFrame({
            "date": manu_part["date"],
            "manu_signal": manu_part[manu_cols].mean(axis=1, skipna=True) if manu_cols else None
        })

        serv_signal = pd.DataFrame({
            "date": serv_part["date"],
            "serv_signal": serv_part[serv_cols].mean(axis=1, skipna=True) if serv_cols else None
        })

        merged = manu_signal.merge(serv_signal, on="date", how="outer")
        merged["composite_signal"] = merged[["manu_signal", "serv_signal"]].mean(axis=1, skipna=True)
        merged = merged.dropna(subset=["date", "composite_signal"]).sort_values("date")

        for _, row in merged.iterrows():
            value = float(row["composite_signal"])
            all_records.append({
                "date": pd.to_datetime(row["date"]).strftime("%Y-%m-%d"),
                "etf": etf,
                "composite_signal": round(value, 6),
                "bias": derive_bias(value),
                "state": derive_state(value),
            })

    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    print(f"Wrote json -> {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()

    