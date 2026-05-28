from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


# =========================================================
# LOCATION
# Put this wherever you keep governed export helpers.
# Reads only the approved FX parquets.
# =========================================================

REPO_ROOT = Path.cwd()

PRICE_PARQUET = REPO_ROOT / "data" / "spine_us" / "us_fx_spot_cross_t2.parquet"
SPREAD_PARQUET = REPO_ROOT / "data" / "spine_us" / "us_fx_10y_spreads.parquet"

OUT_PRICE_JS = REPO_ROOT / "fx_price_data.js"
OUT_SPREAD_JS = REPO_ROOT / "fx_spreads_data.js"


def normalize_pair_key(value: str) -> str:
    return str(value).replace("/", "").replace(" ", "").upper().strip()


def build_price_export(df_price: pd.DataFrame) -> dict[str, list[dict]]:
    required_cols = {"symbol", "date", "close"}
    missing = required_cols - set(df_price.columns)
    if missing:
        raise KeyError(f"Price parquet missing required columns: {missing}")

    df = df_price.copy()
    df["symbol"] = df["symbol"].astype(str).map(normalize_pair_key)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()

    keep_cols = ["symbol", "date", "close"]
    optional_cols = [c for c in ["open", "high", "low"] if c in df.columns]
    df = df[keep_cols + optional_cols].copy()

    for col in ["close", "open", "high", "low"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    export_map: dict[str, list[dict]] = {}

    for symbol, g in df.groupby("symbol", sort=True):
        rows: list[dict] = []
        for _, row in g.iterrows():
            item = {
                "date": row["date"].strftime("%Y-%m-%d"),
                "close": None if pd.isna(row["close"]) else round(float(row["close"]), 6),
            }

            for opt in ["open", "high", "low"]:
                if opt in g.columns:
                    item[opt] = None if pd.isna(row[opt]) else round(float(row[opt]), 6)

            rows.append(item)

        export_map[symbol] = rows

    return export_map


def build_spread_export(df_spread: pd.DataFrame) -> dict[str, list[dict]]:
    required_cols = {"as_of_date", "pair", "base_ccy", "quote_ccy", "yld_10y_diff"}
    missing = required_cols - set(df_spread.columns)
    if missing:
        raise KeyError(f"Spread parquet missing required columns: {missing}")

    df = df_spread.copy()
    df["pair"] = df["pair"].astype(str).map(normalize_pair_key)
    df["base_ccy"] = df["base_ccy"].astype(str).str.upper().str.strip()
    df["quote_ccy"] = df["quote_ccy"].astype(str).str.upper().str.strip()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df[df["as_of_date"].notna()].copy()

    numeric_cols = ["yld_10y_diff"]
    if "yld_10y_base" in df.columns:
        numeric_cols.append("yld_10y_base")
    if "yld_10y_quote" in df.columns:
        numeric_cols.append("yld_10y_quote")

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["pair", "as_of_date"]).reset_index(drop=True)

    export_map: dict[str, list[dict]] = {}

    for pair, g in df.groupby("pair", sort=True):
        g = g.sort_values("as_of_date").copy()

        spread = g["yld_10y_diff"]
        delta_spread = spread.diff()
        z_spread = (
            (delta_spread - delta_spread.mean()) / delta_spread.std(ddof=0)
            if delta_spread.notna().sum() > 1 and float(delta_spread.std(ddof=0) or 0) != 0
            else pd.Series([None] * len(g), index=g.index)
        )

        rows: list[dict] = []
        for idx, (_, row) in enumerate(g.iterrows()):
            item = {
                "as_of_date": row["as_of_date"].strftime("%Y-%m-%d"),
                "pair": pair,
                "base_ccy": row["base_ccy"],
                "quote_ccy": row["quote_ccy"],
                "yld_10y_diff": None if pd.isna(row["yld_10y_diff"]) else round(float(row["yld_10y_diff"]), 6),
                "delta_spread": None if pd.isna(delta_spread.iloc[idx]) else round(float(delta_spread.iloc[idx]), 6),
                "z_spread": None if pd.isna(z_spread.iloc[idx]) else round(float(z_spread.iloc[idx]), 6),
            }

            if "yld_10y_base" in g.columns:
                item["yld_10y_base"] = None if pd.isna(row["yld_10y_base"]) else round(float(row["yld_10y_base"]), 6)
            if "yld_10y_quote" in g.columns:
                item["yld_10y_quote"] = None if pd.isna(row["yld_10y_quote"]) else round(float(row["yld_10y_quote"]), 6)

            rows.append(item)

        export_map[pair] = rows

    return export_map


def write_js_var(path: Path, var_name: str, payload: dict[str, list[dict]]) -> None:
    js = f"window.{var_name} = " + json.dumps(payload, indent=2) + ";\n"
    path.write_text(js, encoding="utf-8")


def main() -> None:
    if not PRICE_PARQUET.exists():
        raise FileNotFoundError(f"Missing price parquet: {PRICE_PARQUET}")
    if not SPREAD_PARQUET.exists():
        raise FileNotFoundError(f"Missing spread parquet: {SPREAD_PARQUET}")

    df_price = pd.read_parquet(PRICE_PARQUET)
    df_spread = pd.read_parquet(SPREAD_PARQUET)

    price_export = build_price_export(df_price)
    spread_export = build_spread_export(df_spread)

    write_js_var(OUT_PRICE_JS, "IV_FX_PRICE_DATA", price_export)
    write_js_var(OUT_SPREAD_JS, "IV_FX_SPREADS_DATA", spread_export)

    print(f"Wrote: {OUT_PRICE_JS}")
    print(f"Wrote: {OUT_SPREAD_JS}")
    print(f"Pairs in price export: {len(price_export)}")
    print(f"Pairs in spread export: {len(spread_export)}")


if __name__ == "__main__":
    main()

    