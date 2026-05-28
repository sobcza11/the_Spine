from __future__ import annotations

from pathlib import Path

import pandas as pd


REPO_ROOT = Path.cwd()

CB_PATH = REPO_ROOT / "data" / "llm" / "approved_inputs" / "isovector_macro_cb_view.parquet"
PMI_PATH = REPO_ROOT / "data" / "llm" / "approved_inputs" / "pmi_geoscen_zt_input.parquet"
RATES_PATH = REPO_ROOT / "data" / "llm" / "approved_inputs" / "isovector_macro_cb_rates_join.parquet"

OUTPUT_PATH = REPO_ROOT / "data" / "serving" / "geoscen" / "zt_v1.parquet"


def minmax_to_unit(series: pd.Series) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce")

    min_v = series.min()
    max_v = series.max()

    if pd.isna(min_v) or pd.isna(max_v) or min_v == max_v:
        return pd.Series(0.0, index=series.index)

    return 2 * ((series - min_v) / (max_v - min_v)) - 1


def load_cb() -> pd.DataFrame:
    df = pd.read_parquet(CB_PATH).copy()
    df["date"] = pd.to_datetime(df["date"])
    df["cb_raw"] = pd.to_numeric(df["policy_tone"], errors="coerce")
    return df[["date", "cb_raw"]]


def load_pmi() -> pd.DataFrame:
    df = pd.read_parquet(PMI_PATH).copy()
    df["date"] = pd.to_datetime(df["date"])
    df["pmi_raw"] = pd.to_numeric(df["pmi_geoscen_zt_input"], errors="coerce")
    return df.groupby("date", as_index=False)["pmi_raw"].mean()


def load_rates() -> pd.DataFrame:
    df = pd.read_parquet(RATES_PATH).copy()
    df["date"] = pd.to_datetime(df["date"])

    if "it_de_10y_spread_z" in df.columns:
        df["rates_raw"] = pd.to_numeric(df["it_de_10y_spread_z"], errors="coerce")
    else:
        df["rates_raw"] = pd.NA

    return df[["date", "rates_raw"]]


def main() -> None:
    cb = load_cb()
    pmi = load_pmi()
    rates = load_rates()

    df = cb.merge(pmi, on="date", how="outer")
    df = df.merge(rates, on="date", how="outer")
    df = df.sort_values("date")
    df = df.groupby("date", as_index=False).mean()
    df = df.sort_values("date").reset_index(drop=True)

    df["cb_norm"] = minmax_to_unit(df["cb_raw"])
    df["pmi_norm"] = minmax_to_unit(df["pmi_raw"])
    df["rates_norm"] = minmax_to_unit(df["rates_raw"])

    weights = {
        "cb_norm": 0.4,
        "pmi_norm": 0.3,
        "rates_norm": 0.3
    }

    def weighted_row(row):
        vals = []
        wts = []

        for k, w in weights.items():
            if pd.notna(row[k]):
                vals.append(row[k] * w)
                wts.append(w)

        if not wts:
            return 0.0

        return sum(vals) / sum(wts)

    df["zt_v1"] = df.apply(weighted_row, axis=1)

    out = df[["date", "cb_norm", "pmi_norm", "rates_norm", "zt_v1"]].copy()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print(f"Z_t v1 written: {OUTPUT_PATH}")
    print(f"Rows: {len(out)}")
    print(out.tail())


if __name__ == "__main__":
    main()
