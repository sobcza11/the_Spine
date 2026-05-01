import pandas as pd

OUTPUT_PATH = "data/rates/eu/de_it_10y_spread_v1.parquet"

REQUIRED_COLS = [
    "date",
    "de_10y",
    "it_10y",
    "it_de_10y_spread",
    "de_it_10y_spread",
    "source",
    "de_series_id",
    "it_series_id",
    "source_layer",
    "version",
]


def run():
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) < 300:
        raise ValueError(f"Row count too low: {len(df)}")

    if df["date"].isna().any():
        raise ValueError("Missing dates detected")

    if df[["de_10y", "it_10y", "it_de_10y_spread"]].isna().any().any():
        raise ValueError("Missing yield/spread values detected")

    print("DE-IT 10Y spread validation passed:", len(df))


if __name__ == "__main__":
    run()

