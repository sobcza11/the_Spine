import os
import pandas as pd

CB_PATH = "data/geoscen/signals/isovector_macro_cb_view_v1.parquet"
RATES_PATH = "data/rates/eu/de_it_10y_spread_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/isovector_macro_cb_rates_join_v1.parquet"


def run():
    cb = pd.read_parquet(CB_PATH).copy()
    rates = pd.read_parquet(RATES_PATH).copy()

    cb["date"] = pd.to_datetime(cb["date"], errors="coerce")
    rates["date"] = pd.to_datetime(rates["date"], errors="coerce")

    cb["month"] = cb["date"].dt.to_period("M").astype(str)
    rates["month"] = rates["date"].dt.to_period("M").astype(str)

    rates_m = (
        rates.groupby("month", as_index=False)
        .agg(
            it_de_10y_spread=("it_de_10y_spread", "mean"),
            de_10y=("de_10y", "mean"),
            it_10y=("it_10y", "mean"),
        )
    )

    out = cb.merge(rates_m, on="month", how="left")

    out["it_de_10y_spread_z"] = (
        (out["it_de_10y_spread"] - out["it_de_10y_spread"].mean()) /
        out["it_de_10y_spread"].std()
    )

    out["source_layer"] = "isovector_macro_cb_rates_join_v1"
    out["version"] = "v1"

    out = out.sort_values(["bank_code", "date"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("IsoVector macro_cb + rates rows:", len(out))


if __name__ == "__main__":
    run()

