import os
from io import StringIO

import pandas as pd
import requests

OUTPUT_PATH = "data/rates/eu/de_it_10y_spread_v1.parquet"

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

SERIES = {
    "de_10y": "IRLTLT01DEM156N",
    "it_10y": "IRLTLT01ITM156N",
}


def fetch_fred_series(series_id: str, value_name: str) -> pd.DataFrame:
    url = FRED_CSV.format(series_id=series_id)
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text))
    df = df.rename(columns={"observation_date": "date", series_id: value_name})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df[value_name] = pd.to_numeric(df[value_name], errors="coerce")

    return df[["date", value_name]].dropna()


def run():
    de = fetch_fred_series(SERIES["de_10y"], "de_10y")
    it = fetch_fred_series(SERIES["it_10y"], "it_10y")

    df = de.merge(it, on="date", how="inner")

    df["it_de_10y_spread"] = df["it_10y"] - df["de_10y"]
    df["de_it_10y_spread"] = df["de_10y"] - df["it_10y"]

    df["source"] = "FRED"
    df["de_series_id"] = SERIES["de_10y"]
    df["it_series_id"] = SERIES["it_10y"]
    df["source_layer"] = "de_it_10y_spread_v1"
    df["version"] = "v1"

    df = df.sort_values("date").reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print("DE-IT 10Y spread rows:", len(df))
    print("Latest:")
    print(df.tail(5).to_string(index=False))


if __name__ == "__main__":
    run()
