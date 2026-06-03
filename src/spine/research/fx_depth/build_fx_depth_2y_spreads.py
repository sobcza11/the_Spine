from pathlib import Path
import json
import xml.etree.ElementTree as ET
import pandas as pd
import requests
from fredapi import Fred

FRED_API_KEY = "f41c1162e50de7362129a5c052dc1327"

ROOT = Path.cwd()

RAW_DIR = ROOT / "data" / "fx" / "fx_depth" / "raw" / "yields"
PROCESSED_DIR = ROOT / "data" / "fx" / "fx_depth" / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
(PROCESSED_DIR / "eurusd").mkdir(parents=True, exist_ok=True)
(PROCESSED_DIR / "usdcad").mkdir(parents=True, exist_ok=True)

fred = Fred(api_key=FRED_API_KEY)

def pull_us_2y():
    s = fred.get_series("DGS2")
    df = (
        s.rename("us_2y")
        .reset_index()
        .rename(columns={"index": "date"})
    )
    df["date"] = pd.to_datetime(df["date"])
    df["us_2y"] = pd.to_numeric(df["us_2y"], errors="coerce")
    df = df.dropna(subset=["date", "us_2y"]).sort_values("date")
    df.to_parquet(RAW_DIR / "us_2y.parquet", index=False)
    return df

def pull_ca_2y():
    url = "https://www.bankofcanada.ca/valet/observations/V122538/json?start_date=2000-01-01"
    payload = requests.get(url, timeout=30).json()

    rows = []
    for obs in payload.get("observations", []):
        date = obs.get("d")
        value = obs.get("V122538", {}).get("v")
        rows.append({"date": date, "ca_2y": value})

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["ca_2y"] = pd.to_numeric(df["ca_2y"], errors="coerce")
    df = df.dropna(subset=["date", "ca_2y"]).sort_values("date")
    df.to_parquet(RAW_DIR / "ca_2y.parquet", index=False)
    return df

def pull_de_2y():
    url = (
        "https://api.statistiken.bundesbank.de/rest/data/"
        "BBSSY/D.REN.EUR.A610.000000WT0202.A?startPeriod=2000-01-01"
    )

    xml_text = requests.get(url, timeout=30).text

    ns = {
        "generic": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
    }

    root = ET.fromstring(xml_text)

    rows = []
    for obs in root.findall(".//generic:Obs", ns):
        date_node = obs.find("generic:ObsDimension", ns)
        value_node = obs.find("generic:ObsValue", ns)

        if date_node is None or value_node is None:
            continue

        rows.append({
            "date": date_node.attrib.get("value"),
            "de_2y": value_node.attrib.get("value")
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["de_2y"] = pd.to_numeric(df["de_2y"], errors="coerce")
    df = df.dropna(subset=["date", "de_2y"]).sort_values("date")
    df.to_parquet(RAW_DIR / "de_2y.parquet", index=False)
    return df

def build_spread(left_df, right_df, left_col, right_col, spread_col):
    df = (
        left_df
        .merge(right_df, on="date", how="inner")
        .sort_values("date")
        .reset_index(drop=True)
    )

    df[spread_col] = df[left_col] - df[right_col]
    df[f"{spread_col}_dod"] = df[spread_col].diff()

    return df

print("Pulling US 2Y...")
us = pull_us_2y()

print("Pulling Germany 2Y...")
de = pull_de_2y()

print("Pulling Canada 2Y...")
ca = pull_ca_2y()

print("Building DE-US 2Y spread...")
de_us = build_spread(
    left_df=de,
    right_df=us,
    left_col="de_2y",
    right_col="us_2y",
    spread_col="de_us_2y_spread"
)

de_us_path = PROCESSED_DIR / "eurusd" / "de_us_2y_spread.parquet"
de_us.to_parquet(de_us_path, index=False)

print("Building US-CA 2Y spread...")
us_ca = build_spread(
    left_df=us,
    right_df=ca,
    left_col="us_2y",
    right_col="ca_2y",
    spread_col="us_ca_2y_spread"
)

us_ca_path = PROCESSED_DIR / "usdcad" / "us_ca_2y_spread.parquet"
us_ca.to_parquet(us_ca_path, index=False)

print("")
print("=" * 80)
print("DE-US 2Y SPREAD .tail(3)")
print("=" * 80)
print(de_us.tail(3))

print("")
print("=" * 80)
print("US-CA 2Y SPREAD .tail(3)")
print("=" * 80)
print(us_ca.tail(3))

print("")
print("Saved:")
print(de_us_path)
print(us_ca_path)
