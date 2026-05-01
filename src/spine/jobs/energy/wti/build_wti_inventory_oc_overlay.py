import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import json
import boto3
import os

# =========================
# LOAD SOURCE (EXISTING PANEL INPUT)
# =========================
REPO_ROOT = Path.cwd()

WTI_CANONICAL_FILE = "us_wti_inventory_canonical.parquet"

panel_path = REPO_ROOT / "data" / "wti" / WTI_CANONICAL_FILE

df = pd.read_parquet(panel_path).copy()
df["date"] = pd.to_datetime(df["date"])

# =========================
# STEP 1 — WEEK OF YEAR
# =========================
df["week"] = df["date"].dt.isocalendar().week

# =========================
# STEP 2 — SPLIT HISTORY VS CURRENT YEAR
# =========================
current_year = df["date"].dt.year.max()

df_hist = df[df["date"].dt.year < current_year].copy()
df_curr = df[df["date"].dt.year == current_year].copy()

# =========================
# STEP 3 — NOTEBOOK-MATCHED INDEX UNIVERSE
# =========================
EXCLUDED_YEARS = {2008, 2003, 2014, 1997}

df["year"] = df["date"].dt.year
df["week"] = df["date"].dt.isocalendar().week.astype(int)

df = df[~df["year"].isin(EXCLUDED_YEARS)].copy()

current_year = int(df["year"].max())

year_start = (
    df.sort_values(["year", "week"])
    .groupby("year")["inventory_mmbbl"]
    .first()
    .to_dict()
)

df["year_start_inventory"] = df["year"].map(year_start)

df["index"] = np.where(
    df["week"] == 1,
    100.0,
    ((df["inventory_mmbbl"] / df["year_start_inventory"]) - 1) * 100 + 100
)

df["index"] = df["index"].round(2)

df_hist = df[df["year"] < current_year].copy()
df_curr = df[df["year"] == current_year].copy()

# =========================
# STEP 4 — SEASONAL BANDS FROM INDEX UNIVERSE
# =========================
seasonal = (
    df_hist.groupby("week")["index"]
    .agg(min="min", avg="mean", max="max")
    .reset_index()
)

current_map = (
    df_curr.sort_values(["week", "date"])
    .groupby("week")["index"]
    .last()
    .to_dict()
)

seasonal["current"] = seasonal["week"].map(current_map)

# =========================
# STEP 5 — Z SCORE FROM LATEST CURRENT VS SAME WEEK BAND
# =========================
latest_row = df_curr.sort_values("date").iloc[-1]
latest_week = int(latest_row["week"])
latest_val = float(latest_row["index"])

if latest_week > 52:
    latest_week = 52

row = seasonal[seasonal["week"] == latest_week]

if not row.empty:
    mu = float(row["avg"].iloc[0])
    sigma = float((row["max"].iloc[0] - row["min"].iloc[0]) / 4)
    z = (latest_val - mu) / sigma if sigma > 0 else 0.0
else:
    z = 0.0

state = (
    "tight" if z > 1
    else "loose" if z < -1
    else "neutral"
)

# =========================
# STEP 6 — FINAL STRUCTURE
# =========================

seasonal = seasonal[seasonal["week"].between(1, 52)].copy()

week_zero = {
    "week": 0,
    "min": 100.0,
    "avg": 100.0,
    "max": 100.0,
    "current": 100.0
}

rows = [week_zero] + (
    seasonal
    .sort_values("week")
    .astype(object)
    .where(pd.notnull(seasonal), None)
    .to_dict(orient="records")
)

payload = {
    "meta": {
        "source": "EIA",
        "as_of_date": df["date"].max().strftime("%Y-%m-%d")
    },
    "overlay": {
        "z": None if pd.isna(z) else float(z),
        "state": state
    },
    "rows": rows
}

# =========================
# SAVE LOCAL
# =========================
output_path = Path("wti_inventory_oc_overlay.json")
with open(output_path, "w") as f:
    json.dump(payload, f, allow_nan=False)

# =========================
# UPLOAD TO CLOUDFLARE R2
# =========================

if all(k in os.environ for k in [
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
        "R2_REGION"
    ]):
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["R2_ENDPOINT"],
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            region_name=os.environ["R2_REGION"]
        )

        s3.upload_file(
            str(output_path),
            os.environ["R2_BUCKET_NAME"],
            "spine_us/serving/wti/wti_inventory_oc_overlay.json"
        )

        print("OC OVERLAY BUILT & UPLOADED TO R2")
else:
    print("OC OVERLAY BUILT (LOCAL ONLY — R2 ENV NOT SET)")
print("WTI OC OVERLAY JOB COMPLETE")
