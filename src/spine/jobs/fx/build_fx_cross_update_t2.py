import os
from io import BytesIO
from datetime import timedelta

import boto3
import pandas as pd


# ---------------------------------------------------------------------
# ENV (fail fast — governance requirement)
# ---------------------------------------------------------------------

R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]

SOURCE_KEY = "spine_us/us_fx_spot_canonical_t2.parquet"
TARGET_KEY = "spine_us/us_fx_spot_cross_t2.parquet"

OVERLAP_DAYS = 5
CROSS_SYMBOL = "NZDAUD"
REQ_SYMBOLS = ["AUDUSD", "NZDUSD"]


# ---------------------------------------------------------------------
# R2 Client
# ---------------------------------------------------------------------

def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


# ---------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------

def read_parquet(client, key):
    obj = client.get_object(Bucket=R2_BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def write_parquet(client, df, key):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.put_object(Bucket=R2_BUCKET, Key=key, Body=buffer.getvalue())


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def normalize_price_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Canonical spot sometimes uses `close`, sometimes `px`.
    Normalize to `close`.
    """
    if "close" in df.columns:
        return df

    if "px" in df.columns:
        df = df.rename(columns={"px": "close"})
        return df

    raise KeyError(f"No price column found. Columns: {list(df.columns)}")


def compute_cross(df_spot: pd.DataFrame) -> pd.DataFrame:
    df = df_spot.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    df = normalize_price_column(df)

    df = df[df["symbol"].isin(REQ_SYMBOLS)]

    if df.empty:
        raise RuntimeError("Required spot symbols not found in source leaf.")

    aud = (
        df[df["symbol"] == "AUDUSD"][["date", "close"]]
        .rename(columns={"close": "aud"})
    )

    nzd = (
        df[df["symbol"] == "NZDUSD"][["date", "close"]]
        .rename(columns={"close": "nzd"})
    )

    merged = pd.merge(nzd, aud, on="date", how="inner")

    if merged.empty:
        raise RuntimeError("AUDUSD & NZDUSD have no overlapping dates.")

    merged["close"] = merged["nzd"] / merged["aud"]
    merged["symbol"] = CROSS_SYMBOL

    out = merged[["symbol", "date", "close"]].copy()
    out = out.dropna()

    return out


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    r2 = get_r2()

    print("=== FX CROSS UPDATE (NZDAUD) ===")
    print("SOURCE_KEY:", SOURCE_KEY)
    print("TARGET_KEY:", TARGET_KEY)

    # -----------------------------------------------------------------
    # Load source canonical spot
    # -----------------------------------------------------------------
    source = read_parquet(r2, SOURCE_KEY)
    source["date"] = pd.to_datetime(source["date"]).dt.tz_localize(None)

    # -----------------------------------------------------------------
    # Load existing cross leaf (if exists)
    # -----------------------------------------------------------------
    try:
        existing = read_parquet(r2, TARGET_KEY)
        existing["date"] = pd.to_datetime(existing["date"]).dt.tz_localize(None)

        max_date = existing["date"].max()
        cutoff = max_date - timedelta(days=OVERLAP_DAYS)

        base = existing[existing["date"] < cutoff].copy()
        source = source[source["date"] >= cutoff]

        print("Existing leaf found.")
        print("Max date:", max_date)
        print("Overlap cutoff:", cutoff)

    except Exception:
        print("No existing cross leaf found. Full rebuild.")
        base = pd.DataFrame(columns=["symbol", "date", "close"])

    # -----------------------------------------------------------------
    # Compute cross
    # -----------------------------------------------------------------
    new = compute_cross(source)

    # -----------------------------------------------------------------
    # Merge
    # -----------------------------------------------------------------
    combined = pd.concat([base, new], ignore_index=True)

    combined = (
        combined
        .drop_duplicates(subset=["symbol", "date"], keep="last")
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )

    # -----------------------------------------------------------------
    # Write canonical leaf
    # -----------------------------------------------------------------
    write_parquet(r2, combined, TARGET_KEY)

    print("NZDAUD UPDATE written.")
    print("Rows:", len(combined))
    print("Max date:", combined["date"].max())


if __name__ == "__main__":
    main()
