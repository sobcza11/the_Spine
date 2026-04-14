from __future__ import annotations

import os
from pathlib import Path

import boto3
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

EXCEL_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\_Trading\_Master_review\_Important_metrics\data\tab_scrape\data_trad_econ\ism_pmi_transp.xlsx"
)

LOCAL_OUTPUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\IsoVector\_py\r2_tmp"
)
LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MANU_OUTPUT = LOCAL_OUTPUT_DIR / "us_ism_manu_pmi_by_industry_canonical.parquet"
SERV_OUTPUT = LOCAL_OUTPUT_DIR / "us_ism_nonmanu_pmi_by_industry_canonical.parquet"

R2_BUCKET = os.environ["R2_BUCKET_NAME"]
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_REGION = os.environ.get("R2_REGION", "auto")
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]

R2_KEY_MANU = "spine_us/us_ism_manu_pmi_by_industry_canonical.parquet"
R2_KEY_SERV = "spine_us/us_ism_nonmanu_pmi_by_industry_canonical.parquet"


# =============================================================================
# HELPERS
# =============================================================================

def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION,
    )


def parse_pmi_sheet(
    excel_file: Path,
    sheet_name: str,
    leaf_name: str,
    source_system: str,
) -> pd.DataFrame:
    # These sheets are already clean wide tables:
    # first column = dates
    # remaining columns = sector PMI series
    df = pd.read_excel(excel_file, sheet_name=sheet_name)

    # Normalize first column to as_of_date
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "as_of_date"})
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")

    # Keep only valid date rows
    df = df.dropna(subset=["as_of_date"]).copy()

    # Melt wide -> long
    melted = df.melt(
        id_vars=["as_of_date"],
        var_name="sector_name",
        value_name="pmi_value",
    )

    melted["pmi_value"] = pd.to_numeric(melted["pmi_value"], errors="coerce")
    melted = melted.dropna(subset=["pmi_value"]).copy()

    melted["country"] = "US"
    melted["ccy"] = "USD"
    melted["leaf_group"] = "MACRO_ISM"
    melted["leaf_name"] = leaf_name
    melted["source_system"] = source_system
    melted["updated_at"] = pd.Timestamp.utcnow()

    melted = melted[
        [
            "as_of_date",
            "country",
            "ccy",
            "sector_name",
            "pmi_value",
            "leaf_group",
            "leaf_name",
            "source_system",
            "updated_at",
        ]
    ].sort_values(["as_of_date", "sector_name"]).reset_index(drop=True)

    return melted


def existing_max_date(path: Path):
    if not path.exists():
        return None

    try:
        df = pd.read_parquet(path)
        if "as_of_date" not in df.columns or df.empty:
            return None
        return pd.to_datetime(df["as_of_date"], errors="coerce").max()
    except Exception:
        return None


def needs_refresh(new_df: pd.DataFrame, output_path: Path) -> bool:
    new_max = pd.to_datetime(new_df["as_of_date"], errors="coerce").max()
    old_max = existing_max_date(output_path)

    if old_max is None:
        return True

    if pd.isna(new_max):
        return False

    return new_max > old_max or not output_path.exists()


def upload_parquet_to_r2(s3_client, local_path: Path, r2_key: str) -> None:
    s3_client.upload_file(str(local_path), R2_BUCKET, r2_key)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"PMI source file not found: {EXCEL_PATH}")

    print(f"Source file: {EXCEL_PATH}")
    print(f"Source modified: {pd.Timestamp(EXCEL_PATH.stat().st_mtime, unit='s')}")

    manu_df = parse_pmi_sheet(
        excel_file=EXCEL_PATH,
        sheet_name="manu_pmi",
        leaf_name="us_ism_manu_pmi_by_industry_canonical",
        source_system="ISM_MANU_EXCEL",
    )

    serv_df = parse_pmi_sheet(
        excel_file=EXCEL_PATH,
        sheet_name="serv_pmi",
        leaf_name="us_ism_nonmanu_pmi_by_industry_canonical",
        source_system="ISM_NONMANU_EXCEL",
    )

    print("\nManufacturing PMI")
    print("shape:", manu_df.shape)
    print("date range:", manu_df["as_of_date"].min(), "->", manu_df["as_of_date"].max())
    print(manu_df.tail())

    print("\nNon-Manufacturing PMI")
    print("shape:", serv_df.shape)
    print("date range:", serv_df["as_of_date"].min(), "->", serv_df["as_of_date"].max())
    print(serv_df.tail())

    manu_refresh = needs_refresh(manu_df, MANU_OUTPUT)
    serv_refresh = needs_refresh(serv_df, SERV_OUTPUT)

    if manu_refresh:
        manu_df.to_parquet(MANU_OUTPUT, index=False)
        print(f"\nSaved: {MANU_OUTPUT}")
    else:
        print(f"\nNo manufacturing refresh needed: {MANU_OUTPUT}")

    if serv_refresh:
        serv_df.to_parquet(SERV_OUTPUT, index=False)
        print(f"Saved: {SERV_OUTPUT}")
    else:
        print(f"No non-manufacturing refresh needed: {SERV_OUTPUT}")

    if manu_refresh or serv_refresh:
        s3 = get_r2_client()

        if manu_refresh:
            upload_parquet_to_r2(s3, MANU_OUTPUT, R2_KEY_MANU)
            print(f"Uploaded: {R2_KEY_MANU}")

        if serv_refresh:
            upload_parquet_to_r2(s3, SERV_OUTPUT, R2_KEY_SERV)
            print(f"Uploaded: {R2_KEY_SERV}")
    else:
        print("\nNothing new to upload.")

    print("\nPMI refresh complete.")


if __name__ == "__main__":
    main()

    