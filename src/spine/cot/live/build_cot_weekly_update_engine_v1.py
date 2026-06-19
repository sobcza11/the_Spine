from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import requests
import zipfile
import io
import json


CURRENT_YEAR = datetime.now(UTC).year

LIVE_SOURCES = {
    "legacy_futures_live": f"https://www.cftc.gov/files/dea/history/deacot{CURRENT_YEAR}.zip",
    "financial_futures_live": f"https://www.cftc.gov/files/dea/history/com_fin_txt_{CURRENT_YEAR}.zip",
    "disaggregated_commodities_live": f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{CURRENT_YEAR}.zip",
}


def download_cftc_zip(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        data_files = [
            x for x in zf.namelist()
            if x.lower().endswith((".csv", ".txt"))
        ]

        if not data_files:
            raise ValueError(
                f"No CSV/TXT found in ZIP payload. Found: {zf.namelist()}"
            )

        with zf.open(data_files[0]) as f:
            df = pd.read_csv(
                f,
                low_memory=False,
                dtype=str,
            )

    return df.copy()


def build_cot_weekly_update_engine_v1():
    repo_root = Path.cwd()

    raw_dir = repo_root / "data" / "cot" / "raw_cftc"
    raw_dir.mkdir(parents=True, exist_ok=True)

    historical_path = raw_dir / "cftc_raw_combined_v1.parquet"

    if historical_path.exists():
        historical_df = pd.read_parquet(historical_path).copy()
        historical_df = historical_df.astype("string")
    else:
        historical_df = pd.DataFrame()

    frames = []

    for source_name, url in LIVE_SOURCES.items():
        print(f"Downloading live source: {source_name}")

        try:
            df = download_cftc_zip(url)
            df["source_name"] = source_name
            df["live_update_timestamp"] = datetime.now(UTC).isoformat()
            frames.append(df.astype("string"))
            print(f"Rows: {len(df)}")

        except Exception as e:
            print(f"FAILED: {source_name}")
            print(str(e))

    if not frames:
        raise ValueError("No live CFTC datasets downloaded")

    live_df = pd.concat(frames, ignore_index=True, sort=False).astype("string")

    combined_df = pd.concat(
        [historical_df, live_df],
        ignore_index=True,
        sort=False,
    ).astype("string")

    combined_df = combined_df.drop_duplicates().copy()

    parquet_path = raw_dir / "cftc_raw_combined_live_v1.parquet"
    json_path = raw_dir / "cftc_raw_combined_live_v1.json"

    combined_df.to_parquet(parquet_path, index=False)

    combined_df.head(1000).to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    metadata = {
        "component": "cot_weekly_update_engine_v1",
        "update_timestamp_utc": datetime.now(UTC).isoformat(),
        "historical_rows": int(len(historical_df)),
        "live_rows": int(len(live_df)),
        "combined_rows": int(len(combined_df)),
        "live_sources": list(LIVE_SOURCES.keys()),
        "status": "live_cot_update_complete",
    }

    metadata_path = raw_dir / "cot_live_update_metadata_v1.json"

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print("COT live weekly update complete")
    print("Historical Rows:", len(historical_df))
    print("Live Rows:", len(live_df))
    print("Combined Rows:", len(combined_df))
    print("PARQUET:", parquet_path)
    print("JSON SAMPLE:", json_path)
    print("METADATA:", metadata_path)
    print("Metadata:", metadata)

    return combined_df


if __name__ == "__main__":
    build_cot_weekly_update_engine_v1()

    