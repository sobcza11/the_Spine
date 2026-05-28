from pathlib import Path
import pandas as pd
import requests
import zipfile
import io


def download_cftc_zip(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        data_files = [
            x for x in zf.namelist()
            if x.lower().endswith((".csv", ".txt"))
        ]

        if not data_files:
            raise ValueError(f"No CSV/TXT found in ZIP payload. Found: {zf.namelist()}")

        with zf.open(data_files[0]) as f:
            df = pd.read_csv(f)

    return df.copy()


def build_cot_ingestion_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "cot" / "raw_cftc"
    out_dir.mkdir(parents=True, exist_ok=True)

    years = range(2020, 2025)

    sources = {}

    for year in years:
        sources[f"legacy_futures_{year}"] = (
            f"https://www.cftc.gov/files/dea/history/deacot{year}.zip"
        )

        sources[f"financial_futures_{year}"] = (
            f"https://www.cftc.gov/files/dea/history/com_fin_txt_{year}.zip"
        )

    frames = []

    for source_name, url in sources.items():
        print(f"Downloading: {source_name}")

        try:
            df = download_cftc_zip(url)
            df = df.copy()
            df["source_name"] = source_name
            frames.append(df)
            print(f"Rows: {len(df)}")

        except Exception as e:
            print(f"FAILED: {source_name}")
            print(str(e))

    if not frames:
        raise ValueError("No CFTC datasets downloaded")

    df_all = pd.concat(frames, ignore_index=True, sort=False).copy()

    parquet_path = out_dir / "cftc_raw_combined_v1.parquet"
    json_path = out_dir / "cftc_raw_combined_v1.json"

    df_all.to_parquet(parquet_path, index=False)

    df_all.head(1000).to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    print("CFTC multi-year ingestion complete")
    print("Total Rows:", len(df_all))
    print("PARQUET:", parquet_path)
    print("JSON SAMPLE:", json_path)

    return df_all


if __name__ == "__main__":
    build_cot_ingestion_v1()
