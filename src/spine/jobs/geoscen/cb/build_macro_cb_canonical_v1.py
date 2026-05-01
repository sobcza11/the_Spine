import os
import pandas as pd

INPUT_PATHS = [
    "data/geoscen/cb/ecb/ecb_combined_canonical_v1.parquet",
    "data/geoscen/fomc/fomc_minutes_canonical.parquet",
    "data/geoscen/fomc/fomc_historical_materials_canonical.parquet",
]

OUTPUT_PATH = "data/geoscen/cb/macro_cb_canonical_v1.parquet"

def normalize_fed(df):
    df = df.copy()

    df["bank"] = "Federal Reserve"
    df["bank_code"] = "FED"
    df["currency"] = "USD"

    if "document_type" not in df.columns:
        df["document_type"] = "fomc_minutes"

    if "title" not in df.columns:
        df["title"] = df["document_type"]

    if "url" not in df.columns:
        df["url"] = None

    if "text_chars" not in df.columns:
        df["text_chars"] = df["text"].fillna("").astype(str).str.len()

    if "ingested_at_utc" not in df.columns:
        df["ingested_at_utc"] = pd.Timestamp.utcnow()

    return df

def run():
    frames = []

    for path in INPUT_PATHS:
        if os.path.exists(path):
            df_part = pd.read_parquet(path)

            if "ecb" in path:
                frames.append(df_part)
            elif "fomc" in path:
                frames.append(normalize_fed(df_part))

    if not frames:
        raise FileNotFoundError("No central bank canonical files found.")

    df = pd.concat(frames, ignore_index=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["bank_code", "date", "document_type"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    df["ingested_at_utc"] = pd.to_datetime(df["ingested_at_utc"], errors="coerce").astype("string")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")

    df["text"] = df["text"].fillna("").astype(str)
    df["text_chars"] = df["text"].str.len()
    df = df[df["text_chars"] > 0].copy()

    df.to_parquet(OUTPUT_PATH, index=False)

    print("macro_cb canonical rows:", len(df))


if __name__ == "__main__":
    run()
