import os
import pandas as pd

INPUT_PATHS = [
    "data/geoscen/cb/ecb/ecb_combined_canonical_v1.parquet",
]

OUTPUT_PATH = "data/geoscen/cb/macro_cb_canonical_v1.parquet"


def run():
    frames = []

    for path in INPUT_PATHS:
        if os.path.exists(path):
            frames.append(pd.read_parquet(path))

    if not frames:
        raise FileNotFoundError("No central bank canonical files found.")

    df = pd.concat(frames, ignore_index=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["bank_code", "date", "document_type"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print("macro_cb canonical rows:", len(df))


if __name__ == "__main__":
    run()
