import pandas as pd
from .ecb_constants import *

REQUIRED_COLS = [
    "bank","bank_code","currency","document_type",
    "title","date","url","text","text_chars","ingested_at_utc"
]

def run():
    df = pd.read_parquet(ECB_OUTPUT_POLICY_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if df["text_chars"].min() <= 0:
        raise ValueError("Empty text detected")

    print("ECB Policy Decisions validation passed:", len(df))


if __name__ == "__main__":
    run()

    