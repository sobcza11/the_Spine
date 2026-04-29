import os
import pandas as pd

from .ecb_constants import *

ECB_OUTPUT_COMBINED_PATH = "data/geoscen/cb/ecb/ecb_combined_canonical_v1.parquet"


def run():
    df_policy = pd.read_parquet(ECB_OUTPUT_POLICY_PATH)
    df_accounts = pd.read_parquet(ECB_OUTPUT_ACCOUNTS_PATH)

    df = pd.concat([df_policy, df_accounts], ignore_index=True)
    df["source_layer"] = "ecb_combined_canonical_v1"
    df["version"] = "v1"

    df = df.sort_values(["date", "document_type"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(ECB_OUTPUT_COMBINED_PATH), exist_ok=True)
    df.to_parquet(ECB_OUTPUT_COMBINED_PATH, index=False)

    print("ECB combined canonical rows:", len(df))


if __name__ == "__main__":
    run()

