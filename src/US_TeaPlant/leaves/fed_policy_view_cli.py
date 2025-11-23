from pathlib import Path
import pandas as pd

from US_TeaPlant.leaves.fed_rate_monitor_leaf import (
    update_r2_parquet,
    build_fed_policy_view,
)
from common.r2_client import write_parquet_to_r2


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[2]

    # 1) Ensure raw Fed monitor history is up to date (local parquet)
    raw_path = base_dir / "data" / "R2" / "us_fed_rate_monitor.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    update_r2_parquet(str(raw_path))

    # 2) Build policy view from raw history
    df_raw = pd.read_parquet(raw_path)
    df_policy = build_fed_policy_view(df_raw)

    # 3) Write local policy view parquet
    policy_path = base_dir / "data" / "R2" / "us_fed_policy_view.parquet"
    df_policy.to_parquet(policy_path, index=False)
    print(f"[fed_policy_view_cli] Wrote local file: {policy_path}")

    # 4) Mirror to R2 for Spine consumption
    write_parquet_to_r2(df_policy, "spine_us/us_fed_policy_view.parquet")
    print("[fed_policy_view_cli] Uploaded to R2: spine_us/us_fed_policy_view.parquet")
