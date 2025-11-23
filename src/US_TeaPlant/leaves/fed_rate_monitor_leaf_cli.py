from pathlib import Path

from common.r2_client import write_parquet_to_r2
from US_TeaPlant.leaves.fed_rate_monitor_leaf import build_fed_rate_monitor_leaf


if __name__ == "__main__":
    # __file__ = .../the_Spine/src/US_TeaPlant/leaves/fed_rate_monitor_leaf_cli.py
    # parents[3] = .../the_Spine  (repo root)
    repo_root = Path(__file__).resolve().parents[3]

    # Local parquet path for debugging / Power BI via file if needed
    local_path = repo_root / "src" / "data" / "R2" / "us_fed_rate_monitor.parquet"
    local_path.parent.mkdir(parents=True, exist_ok=True)

    df = build_fed_rate_monitor_leaf(max_sections=3)

    df.to_parquet(local_path, index=False)
    print(f"[fed_rate_monitor_leaf_cli] Wrote local file: {local_path}")

    r2_key = "spine_us/us_fed_rate_monitor.parquet"
    write_parquet_to_r2(df, r2_key)
    print(f"[fed_rate_monitor_leaf_cli] Uploaded to R2: {r2_key}")
    
