from pathlib import Path
import json
import pandas as pd


def main():
    repo_root = Path(__file__).resolve().parents[1]

    leaf_path = repo_root / "data" / "processed" / "FedSpeak" / "combined_policy_leaf.parquet"
    if not leaf_path.exists():
        raise FileNotFoundError(f"combined_policy_leaf not found at {leaf_path}")

    df = pd.read_parquet(leaf_path)

    required_cols = ["inflation_risk", "growth_risk", "policy_bias"]
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"Required column '{col}' not found in combined_policy_leaf")

    baseline = {}
    for col in required_cols:
        series = df[col].dropna()
        baseline[col] = {
            "mean": float(series.mean()),
            "std": float(series.std(ddof=0)),  # population std
        }

    out_dir = repo_root / "config"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "fedspeak_feature_baseline.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(baseline, f, indent=2)

    print(f"Wrote FedSpeak feature baseline → {out_path}")
    print("Baseline values:", baseline)


if __name__ == "__main__":
    main()

