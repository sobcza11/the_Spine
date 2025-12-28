import pandas as pd
from pathlib import Path

INPUT = Path("data/fed_block_for_spine.csv")
OUTPUT = Path("data/fed_drift_metrics.csv")

def compute_drift(series):
    return series.diff().fillna(0)

def main():
    df = pd.read_csv(INPUT, parse_dates=["date"]).sort_values("date")

    df["uncertainty_drift"] = compute_drift(df["policy_uncertainty"])
    df["coherence_drift"] = compute_drift(df["policy_coherence"])

    df["uncertainty_rolling"] = df["policy_uncertainty"].rolling(3).mean()
    df["coherence_rolling"] = df["policy_coherence"].rolling(3).mean()

    df.to_csv(OUTPUT, index=False)
    print("Saved:", OUTPUT)

if __name__ == "__main__":
    main()

