from pathlib import Path
import pandas as pd

from fed_speak.fusion_fed_speak import compute_fed_composites


INPUT = Path("data/fed_block_for_spine.csv")
OUTPUT = Path("data/fed_drift_metrics.csv")


def compute_drift(series: pd.Series) -> pd.Series:
    """Simple first-difference drift metric."""
    return series.diff().fillna(0)


def main() -> None:
    if not INPUT.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT}")

    # Base Fed block (minutes + statements + Beige features, etc.)
    df = pd.read_csv(INPUT, parse_dates=["date"]).sort_values("date")

    # Compute composites on the fly for each row
    comps = df.apply(
        lambda r: pd.Series(compute_fed_composites(r)),
        axis=1,
    )

    # Combine date + composites into one frame
    out = pd.concat([df[["date"]], comps], axis=1)

    # Sanity check
    for col in ["policy_uncertainty", "policy_coherence"]:
        if col not in out.columns:
            raise ValueError(
                f"Composite column '{col}' missing after compute_fed_composites; "
                "check fusion_fed_speak.compute_fed_composites."
            )

    # Drift metrics
    out["uncertainty_drift"] = compute_drift(out["policy_uncertainty"])
    out["coherence_drift"] = compute_drift(out["policy_coherence"])

    # 3-meeting rolling averages for smoother trend
    out["uncertainty_rolling"] = (
        out["policy_uncertainty"].rolling(3, min_periods=1).mean()
    )
    out["coherence_rolling"] = (
        out["policy_coherence"].rolling(3, min_periods=1).mean()
    )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT, index=False)
    print(f"Saved drift metrics to: {OUTPUT}")


if __name__ == "__main__":
    main()


