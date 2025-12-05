from pathlib import Path

import pandas as pd


def classify_econ_state(score: float) -> str:
    """
    Rule-based econ_state classification from econ_score ∈ [-1, 1].

    - score <= -0.3      → 'contraction'
    - -0.3 < score < 0.3 → 'soft'
    - score >= 0.3       → 'expansion'
    """
    if pd.isna(score):
        return "unknown"

    if score <= -0.3:
        return "contraction"
    if score >= 0.3:
        return "expansion"
    return "soft"


def classify_inflation_regime(score: float) -> str:
    """
    Rule-based inflation_regime classification from inflation_score ∈ [-1, 1].

    - score <= -0.3      → 'disinflation'
    - -0.3 < score < 0.3 → 'anchored'
    - score >= 0.3       → 'overheat'
    """
    if pd.isna(score):
        return "unknown"

    if score <= -0.3:
        return "disinflation"
    if score >= 0.3:
        return "overheat"
    return "anchored"


def main():
    repo_root = Path(__file__).resolve().parents[1]

    # ---- Econ leaf ----
    econ_path = repo_root / "data" / "processed" / "p_Econ_US" / "econ_leaf.parquet"
    if econ_path.exists():
        econ_df = pd.read_parquet(econ_path)
        if "econ_score" not in econ_df.columns:
            raise ValueError("econ_leaf.parquet missing 'econ_score' column")

        econ_df["econ_state"] = econ_df["econ_score"].apply(classify_econ_state)
        econ_df = econ_df.sort_values("date").reset_index(drop=True)
        econ_path.parent.mkdir(parents=True, exist_ok=True)
        econ_df.to_parquet(econ_path, index=False)
        print(f"Updated econ_leaf with econ_state → {econ_path}")
    else:
        print(f"[WARN] econ_leaf not found at {econ_path}")

    # ---- Inflation leaf ----
    infl_path = (
        repo_root
        / "data"
        / "processed"
        / "p_Inflation_US"
        / "inflation_leaf.parquet"
    )
    if infl_path.exists():
        infl_df = pd.read_parquet(infl_path)
        if "inflation_score" not in infl_df.columns:
            raise ValueError("inflation_leaf.parquet missing 'inflation_score' column")

        infl_df["inflation_regime"] = infl_df["inflation_score"].apply(
            classify_inflation_regime
        )
        infl_df = infl_df.sort_values("date").reset_index(drop=True)
        infl_path.parent.mkdir(parents=True, exist_ok=True)
        infl_df.to_parquet(infl_path, index=False)
        print(f"Updated inflation_leaf with inflation_regime → {infl_path}")
    else:
        print(f"[WARN] inflation_leaf not found at {infl_path}")


if __name__ == "__main__":
    main()
