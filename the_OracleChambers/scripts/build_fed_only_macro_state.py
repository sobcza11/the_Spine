from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from fed_speak.fusion_fed_speak import (
    compute_fed_regime_weights,
    REGIME_NAMES,
    build_quantile_thresholds,
)


def _pick_discrete_regime(weights: Dict[str, float]) -> str:
    """
    Pick a single regime label from FedSpeak-implied weights.

    Logic:
      - Prefer the highest non-'Unknown' regime.
      - If even that is very small (< 0.15), return 'Unknown'.
    """
    cleaned = {k: float(v) for k, v in weights.items()}

    if all(abs(v) < 1e-6 for v in cleaned.values()):
        return "Unknown"

    # Prefer non-Unknown regimes when possible
    candidates = [r for r in REGIME_NAMES if r != "Unknown"]
    best_regime = max(candidates, key=lambda r: cleaned.get(r, 0.0))

    if cleaned.get(best_regime, 0.0) < 0.15:
        return "Unknown"

    return best_regime


def main() -> None:
    data_dir = Path("data")
    fed_path = data_dir / "fed_block_for_spine.csv"

    if not fed_path.exists():
        print(f"! Fed block not found: {fed_path}")
        return

    df = pd.read_csv(fed_path, parse_dates=["date"])
    print(f"Loaded Fed block: {df.shape[0]} rows")

    # Build data-driven thresholds for composite signals
    thresholds = build_quantile_thresholds(df)
    print("Using thresholds:")
    for k, (lo, hi) in thresholds.items():
        print(f"  {k}: low={lo:.6f}, high={hi:.6f}")

    regime_weight_rows = []
    discrete_labels = []

    for _, row in df.iterrows():
        fed_out = compute_fed_regime_weights(row, thresholds=thresholds)
        weights = fed_out.weights

        # Expand into columns with a clear prefix
        weight_row = {
            f"fed_regime_{k}": float(v) for k, v in weights.items()
        }
        regime_weight_rows.append(weight_row)

        label = _pick_discrete_regime(weights)
        discrete_labels.append(label)

    weights_df = pd.DataFrame(regime_weight_rows)

    df_out = pd.concat([df.reset_index(drop=True), weights_df], axis=1)
    df_out["macro_state_spine_us_fed_only"] = discrete_labels

    out_csv = data_dir / "fed_only_macro_state.csv"
    out_json = data_dir / "fed_only_macro_state.json"

    df_out.to_csv(out_csv, index=False)
    df_out.to_json(out_json, orient="records", indent=2, date_format="iso")

    print("\nSaved Fed-only macro state to:")
    print(f"  {out_csv}")
    print(f"  {out_json}")
    print("\nSample of regime columns:")
    cols = [c for c in df_out.columns if c.startswith("fed_regime_")]
    print(cols + ["macro_state_spine_us_fed_only"])


if __name__ == "__main__":
    main()

