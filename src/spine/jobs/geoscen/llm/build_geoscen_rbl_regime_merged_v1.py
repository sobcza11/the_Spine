from pathlib import Path

import pandas as pd


def main():
    repo_root = Path.cwd()

    rbl_path = repo_root / "data" / "geoscen" / "llm" / "geoscen_rbl_template_v2.parquet"
    regime_path = repo_root / "data" / "geoscen" / "llm" / "geoscen_regime_layer_v1.parquet"

    out_path = repo_root / "data" / "geoscen" / "llm" / "geoscen_rbl_regime_merged_v1.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not rbl_path.exists():
        raise FileNotFoundError(f"Missing RBL file: {rbl_path}")

    if not regime_path.exists():
        raise FileNotFoundError(f"Missing regime file: {regime_path}")

    rbl = pd.read_parquet(rbl_path).copy()
    regime = pd.read_parquet(regime_path).copy()

    rbl["date"] = pd.to_datetime(rbl["date"])
    regime["date"] = pd.to_datetime(regime["date"])

    needed_rbl = {"date", "rbl_report"}
    needed_regime = {"date", "regime_label", "regime_confidence"}

    missing_rbl = needed_rbl - set(rbl.columns)
    missing_regime = needed_regime - set(regime.columns)

    if missing_rbl:
        raise KeyError(f"RBL file missing columns: {missing_rbl}")

    if missing_regime:
        raise KeyError(f"Regime file missing columns: {missing_regime}")

    df = rbl.merge(
        regime[["date", "regime_label", "regime_confidence"]],
        on="date",
        how="left",
        validate="one_to_one",
    )

    df["regime_label"] = df["regime_label"].fillna("Unclassified / Missing Regime")
    df["regime_confidence"] = df["regime_confidence"].fillna(0.0)

    df["regime_block"] = (
        "Regime: "
        + df["regime_label"].astype(str)
        + "\n"
        + "Confidence: "
        + df["regime_confidence"].round(3).astype(str)
    )

    df["rbl_report_with_regime"] = (
        df["rbl_report"].astype(str).str.strip()
        + "\n\n"
        + df["regime_block"]
    )

    preferred_cols = [
        "date",
        "regime_label",
        "regime_confidence",
        "dominance_mean",
        "signal_strength",
        "tone_direction",
        "rbl_report",
        "regime_block",
        "rbl_report_with_regime",
    ]

    remaining_cols = [c for c in df.columns if c not in preferred_cols]
    df = df[preferred_cols + remaining_cols]

    df = df.sort_values("date").reset_index(drop=True)
    df.to_parquet(out_path, index=False)

    print("OK | GeoScen RBL + Regime merged v1")
    print(f"output: {out_path}")
    print(df[["date", "regime_label", "regime_confidence"]])


if __name__ == "__main__":
    main()
    