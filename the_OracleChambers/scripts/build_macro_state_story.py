from pathlib import Path
import pandas as pd

from scripts.write_metadata import write_metadata


def macro_story(econ_state: str, inflation_regime: str) -> str:
    """
    Deterministic mapping from (econ_state, inflation_regime) -> macro_story.
    Fully rule-based, CPMAI-aligned.
    """
    if econ_state is None or pd.isna(econ_state):
        econ_state = "unknown"
    if inflation_regime is None or pd.isna(inflation_regime):
        inflation_regime = "unknown"

    mapping = {
        ("contraction", "disinflation"): "Weak Growth + Disinflation",
        ("contraction", "anchored"): "Recessionary + Stable Prices",
        ("contraction", "overheat"): "Stagflation Crash Risk",

        ("soft", "disinflation"): "Soft Growth + Cooling Prices",
        ("soft", "anchored"): "Soft Growth + Anchored Inflation",
        ("soft", "overheat"): "Soft Growth + Overheating Prices",

        ("expansion", "disinflation"): "Expansion + Mild Disinflation",
        ("expansion", "anchored"): "Healthy Expansion",
        ("expansion", "overheat"): "Late-Cycle Overheating",
    }

    return mapping.get((econ_state, inflation_regime), "Unknown Macro Mix")


def market_story(macro_state_label: str) -> str:
    """
    Mapping from macro_state_label -> market_story.
    """
    mapping = {
        "pro_value": "Value Supported",
        "neutral": "Neutral Market Tone",
        "anti_value": "Value Under Pressure",
        "unknown": "Ambiguous / Inconclusive",
    }
    return mapping.get(macro_state_label, "Ambiguous / Inconclusive")


def main():
    repo_root = Path(__file__).resolve().parents[1]

    # --- Load fusion macro_state ---
    fusion_path = repo_root / "data" / "processed" / "fusion" / "macro_state_spine_us.parquet"
    if not fusion_path.exists():
        raise FileNotFoundError(f"macro_state_spine_us not found at {fusion_path}")

    fusion_df = pd.read_parquet(fusion_path)
    fusion_df["date"] = pd.to_datetime(fusion_df["date"])

    # --- Load econ & inflation regimes (from their leaves) ---
    econ_path = repo_root / "data" / "processed" / "p_Econ_US" / "econ_leaf.parquet"
    infl_path = repo_root / "data" / "processed" / "p_Inflation_US" / "inflation_leaf.parquet"

    econ_df = pd.read_parquet(econ_path)[["date", "econ_state"]]
    infl_df = pd.read_parquet(infl_path)[["date", "inflation_regime"]]

    econ_df["date"] = pd.to_datetime(econ_df["date"])
    infl_df["date"] = pd.to_datetime(infl_df["date"])

    # --- Merge regimes into fusion frame ---
    df = (
        fusion_df.merge(econ_df, on="date", how="left")
        .merge(infl_df, on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # --- Derive narrative fields ---
    df["macro_story"] = df.apply(
        lambda row: macro_story(row.get("econ_state"), row.get("inflation_regime")),
        axis=1,
    )
    df["market_story"] = df["macro_state_label"].apply(market_story)

    # --- Save narrative artifact ---
    out_dir = repo_root / "data" / "processed" / "narratives"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "macro_state_story.parquet"
    df.to_parquet(out_path, index=False)

    # --- Metadata for narrative artifact ---
    data_date = (
        df["date"].max().strftime("%Y-%m-%d") if not df["date"].isna().all() else ""
    )
    write_metadata(
        artifact_path=out_path,
        artifact_name="macro_state_story",
        artifact_type="narrative",
        channel="fusion_us",
        data_date=data_date,
        df=df,
        pipeline_config_version="macro_state_story_v1.0.0",
        notes="Rule-based macro + market narrative derived from macro_state_spine_us, econ_state, and inflation_regime.",
    )

    print(f"Wrote macro_state_story → {out_path}")


if __name__ == "__main__":
    main()

