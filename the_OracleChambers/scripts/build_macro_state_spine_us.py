import json
from pathlib import Path

import pandas as pd

from scripts.write_metadata import write_metadata  # you already created this


def load_config() -> dict:
    repo_root = Path(__file__).resolve().parents[1]
    cfg_path = repo_root / "config" / "pipelines" / "macro_state_spine_us.json"
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_parquet(repo_root: Path, relative_path: str) -> pd.DataFrame:
    return pd.read_parquet(repo_root / relative_path)


def _prep_macro_leaf(df: pd.DataFrame, score_col: str, date_col: str = "date") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    return df[[date_col, score_col]].dropna().drop_duplicates()


def _prep_technical_leaf(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df[
        [
            "date",
            "liquidity_score",
            "credit_stress_score",
            "vol_regime_score",
            "breadth_score",
            "fx_risk_score",
            "metals_signal_score",
            "overall_technical_regime",
        ]
    ].dropna(subset=["date"]).drop_duplicates()


def _prep_vinv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "vinv_score", "macro_value_support", "policy_value_support"]].drop_duplicates()


def _prep_policy(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Expect event_date in combined_policy_leaf
    if "event_date" not in df.columns:
        raise ValueError("combined_policy_leaf must contain 'event_date' column.")
    df["date"] = pd.to_datetime(df["event_date"])

    required_cols = ["policy_bias", "inflation_risk", "growth_risk"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"combined_policy_leaf missing required column: {c}")

    grouped = (
        df.groupby("date", as_index=False)[required_cols].mean()
        .sort_values("date")
        .reset_index(drop=True)
    )
    grouped.rename(
        columns={
            "inflation_risk": "avg_inflation_risk",
            "growth_risk": "avg_growth_risk",
            "policy_bias": "avg_policy_bias",
        },
        inplace=True,
    )
    return grouped


def classify_macro_state(
    row,
    thresholds: dict,
):
    """
    Rule-based classification of macro_state_label.

    - If any critical pillar is missing → 'unknown'
    - Else use macro_heat, policy_stance, vinv_score to decide:
        - 'pro_value'
        - 'anti_value'
        - 'neutral'
    """
    mh = row["macro_heat"]
    ps = row["policy_stance"]
    vinv = row["vinv_score"]

    if pd.isna(mh) or pd.isna(ps) or pd.isna(vinv):
        return "unknown"

    mh_thr = thresholds["macro_heat"]
    ps_thr = thresholds["policy_stance"]
    vinv_thr = thresholds["vinv_score"]

    # Pro-value regime: decent macro_heat, non-restrictive policy, + vinv support
    if (
        mh >= mh_thr["pro_value_min"]
        and ps <= ps_thr["pro_value_max"]
        and vinv >= vinv_thr["pro_value_min"]
    ):
        return "pro_value"

    # Anti-value regime: weak macro_heat OR restrictive policy OR vinv structurally negative
    if (
        mh <= mh_thr["anti_value_max"]
        or ps >= ps_thr["anti_value_min"]
        or vinv <= vinv_thr["anti_value_max"]
    ):
        return "anti_value"

    return "neutral"


def build_macro_state():
    repo_root = Path(__file__).resolve().parents[1]
    cfg = load_config()
    inputs = cfg["inputs"]
    output_path = repo_root / cfg["output"]["macro_state_spine_us"]

    # --- Load leaves ---
    econ = _load_parquet(repo_root, inputs["econ_leaf"])
    infl = _load_parquet(repo_root, inputs["inflation_leaf"])
    wti = _load_parquet(repo_root, inputs["wti_pressure_leaf"])
    policy = _load_parquet(repo_root, inputs["combined_policy_leaf"])
    vinv = _load_parquet(repo_root, inputs["vinv_signal"])
    tech = _load_parquet(repo_root, inputs["technical_leaf"])

    econ_p = _prep_macro_leaf(econ, "econ_score")
    infl_p = _prep_macro_leaf(infl, "inflation_score")
    wti_p = _prep_macro_leaf(wti, "wti_pressure_score")
    policy_p = _prep_policy(policy)
    vinv_p = _prep_vinv(vinv)
    tech_p = _prep_technical_leaf(tech)

    # --- Merge on date ---
    fusion = (
        econ_p.merge(infl_p, on="date", how="outer")
        .merge(wti_p, on="date", how="outer")
        .merge(policy_p, on="date", how="outer")
        .merge(vinv_p, on="date", how="outer")
        .merge(tech_p, on="date", how="outer")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # --- Derived fields ---
    fusion["macro_heat"] = (
        fusion["econ_score"] - fusion["inflation_score"] - fusion["wti_pressure_score"]
    )

    fusion["policy_stance"] = (
        fusion["avg_policy_bias"]
        + fusion["avg_inflation_risk"]
        - fusion["avg_growth_risk"]
    )

    # For completeness: alias overall_technical_regime as technical_index if desired
    # but we keep existing column to match tests:
    # fusion["technical_index"] = fusion["overall_technical_regime"]

    # --- Classification ---
    thresholds = cfg["classification_thresholds"]
    fusion["macro_state_label"] = fusion.apply(
        classify_macro_state, axis=1, thresholds=thresholds
    )

    # --- Save artifact ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fusion.to_parquet(output_path, index=False)

    # --- Write metadata ---
    # Use latest date as data_date for this fused artifact
    data_date = (
        fusion["date"].max().strftime("%Y-%m-%d") if not fusion["date"].isna().all() else ""
    )
    write_metadata(
        artifact_path=output_path,
        artifact_name="macro_state_spine_us",
        artifact_type="fusion_leaf",
        channel="fusion_us",
        data_date=data_date,
        df=fusion,
        pipeline_config_version=cfg.get("version", "v1.0.0"),
        notes="Fusion of macro, FedSpeak, VinV, and technical pillars into macro_state_spine_us."
    )


if __name__ == "__main__":
    build_macro_state()
