# tests/test_artifacts_validation.py

import json
from pathlib import Path
import sys

import pandas as pd
import pytest

# Ensure project root is on sys.path so 'scripts' can be imported
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.write_metadata import compute_schema_hash


# at the top of file
def test_fedspeak_meta_files_exist():
    df = load_parquet("combined_policy_leaf")
    artifact_path = Path(
        Path(__file__).resolve().parents[1]
        / load_paths_config()["combined_policy_leaf"]
    )
    meta_path = artifact_path.with_suffix(".meta.json")

    assert meta_path.exists(), "Missing metadata file for combined_policy_leaf"


def test_fedspeak_schema_matches_metadata():
    df = load_parquet("combined_policy_leaf")

    artifact_path = (
        Path(__file__).resolve().parents[1]
        / load_paths_config()["combined_policy_leaf"]
    )
    meta_path = artifact_path.with_suffix(".meta.json")

    if not meta_path.exists():
        pytest.skip("No meta file yet (expected for early dev).")

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    # 1) Same columns
    assert set(meta["schema_columns"]) == set(df.columns), (
        "Metadata schema_columns does not match DataFrame columns"
    )

    # 2) Same schema hash
    assert meta["schema_hash"] == compute_schema_hash(
        df.columns
    ), "Metadata schema_hash mismatch"


def test_fedspeak_record_count_matches_metadata():
    df = load_parquet("combined_policy_leaf")

    artifact_path = (
        Path(__file__).resolve().parents[1]
        / load_paths_config()["combined_policy_leaf"]
    )
    meta_path = artifact_path.with_suffix(".meta.json")

    if not meta_path.exists():
        pytest.skip("No meta file yet (expected for early dev).")

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    assert meta["record_count"] == len(df), (
        "Metadata record_count does not match DataFrame length"
    )





def load_parquet(artifact_key: str) -> pd.DataFrame:
    """
    Load a Parquet artifact based on its logical key (e.g., 'econ_leaf').

    The actual path is defined in config/artifacts_paths.json.
    """
    paths = load_paths_config()
    if artifact_key not in paths:
        raise KeyError(f"Artifact key '{artifact_key}' not found in artifacts_paths.json")

    repo_root = Path(__file__).resolve().parents[1]
    parquet_path = repo_root / paths[artifact_key]

    if not parquet_path.exists():
        # For required artifacts, this is a hard failure
        raise FileNotFoundError(f"Artifact '{artifact_key}' not found at {parquet_path}")

    return pd.read_parquet(parquet_path)

# ---- Helpers ----

def load_paths_config() -> dict:
    """
    Load artifact path configuration from a JSON file located in config/.
    Paths are defined outside of the code to avoid hard-coding storage locations.
    """
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config" / "artifacts_paths.json"
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)




# ---- Econ pillar tests ----

def test_inflation_leaf_structure_and_bounds():
    df = load_parquet("inflation_leaf")

    expected_cols = {"date", "inflation_score", "inflation_regime"}
    assert expected_cols.issubset(
        df.columns
    ), "inflation_leaf missing required columns"

    assert df["inflation_score"].between(
        -1, 1
    ).all(), "inflation_score must be in [-1, 1]"
    assert df["date"].is_monotonic_increasing, "inflation_leaf dates should be sorted"

    allowed_regimes = {"disinflation", "anchored", "overheat", "unknown"}
    assert set(df["inflation_regime"]).issubset(
        allowed_regimes
    ), "inflation_regime contains unexpected labels"



# ---- WTI / energy pillar tests ----

def test_wti_pressure_leaf_structure_and_bounds():
    df = load_parquet("wti_pressure_leaf")
    expected_cols = {"date", "wti_pressure_score"}

    assert expected_cols.issubset(
        df.columns
    ), "wti_pressure_leaf missing required columns"

    assert df["wti_pressure_score"].between(
        -1, 1
    ).all(), "wti_pressure_score must be in [-1, 1]"


# ---- FedSpeak combined policy leaf tests ----

def test_fedspeak_combined_policy_leaf_structure_and_bounds():
    df = load_parquet("combined_policy_leaf")
    expected_cols = {
        "event_id",
        "event_date",
        "inflation_risk",
        "growth_risk",
        "policy_bias",
    }

    assert expected_cols.issubset(
        df.columns
    ), "combined_policy_leaf missing required core columns"

    for col in ["inflation_risk", "growth_risk", "policy_bias"]:
        assert df[col].between(-1, 1).all(), f"{col} must be in [-1, 1]"



# ---- VinV / equity pillar tests ----

def test_vinv_signal_structure_and_bounds():
    df = load_parquet("vinv_signal")
    expected_cols = {
        "date",
        "vinv_score",
        "macro_value_support",
        "policy_value_support",
    }

    assert expected_cols.issubset(df.columns), "vinv_signal missing required columns"

    assert df["vinv_score"].between(-1, 1).all(), "vinv_score must be in [-1, 1]"
    assert df["macro_value_support"].between(
        -1, 1
    ).all(), "macro_value_support must be in [-1, 1]"
    assert df["policy_value_support"].between(
        -1, 1
    ).all(), "policy_value_support must be in [-1, 1]"

    assert df["date"].is_monotonic_increasing, "vinv_signal dates should be sorted"
    assert not df["date"].duplicated().any(), "vinv_signal should not have duplicate dates"


# ---- Technicals pillar tests ----

def test_technical_leaf_structure_and_bounds():
    df = load_parquet("technical_leaf")
    expected_cols = {
        "date",
        "liquidity_score",
        "credit_stress_score",
        "vol_regime_score",
        "breadth_score",
        "fx_risk_score",
        "metals_signal_score",
        "overall_technical_regime",
    }

    assert expected_cols.issubset(
        df.columns
    ), "technical_leaf missing required columns"

    score_cols = [
        "liquidity_score",
        "credit_stress_score",
        "vol_regime_score",
        "breadth_score",
        "fx_risk_score",
        "metals_signal_score",
    ]
    for col in score_cols:
        assert df[col].between(-1, 1).all(), f"{col} must be in [-1, 1]"


# ---- Fusion layer tests ----

def test_macro_state_spine_us_structure_and_bounds():
    df = load_parquet("macro_state_spine_us")

    expected_cols = {
        "date",
        "econ_score",
        "inflation_score",
        "wti_pressure_score",
        "avg_policy_bias",
        "avg_inflation_risk",
        "avg_growth_risk",
        "vinv_score",
        "overall_technical_regime",
        "macro_heat",
        "policy_stance",
        "macro_state_label",
    }

    assert expected_cols.issubset(
        df.columns
    ), "macro_state_spine_us missing required columns"

    bounded_cols = [
        "econ_score",
        "inflation_score",
        "wti_pressure_score",
        "avg_policy_bias",
        "avg_inflation_risk",
        "avg_growth_risk",
        "vinv_score",
    ]

    for col in bounded_cols:
        non_null = df[col].dropna()
        # Allow NaN for missing pillars, but enforce bounds on actual values
        if not non_null.empty:
            assert non_null.between(-1, 1).all(), (
                f"{col} must be in [-1, 1] whenever present"
            )

    allowed_labels = {"pro_value", "neutral", "anti_value", "unknown"}
    assert set(df["macro_state_label"]).issubset(
        allowed_labels
    ), "macro_state_label contains unexpected values"


# ---- OracleChambers narrative sanity tests ----

@pytest.mark.parametrize(
    "artifact_key, label_col",
    [
        ("macro_state_story", "macro_state_label"),
        ("vinv_story", "vinv_regime"),
    ],
)
def test_narrative_artifacts_reference_valid_labels(artifact_key, label_col):
    """
    Narrative artifacts are optional labs for now.
    If present, they must not invent new labels.
    If absent, this test is skipped.
    """
    try:
        narratives_df = load_parquet(artifact_key)
    except FileNotFoundError:
        pytest.skip(f"Optional narrative artifact '{artifact_key}' not found.")
        return

    fusion_df = load_parquet("macro_state_spine_us")

    if label_col == "macro_state_label":
        allowed_labels = set(fusion_df["macro_state_label"])
        assert set(narratives_df[label_col]).issubset(
            allowed_labels
        ), f"{artifact_key} uses labels not present in macro_state_spine_us"

    # For vinv_regime, we can tighten the contract once we standardize regime labels

def test_econ_leaf_structure_bounds_and_regimes():
    df = load_parquet("econ_leaf")

    expected_cols = {"date", "econ_score", "econ_state"}
    assert expected_cols.issubset(df.columns), "econ_leaf missing required columns"

    # Bounds
    assert df["econ_score"].between(-1, 1).all(), "econ_score must be in [-1, 1]"

    # Dates
    assert df["date"].is_monotonic_increasing, "econ_leaf dates should be sorted"

    # Regime labels
    allowed_states = {"contraction", "soft", "expansion", "unknown"}
    assert set(df["econ_state"]).issubset(
        allowed_states
    ), "econ_state contains unexpected labels"

# ---- FedSpeak diagnostics tests (optional artifacts) ----

def test_fedspeak_drift_report_structure():
    """
    Drift report is optional; if present, it must have the expected columns.
    """
    try:
        df = load_parquet("fedspeak_drift_report")
    except (FileNotFoundError, KeyError):
        pytest.skip("Optional FedSpeak artifact 'fedspeak_drift_report' not found.")
        return

    expected_cols = {
        "event_date",
        "inflation_risk_z",
        "growth_risk_z",
        "policy_bias_z",
    }
    assert expected_cols.issubset(df.columns), "fedspeak_drift_report missing required columns"


def test_fedspeak_stability_structure():
    """
    Stability summary is optional; if present, it must have the expected columns.
    """
    try:
        df = load_parquet("fedspeak_stability")
    except (FileNotFoundError, KeyError):
        pytest.skip("Optional FedSpeak artifact 'fedspeak_stability' not found.")
        return

    expected_cols = {
        "quarter",
        "quarter_start",
        "quarter_end",
        "n_events",
        "mean_policy_bias",
        "std_policy_bias",
        "mean_inflation_risk",
        "std_inflation_risk",
        "mean_growth_risk",
        "std_growth_risk",
    }
    assert expected_cols.issubset(df.columns), "fedspeak_stability missing required columns"

