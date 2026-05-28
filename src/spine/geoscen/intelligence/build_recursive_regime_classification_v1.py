from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def load_json(path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_recursive_regime_classification_v1():

    repo_root = Path.cwd()

    kalman = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "intelligence"
        / "kalman_recursive_layer_summary_v1.json"
    )

    fragility = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "systemic_fragility_state_machine_summary_v1.json"
    )

    policy = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "policy"
        / "recursive_policy_response_layer_summary_v1.json"
    )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "intelligence"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    latent_state = kalman.get(
        "latent_regime_state"
    )

    fragility_score = float(
        fragility.get(
            "systemic_fragility_score",
            0.0,
        )
    )

    policy_bias = policy.get(
        "dominant_policy_bias"
    )

    if (
        latent_state == "stable_latent_regime"
        and fragility_score < 0.35
    ):
        regime = "stable_liquidity_regime"

    elif (
        policy_bias == "hawkish_constraint"
        and fragility_score >= 0.35
    ):
        regime = "hawkish_recursive_fragility"

    elif fragility_score >= 0.50:
        regime = "recursive_systemic_transition"

    else:
        regime = "watch_recursive_regime"

    regime_df = pd.DataFrame(
        [
            {
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "latent_state": latent_state,
                "fragility_score": fragility_score,
                "policy_bias": policy_bias,
                "classified_regime": regime,
            }
        ]
    )

    summary = {
        "component": "recursive_regime_classification_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "classified_regime": regime,
        "latent_state": latent_state,
        "policy_bias": policy_bias,
        "fragility_score": fragility_score,
        "status": "recursive_regime_classification_complete",
    }

    parquet_path = (
        out_dir
        / "recursive_regime_classification_v1.parquet"
    )

    json_path = (
        out_dir
        / "recursive_regime_classification_v1.json"
    )

    summary_path = (
        out_dir
        / "recursive_regime_classification_summary_v1.json"
    )

    regime_df.to_parquet(
        parquet_path,
        index=False,
    )

    regime_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            indent=2,
        )

    print("Recursive regime classification complete")
    print("Classified Regime:", regime)
    print("SUMMARY:", summary_path)

    return regime_df


if __name__ == "__main__":
    build_recursive_regime_classification_v1()
