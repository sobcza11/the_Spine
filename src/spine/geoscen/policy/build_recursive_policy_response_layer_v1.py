from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


ISM_QUAL_INPUTS = [
    {
        "component": "new_orders_tone",
        "score": 0.38,
        "weight": 0.18,
        "policy_channel": "growth_slowdown",
    },
    {
        "component": "prices_paid_tone",
        "score": 0.44,
        "weight": 0.20,
        "policy_channel": "inflation_persistence",
    },
    {
        "component": "employment_tone",
        "score": 0.31,
        "weight": 0.14,
        "policy_channel": "labor_fragility",
    },
    {
        "component": "supplier_delivery_tone",
        "score": 0.36,
        "weight": 0.14,
        "policy_channel": "supply_chain_pressure",
    },
    {
        "component": "inventory_tone",
        "score": 0.29,
        "weight": 0.12,
        "policy_channel": "demand_fragility",
    },
    {
        "component": "business_uncertainty_tone",
        "score": 0.41,
        "weight": 0.22,
        "policy_channel": "forward_uncertainty",
    },
]


def load_json(path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def classify_policy_state(score):
    if score >= 0.80:
        return "emergency_policy_response_risk"

    if score >= 0.65:
        return "high_policy_response_pressure"

    if score >= 0.50:
        return "elevated_policy_response_pressure"

    if score >= 0.35:
        return "watch_policy_response_pressure"

    return "stable_policy_response"


def build_recursive_policy_response_layer_v1():
    repo_root = Path.cwd()

    rates_summary = load_json(
        repo_root
        / "data"
        / "rates"
        / "recursive"
        / "rates_recursive_pressure_summary_v1.json"
    )

    fx_summary = load_json(
        repo_root
        / "data"
        / "fx"
        / "recursive"
        / "fx_recursive_stress_summary_v1.json"
    )

    finstate_summary = load_json(
        repo_root
        / "data"
        / "finstate"
        / "recursive"
        / "finstate_recursive_integration_summary_v1.json"
    )

    fusion_summary = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "fusion"
        / "cross_domain_recursive_fusion_summary_v1.json"
    )

    projection_summary = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "projection"
        / "recursive_scenario_projection_summary_v1.json"
    )

    governance_summary = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "geoscen_recursive_governance_summary_v1.json"
    )

    out_dir = repo_root / "data" / "geoscen" / "policy"
    out_dir.mkdir(parents=True, exist_ok=True)

    ism_rows = []

    for item in ISM_QUAL_INPUTS:
        weighted_score = round(
            min(1.0, item["score"] * item["weight"]),
            4,
        )

        ism_rows.append(
            {
                "component": item["component"],
                "raw_score": item["score"],
                "weight": item["weight"],
                "weighted_score": weighted_score,
                "policy_channel": item["policy_channel"],
            }
        )

    ism_df = pd.DataFrame(ism_rows)

    ism_qual_policy_pressure = round(
        float(ism_df["weighted_score"].sum()),
        4,
    )

    rates_pressure = float(
        rates_summary.get("rates_recursive_pressure", 0.0) or 0.0
    )

    fx_pressure = float(
        fx_summary.get("fx_recursive_pressure", 0.0) or 0.0
    )

    finstate_pressure = float(
        finstate_summary.get("finstate_recursive_pressure", 0.0) or 0.0
    )

    cross_domain_pressure = float(
        fusion_summary.get("cross_domain_recursive_pressure", 0.0) or 0.0
    )

    projected_systemic_score = float(
        projection_summary.get("max_projected_systemic_score", 0.0) or 0.0
    )

    projected_cascade_probability = float(
        projection_summary.get("max_projected_cascade_probability", 0.0) or 0.0
    )

    governance_pressure = float(
        governance_summary.get("governance_pressure", 0.0) or 0.0
    )

    policy_response_pressure = round(
        min(
            1.0,
            0.20 * rates_pressure
            + 0.15 * fx_pressure
            + 0.15 * finstate_pressure
            + 0.18 * ism_qual_policy_pressure
            + 0.14 * cross_domain_pressure
            + 0.10 * projected_systemic_score
            + 0.05 * projected_cascade_probability
            + 0.03 * governance_pressure,
        ),
        4,
    )

    inflation_constraint_pressure = round(
        min(
            1.0,
            0.45 * ism_df.loc[
                ism_df["policy_channel"] == "inflation_persistence",
                "raw_score",
            ].max()
            + 0.30 * rates_pressure
            + 0.25 * fx_pressure,
        ),
        4,
    )

    growth_support_pressure = round(
        min(
            1.0,
            0.35 * ism_df.loc[
                ism_df["policy_channel"] == "growth_slowdown",
                "raw_score",
            ].max()
            + 0.30 * finstate_pressure
            + 0.20 * cross_domain_pressure
            + 0.15 * projected_systemic_score,
        ),
        4,
    )

    liquidity_stabilization_pressure = round(
        min(
            1.0,
            0.35 * rates_pressure
            + 0.25 * fx_pressure
            + 0.25 * cross_domain_pressure
            + 0.15 * projected_cascade_probability,
        ),
        4,
    )

    policy_tension_score = round(
        abs(inflation_constraint_pressure - growth_support_pressure),
        4,
    )

    if inflation_constraint_pressure > growth_support_pressure:
        dominant_policy_bias = "hawkish_constraint"
    elif growth_support_pressure > inflation_constraint_pressure:
        dominant_policy_bias = "dovish_support"
    else:
        dominant_policy_bias = "balanced_policy_bias"

    policy_state = classify_policy_state(policy_response_pressure)

    response_paths = [
        {
            "policy_path": "no_response",
            "probability_proxy": round(max(0.0, 1.0 - policy_response_pressure), 4),
            "description": "Policy remains reactive rather than preemptive.",
        },
        {
            "policy_path": "hawkish_hold",
            "probability_proxy": round(inflation_constraint_pressure * 0.55, 4),
            "description": "Inflation persistence limits easing flexibility.",
        },
        {
            "policy_path": "liquidity_stabilization",
            "probability_proxy": round(liquidity_stabilization_pressure * 0.60, 4),
            "description": "Authorities respond to market-function or liquidity stress.",
        },
        {
            "policy_path": "growth_support_shift",
            "probability_proxy": round(growth_support_pressure * 0.50, 4),
            "description": "Growth deterioration increases support probability.",
        },
    ]

    response_df = pd.DataFrame(response_paths)

    summary = {
        "component": "recursive_policy_response_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "policy_response_pressure": policy_response_pressure,
        "policy_response_state": policy_state,
        "ism_qual_policy_pressure": ism_qual_policy_pressure,
        "inflation_constraint_pressure": inflation_constraint_pressure,
        "growth_support_pressure": growth_support_pressure,
        "liquidity_stabilization_pressure": liquidity_stabilization_pressure,
        "policy_tension_score": policy_tension_score,
        "dominant_policy_bias": dominant_policy_bias,
        "inputs": {
            "rates_pressure": round(rates_pressure, 4),
            "fx_pressure": round(fx_pressure, 4),
            "finstate_pressure": round(finstate_pressure, 4),
            "cross_domain_pressure": round(cross_domain_pressure, 4),
            "projected_systemic_score": round(projected_systemic_score, 4),
            "projected_cascade_probability": round(projected_cascade_probability, 4),
            "governance_pressure": round(governance_pressure, 4),
        },
        "ism_policy_channels": ism_df.to_dict(orient="records"),
        "policy_response_paths": response_paths,
        "status": "recursive_policy_response_layer_complete",
    }

    ism_parquet = out_dir / "recursive_policy_ism_quals_v1.parquet"
    ism_json = out_dir / "recursive_policy_ism_quals_v1.json"
    response_parquet = out_dir / "recursive_policy_response_paths_v1.parquet"
    response_json = out_dir / "recursive_policy_response_paths_v1.json"
    summary_path = out_dir / "recursive_policy_response_layer_summary_v1.json"

    ism_df.to_parquet(ism_parquet, index=False)
    response_df.to_parquet(response_parquet, index=False)

    ism_df.to_json(
        ism_json,
        orient="records",
        indent=2,
    )

    response_df.to_json(
        response_json,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive policy response layer complete")
    print("Policy Response Pressure:", policy_response_pressure)
    print("Policy Response State:", policy_state)
    print("ISM Qual Policy Pressure:", ism_qual_policy_pressure)
    print("Dominant Policy Bias:", dominant_policy_bias)
    print("ISM PARQUET:", ism_parquet)
    print("RESPONSE PARQUET:", response_parquet)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return summary


if __name__ == "__main__":
    build_recursive_policy_response_layer_v1()
