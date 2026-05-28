from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


DOMAIN_CONFIG = {
    "COT": {
        "summary_path": ["data", "geoscen", "recursive", "geoscen_systemic_escalation_registry_summary_v1.json"],
        "score_key": "systemic_escalation_score",
        "state_key": "systemic_state",
        "weight": 0.25,
    },
    "FINSTATE": {
        "summary_path": ["data", "finstate", "recursive", "finstate_recursive_integration_summary_v1.json"],
        "score_key": "finstate_recursive_pressure",
        "state_key": "finstate_recursive_state",
        "weight": 0.25,
    },
    "RATES": {
        "summary_path": ["data", "rates", "recursive", "rates_recursive_pressure_summary_v1.json"],
        "score_key": "rates_recursive_pressure",
        "state_key": "rates_recursive_state",
        "weight": 0.25,
    },
    "FX": {
        "summary_path": ["data", "fx", "recursive", "fx_recursive_stress_summary_v1.json"],
        "score_key": "fx_recursive_pressure",
        "state_key": "fx_recursive_state",
        "weight": 0.25,
    },
}


FUSION_LINKS = [
    {
        "source_domain": "FX",
        "target_domain": "RATES",
        "channel": "currency_to_rates_reflexivity",
        "amplification_weight": 0.30,
    },
    {
        "source_domain": "RATES",
        "target_domain": "FINSTATE",
        "channel": "rates_to_corporate_fragility",
        "amplification_weight": 0.30,
    },
    {
        "source_domain": "FINSTATE",
        "target_domain": "COT",
        "channel": "corporate_fragility_to_positioning",
        "amplification_weight": 0.25,
    },
    {
        "source_domain": "COT",
        "target_domain": "FX",
        "channel": "positioning_to_currency_stress",
        "amplification_weight": 0.20,
    },
    {
        "source_domain": "FX",
        "target_domain": "COT",
        "channel": "fx_to_positioning_instability",
        "amplification_weight": 0.25,
    },
    {
        "source_domain": "RATES",
        "target_domain": "COT",
        "channel": "rates_to_positioning_instability",
        "amplification_weight": 0.25,
    },
]


def classify_fusion_state(score):
    if score >= 0.80:
        return "systemic_cross_domain_recursion"

    if score >= 0.65:
        return "fragile_cross_domain_recursion"

    if score >= 0.50:
        return "elevated_cross_domain_recursion"

    if score >= 0.35:
        return "watch_cross_domain_recursion"

    return "stable_cross_domain_recursion"


def load_summary(repo_root, config):
    path = repo_root.joinpath(*config["summary_path"])

    if not path.exists():
        return None, path

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f), path


def build_cross_domain_recursive_fusion_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "geoscen" / "fusion"
    out_dir.mkdir(parents=True, exist_ok=True)

    domain_rows = []

    for domain, config in DOMAIN_CONFIG.items():
        summary, path = load_summary(repo_root, config)

        if summary is None:
            domain_rows.append(
                {
                    "domain": domain,
                    "source_available": False,
                    "source_path": str(path),
                    "domain_score": 0.0,
                    "domain_state": "missing",
                    "domain_weight": config["weight"],
                    "weighted_domain_pressure": 0.0,
                }
            )
            continue

        score = float(summary.get(config["score_key"], 0.0) or 0.0)
        weight = float(config["weight"])

        domain_rows.append(
            {
                "domain": domain,
                "source_available": True,
                "source_path": str(path),
                "domain_score": round(score, 4),
                "domain_state": summary.get(config["state_key"], "unknown"),
                "domain_weight": weight,
                "weighted_domain_pressure": round(score * weight, 4),
            }
        )

    domain_df = pd.DataFrame(domain_rows)

    score_map = dict(zip(domain_df["domain"], domain_df["domain_score"]))
    state_map = dict(zip(domain_df["domain"], domain_df["domain_state"]))

    fusion_rows = []

    for link in FUSION_LINKS:
        source = link["source_domain"]
        target = link["target_domain"]

        source_score = float(score_map.get(source, 0.0))
        target_score = float(score_map.get(target, 0.0))
        amplification_weight = float(link["amplification_weight"])

        cross_domain_feedback = round(
            min(
                1.0,
                source_score * target_score * amplification_weight,
            ),
            4,
        )

        adjusted_target_pressure = round(
            min(
                1.0,
                target_score + cross_domain_feedback,
            ),
            4,
        )

        fusion_rows.append(
            {
                "source_domain": source,
                "target_domain": target,
                "channel": link["channel"],
                "source_score": round(source_score, 4),
                "target_score": round(target_score, 4),
                "source_state": state_map.get(source, "unknown"),
                "target_state": state_map.get(target, "unknown"),
                "amplification_weight": amplification_weight,
                "cross_domain_feedback": cross_domain_feedback,
                "adjusted_target_pressure": adjusted_target_pressure,
                "fusion_state": classify_fusion_state(adjusted_target_pressure),
            }
        )

    fusion_df = pd.DataFrame(fusion_rows)

    cross_domain_recursive_pressure = round(
        float(domain_df["weighted_domain_pressure"].sum()),
        4,
    )

    avg_cross_domain_feedback = round(
        float(fusion_df["cross_domain_feedback"].mean()),
        4,
    )

    max_cross_domain_feedback = round(
        float(fusion_df["cross_domain_feedback"].max()),
        4,
    )

    avg_adjusted_target_pressure = round(
        float(fusion_df["adjusted_target_pressure"].mean()),
        4,
    )

    max_adjusted_target_pressure = round(
        float(fusion_df["adjusted_target_pressure"].max()),
        4,
    )

    systemic_recursive_state = classify_fusion_state(
        max(
            cross_domain_recursive_pressure,
            avg_adjusted_target_pressure,
        )
    )

    summary = {
        "component": "cross_domain_recursive_fusion_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "domain_count": int(len(domain_df)),
        "active_domain_count": int(domain_df["source_available"].sum()),
        "fusion_link_count": int(len(fusion_df)),
        "cross_domain_recursive_pressure": cross_domain_recursive_pressure,
        "avg_cross_domain_feedback": avg_cross_domain_feedback,
        "max_cross_domain_feedback": max_cross_domain_feedback,
        "avg_adjusted_target_pressure": avg_adjusted_target_pressure,
        "max_adjusted_target_pressure": max_adjusted_target_pressure,
        "systemic_recursive_state": systemic_recursive_state,
        "domain_states": dict(zip(domain_df["domain"], domain_df["domain_state"])),
        "highest_feedback_links": fusion_df.sort_values(
            "cross_domain_feedback",
            ascending=False,
        )
        .head(3)[
            [
                "source_domain",
                "target_domain",
                "cross_domain_feedback",
            ]
        ]
        .to_dict(orient="records"),
        "status": "cross_domain_recursive_fusion_complete",
    }

    domain_parquet = out_dir / "cross_domain_recursive_fusion_domains_v1.parquet"
    fusion_parquet = out_dir / "cross_domain_recursive_fusion_links_v1.parquet"
    domain_json = out_dir / "cross_domain_recursive_fusion_domains_v1.json"
    fusion_json = out_dir / "cross_domain_recursive_fusion_links_v1.json"
    summary_path = out_dir / "cross_domain_recursive_fusion_summary_v1.json"

    domain_df.to_parquet(domain_parquet, index=False)
    fusion_df.to_parquet(fusion_parquet, index=False)

    domain_df.to_json(
        domain_json,
        orient="records",
        indent=2,
    )

    fusion_df.to_json(
        fusion_json,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Cross-domain recursive fusion complete")
    print("Domains:", len(domain_df))
    print("Fusion Links:", len(fusion_df))
    print("Cross-Domain Recursive Pressure:", cross_domain_recursive_pressure)
    print("Systemic Recursive State:", systemic_recursive_state)
    print("DOMAIN PARQUET:", domain_parquet)
    print("FUSION PARQUET:", fusion_parquet)
    print("DOMAIN JSON:", domain_json)
    print("FUSION JSON:", fusion_json)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return domain_df, fusion_df


if __name__ == "__main__":
    build_cross_domain_recursive_fusion_v1()
