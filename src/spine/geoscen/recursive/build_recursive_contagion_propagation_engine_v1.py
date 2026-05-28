from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


PROPAGATION_LINKS = [
    {
        "source": "RTY",
        "target": "ES",
        "weight": 0.85,
        "channel": "equity_beta",
    },
    {
        "source": "RTY",
        "target": "NQ",
        "weight": 0.75,
        "channel": "growth_fragility",
    },
    {
        "source": "GBP",
        "target": "EUR",
        "weight": 0.65,
        "channel": "fx_instability",
    },
    {
        "source": "BTC",
        "target": "NQ",
        "weight": 0.70,
        "channel": "risk_asset_reflexivity",
    },
    {
        "source": "CHF",
        "target": "US10Y",
        "weight": 0.60,
        "channel": "flight_to_safety",
    },
]


def classify_contagion_state(score):
    if score >= 0.80:
        return "recursive_cascade"

    if score >= 0.65:
        return "systemic_contagion"

    if score >= 0.50:
        return "fragile_contagion"

    if score >= 0.35:
        return "elevated_contagion"

    if score >= 0.20:
        return "watch_contagion"

    return "stable_contagion"


def build_recursive_contagion_propagation_engine_v1():
    repo_root = Path.cwd()

    geoscen_route_path = (
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_geoscen_route_v1.parquet"
    )

    recursive_summary_path = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "recursive_escalation_engine_summary_v1.json"
    )

    fragility_summary_path = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "systemic_fragility_state_machine_summary_v1.json"
    )

    memory_summary_path = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "recursive_topology_memory_summary_v1.json"
    )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    route_df = pd.read_parquet(geoscen_route_path).copy()

    with open(recursive_summary_path, "r", encoding="utf-8") as f:
        recursive_summary = json.load(f)

    with open(fragility_summary_path, "r", encoding="utf-8") as f:
        fragility_summary = json.load(f)

    with open(memory_summary_path, "r", encoding="utf-8") as f:
        memory_summary = json.load(f)

    stress_map = dict(
        zip(
            route_df["instrument"],
            route_df["geoscen_cot_stress"],
        )
    )

    recursive_pressure = float(
        recursive_summary.get("recursive_escalation_pressure", 0.0) or 0.0
    )

    fragility_score = float(
        fragility_summary.get("systemic_fragility_score", 0.0) or 0.0
    )

    memory_score = float(
        memory_summary.get("memory_score", 0.0) or 0.0
    )

    rows = []

    for link in PROPAGATION_LINKS:
        source = link["source"]
        target = link["target"]

        source_stress = float(stress_map.get(source, 0.0))
        target_stress = float(stress_map.get(target, 0.0))

        weight = float(link["weight"])

        recursive_contagion_pressure = (
            source_stress
            * weight
            * (
                0.45
                + 0.30 * recursive_pressure
                + 0.15 * fragility_score
                + 0.10 * memory_score
            )
        )

        recursive_contagion_pressure = round(
            min(1.0, recursive_contagion_pressure),
            4,
        )

        propagated_target_stress = round(
            min(
                1.0,
                target_stress + recursive_contagion_pressure,
            ),
            4,
        )

        rows.append(
            {
                "source_instrument": source,
                "target_instrument": target,
                "contagion_channel": link["channel"],
                "source_stress": round(source_stress, 4),
                "target_stress": round(target_stress, 4),
                "propagation_weight": weight,
                "recursive_pressure": round(recursive_pressure, 4),
                "fragility_score": round(fragility_score, 4),
                "memory_score": round(memory_score, 4),
                "recursive_contagion_pressure": recursive_contagion_pressure,
                "propagated_target_stress": propagated_target_stress,
                "recursive_contagion_state": classify_contagion_state(
                    propagated_target_stress
                ),
            }
        )

    propagation_df = pd.DataFrame(rows)

    avg_recursive_contagion_pressure = round(
        float(propagation_df["recursive_contagion_pressure"].mean()),
        4,
    )

    max_recursive_contagion_pressure = round(
        float(propagation_df["recursive_contagion_pressure"].max()),
        4,
    )

    avg_propagated_target_stress = round(
        float(propagation_df["propagated_target_stress"].mean()),
        4,
    )

    max_propagated_target_stress = round(
        float(propagation_df["propagated_target_stress"].max()),
        4,
    )

    recursive_contagion_state = classify_contagion_state(
        avg_propagated_target_stress
    )

    summary = {
        "component": "recursive_contagion_propagation_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "link_count": int(len(propagation_df)),
        "avg_recursive_contagion_pressure": avg_recursive_contagion_pressure,
        "max_recursive_contagion_pressure": max_recursive_contagion_pressure,
        "avg_propagated_target_stress": avg_propagated_target_stress,
        "max_propagated_target_stress": max_propagated_target_stress,
        "recursive_contagion_state": recursive_contagion_state,
        "highest_contagion_links": propagation_df.sort_values(
            "recursive_contagion_pressure",
            ascending=False,
        )
        .head(3)[
            [
                "source_instrument",
                "target_instrument",
                "recursive_contagion_pressure",
            ]
        ]
        .to_dict(orient="records"),
        "status": "recursive_contagion_propagation_complete",
    }

    parquet_path = (
        out_dir
        / "recursive_contagion_propagation_engine_v1.parquet"
    )

    json_path = (
        out_dir
        / "recursive_contagion_propagation_engine_v1.json"
    )

    summary_path = (
        out_dir
        / "recursive_contagion_propagation_summary_v1.json"
    )

    propagation_df.to_parquet(
        parquet_path,
        index=False,
    )

    propagation_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive contagion propagation engine complete")
    print("Rows:", len(propagation_df))
    print("Avg Recursive Contagion Pressure:", avg_recursive_contagion_pressure)
    print("Max Recursive Contagion Pressure:", max_recursive_contagion_pressure)
    print("Recursive Contagion State:", recursive_contagion_state)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return propagation_df


if __name__ == "__main__":
    build_recursive_contagion_propagation_engine_v1()
