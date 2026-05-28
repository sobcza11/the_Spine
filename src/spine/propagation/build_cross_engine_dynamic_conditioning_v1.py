from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


CONDITIONING_RULES = [
    {
        "source": "i2",
        "target": "geoscen",
        "effect": "financial_durability_conditions_escalation",
        "weight": 0.25,
    },
    {
        "source": "fusion",
        "target": "geoscen",
        "effect": "cross_engine_fusion_conditions_escalation",
        "weight": 0.30,
    },
    {
        "source": "propagation",
        "target": "fusion",
        "effect": "recursive_coordination_conditions_fusion",
        "weight": 0.20,
    },
    {
        "source": "narrative",
        "target": "geoscen",
        "effect": "semantic_context_conditions_interpretation_only",
        "weight": 0.05,
    },
    {
        "source": "geoscen",
        "target": "executive",
        "effect": "geoscen_state_conditions_executive_read",
        "weight": 0.20,
    },
]


def classify(x):
    if x >= 0.70:
        return "systemic_dynamic_conditioning"
    if x >= 0.55:
        return "fragile_dynamic_conditioning"
    if x >= 0.40:
        return "elevated_dynamic_conditioning"
    if x >= 0.25:
        return "watch_dynamic_conditioning"
    return "stable_dynamic_conditioning"


def build_cross_engine_dynamic_conditioning_v1():
    root = Path.cwd()
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    memory_path = out / "persistent_recursive_state_trends_v1.parquet"

    if not memory_path.exists():
        raise FileNotFoundError(f"Missing persistent memory trends: {memory_path}")

    memory = pd.read_parquet(memory_path).copy()

    memory["latest_pressure"] = pd.to_numeric(memory["latest_pressure"], errors="coerce").fillna(0)
    memory["pressure_delta"] = pd.to_numeric(memory["pressure_delta"], errors="coerce").fillna(0)

    rows = []

    for rule in CONDITIONING_RULES:
        src = rule["source"]
        target = rule["target"]

        source_row = memory[memory["engine"] == src]

        if source_row.empty:
            source_pressure = 0.0
            source_delta = 0.0
            source_state = "missing_source_state"
            memory_depth = 0
            available = False
        else:
            r = source_row.iloc[0]
            source_pressure = float(r["latest_pressure"])
            source_delta = float(r["pressure_delta"])
            source_state = str(r["latest_state"])
            memory_depth = int(r["memory_depth"])
            available = True

        trend_boost = max(0.0, source_delta) * 0.25
        persistence_boost = min(memory_depth / 10, 1.0) * 0.10

        conditioned_pressure = round(
            min(
                1.0,
                (source_pressure * rule["weight"]) + trend_boost + persistence_boost
            ),
            4
        )

        rows.append({
            "source_engine": src,
            "target_engine": target,
            "effect": rule["effect"],
            "source_pressure": round(source_pressure, 4),
            "source_delta": round(source_delta, 4),
            "source_state": source_state,
            "memory_depth": memory_depth,
            "rule_weight": rule["weight"],
            "trend_boost": round(trend_boost, 4),
            "persistence_boost": round(persistence_boost, 4),
            "conditioned_pressure": conditioned_pressure,
            "conditioning_state": classify(conditioned_pressure),
            "available": available,
        })

    conditioning = pd.DataFrame(rows)

    target_summary = (
        conditioning.groupby("target_engine", as_index=False)
        .agg(
            dynamic_conditioned_pressure=("conditioned_pressure", "sum"),
            source_count=("source_engine", "count"),
            active_source_count=("available", "sum"),
            source_engines=("source_engine", lambda x: sorted(set(x))),
        )
    )

    target_summary["dynamic_conditioned_pressure"] = (
        target_summary["dynamic_conditioned_pressure"]
        .clip(upper=1.0)
        .round(4)
    )

    target_summary["dynamic_conditioning_state"] = target_summary[
        "dynamic_conditioned_pressure"
    ].apply(classify)

    summary = {
        "component": "cross_engine_dynamic_conditioning_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "conditioning_edges": int(len(conditioning)),
        "target_count": int(target_summary["target_engine"].nunique()),
        "average_dynamic_conditioned_pressure": round(
            float(target_summary["dynamic_conditioned_pressure"].mean()),
            4
        ),
        "status": "cross_engine_dynamic_conditioning_complete",
    }

    conditioning.to_parquet(
        out / "cross_engine_dynamic_conditioning_edges_v1.parquet",
        index=False
    )

    conditioning.to_json(
        out / "cross_engine_dynamic_conditioning_edges_v1.json",
        orient="records",
        indent=2
    )

    target_summary.to_parquet(
        out / "cross_engine_dynamic_conditioning_targets_v1.parquet",
        index=False
    )

    target_summary.to_json(
        out / "cross_engine_dynamic_conditioning_targets_v1.json",
        orient="records",
        indent=2
    )

    with open(
        out / "cross_engine_dynamic_conditioning_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    md = []
    md.append("# Cross-Engine Dynamic Conditioning")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append("## Summary")
    md.append("")
    for k, v in summary.items():
        if k not in ["component", "status"]:
            md.append(f"- {k}: {v}")
    md.append("")
    md.append("## Conditioning Edges")
    md.append("")
    md.append(conditioning.to_markdown(index=False))
    md.append("")
    md.append("## Target Summary")
    md.append("")
    md.append(target_summary.to_markdown(index=False))
    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append(
        "Engines now condition downstream recursive targets using current pressure, pressure deltas, and recursive memory depth."
    )

    md_path = out / "cross_engine_dynamic_conditioning_v1.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    print("Cross-Engine Dynamic Conditioning complete")
    print("Edges:", summary["conditioning_edges"])
    print("Targets:", summary["target_count"])
    print("Average pressure:", summary["average_dynamic_conditioned_pressure"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_cross_engine_dynamic_conditioning_v1()
