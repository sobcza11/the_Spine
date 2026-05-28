from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


MEMORY_SOURCES = [
    {
        "engine": "fusion",
        "path": "data/fusion/cross_engine_fusion_score_summary_v1.json",
        "pressure_field": "fusion_pressure",
        "state_field": "fusion_state",
    },
    {
        "engine": "geoscen",
        "path": "data/geoscen/geoscen_executive_synthesis_integration_v1.json",
        "pressure_field": "geoscen_executive_state.geoscen_pressure",
        "state_field": "geoscen_executive_state.geoscen_state",
    },
    {
        "engine": "propagation",
        "path": "data/propagation/institutional_recursive_coordination_layer_summary_v1.json",
        "pressure_field": "average_coordinated_pressure",
        "state_field": "coordination_state",
    },
    {
        "engine": "narrative",
        "path": "data/narrative/institutional_semantic_runtime_summary_v1.json",
        "pressure_field": "average_semantic_pressure",
        "state_field": "runtime_status",
    },
    {
        "engine": "i2",
        "path": "data/i2/i2_top_bottom_company_report_summary_v1.json",
        "pressure_field": "average_fragility_pressure",
        "state_field": "status",
    },
]


def read_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def nested_get(d, key):
    try:
        current = d
        for k in key.split("."):
            current = current[k]
        return current
    except Exception:
        return None


def classify_trend(delta):
    if delta is None:
        return "unknown"

    if delta >= 0.10:
        return "rapidly_increasing"

    if delta >= 0.03:
        return "increasing"

    if delta <= -0.10:
        return "rapidly_decreasing"

    if delta <= -0.03:
        return "decreasing"

    return "stable"


def build_persistent_recursive_state_memory_v1():
    root = Path.cwd()
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    memory_path = out / "persistent_recursive_state_memory_v1.parquet"

    existing_memory = (
        pd.read_parquet(memory_path)
        if memory_path.exists()
        else pd.DataFrame()
    )

    new_rows = []

    timestamp = datetime.now(UTC).isoformat()

    for source in MEMORY_SOURCES:

        fp = root / source["path"]

        payload = read_json(fp)

        pressure = nested_get(payload, source["pressure_field"])
        state = nested_get(payload, source["state_field"])

        try:
            pressure = float(pressure) if pressure is not None else None
        except Exception:
            pressure = None

        new_rows.append({
            "timestamp_utc": timestamp,
            "engine": source["engine"],
            "pressure": pressure,
            "state": state,
        })

    current_df = pd.DataFrame(new_rows)

    # =====================================================
    # APPEND TO MEMORY
    # =====================================================

    if not existing_memory.empty:
        memory = pd.concat(
            [existing_memory, current_df],
            ignore_index=True
        )
    else:
        memory = current_df.copy()

    memory["pressure"] = pd.to_numeric(
        memory["pressure"],
        errors="coerce"
    )

    memory = memory.sort_values(
        ["engine", "timestamp_utc"]
    ).reset_index(drop=True)

    # =====================================================
    # RECURSIVE TREND ANALYSIS
    # =====================================================

    trend_rows = []

    for engine, grp in memory.groupby("engine"):

        grp = grp.sort_values("timestamp_utc")

        latest = grp.tail(1)
        previous = grp.tail(2)

        latest_pressure = latest["pressure"].iloc[0]

        if len(previous) >= 2:
            prev_pressure = previous["pressure"].iloc[0]
            delta = latest_pressure - prev_pressure
        else:
            prev_pressure = None
            delta = None

        trend_rows.append({
            "engine": engine,
            "latest_pressure": latest_pressure,
            "previous_pressure": prev_pressure,
            "pressure_delta": round(float(delta), 4) if delta is not None else None,
            "trend_state": classify_trend(delta),
            "memory_depth": int(len(grp)),
            "latest_state": latest["state"].iloc[0],
        })

    trends = pd.DataFrame(trend_rows)

    # =====================================================
    # RECURSIVE CONTINUITY SCORE
    # =====================================================

    continuity_score = round(
        float(
            trends["memory_depth"].mean() /
            max(len(MEMORY_SOURCES), 1)
        ),
        4
    )

    # =====================================================
    # SAVE
    # =====================================================

    memory.to_parquet(memory_path, index=False)

    trends.to_parquet(
        out / "persistent_recursive_state_trends_v1.parquet",
        index=False
    )

    trends.to_json(
        out / "persistent_recursive_state_trends_v1.json",
        orient="records",
        indent=2
    )

    summary = {
        "component": "persistent_recursive_state_memory_v1",
        "generated_at_utc": timestamp,
        "engine_count": int(trends["engine"].nunique()),
        "memory_rows": int(len(memory)),
        "average_memory_depth": round(float(trends["memory_depth"].mean()), 4),
        "continuity_score": continuity_score,
        "increasing_engines": int(
            trends["trend_state"].isin([
                "increasing",
                "rapidly_increasing"
            ]).sum()
        ),
        "decreasing_engines": int(
            trends["trend_state"].isin([
                "decreasing",
                "rapidly_decreasing"
            ]).sum()
        ),
        "stable_engines": int(
            (trends["trend_state"] == "stable").sum()
        ),
        "status": "persistent_recursive_state_memory_complete",
    }

    with open(
        out / "persistent_recursive_state_memory_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    # =====================================================
    # MARKDOWN REPORT
    # =====================================================

    md = []

    md.append("# Persistent Recursive State Memory")
    md.append("")
    md.append(f"Generated: {timestamp}")
    md.append("")
    md.append("## Summary")
    md.append("")

    for k, v in summary.items():
        if k not in ["component", "status"]:
            md.append(f"- {k}: {v}")

    md.append("")
    md.append("## Recursive Trend States")
    md.append("")
    md.append(
        trends[[
            "engine",
            "latest_pressure",
            "previous_pressure",
            "pressure_delta",
            "trend_state",
            "memory_depth",
            "latest_state",
        ]].to_markdown(index=False)
    )

    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append(
        "GeoScen now maintains persistent recursive pressure continuity across engines rather than evaluating only isolated current-state conditions."
    )

    md_path = out / "persistent_recursive_state_memory_v1.md"

    md_path.write_text(
        "\n".join(md),
        encoding="utf-8"
    )

    print("Persistent Recursive State Memory complete")
    print("Memory rows:", summary["memory_rows"])
    print("Continuity score:", summary["continuity_score"])
    print("Stable engines:", summary["stable_engines"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_persistent_recursive_state_memory_v1()
