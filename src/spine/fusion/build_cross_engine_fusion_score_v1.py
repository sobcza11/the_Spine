from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


ENGINE_WEIGHTS = {
    "I2": 0.22,
    "COT": 0.18,
    "RATES": 0.14,
    "VinV": 0.14,
    "GeoScen": 0.18,
    "Narrative": 0.06,
    "Runtime": 0.08,
}


def read_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def read_parquet(path: Path):
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def clamp01(x):
    try:
        if x is None:
            return None
        return max(0.0, min(1.0, float(x)))
    except Exception:
        return None


def classify(x):
    if x >= 0.70: return "systemic_cross_engine_pressure"
    if x >= 0.55: return "fragile_cross_engine_pressure"
    if x >= 0.40: return "elevated_cross_engine_pressure"
    if x >= 0.25: return "watch_cross_engine_pressure"
    return "stable_cross_engine_pressure"


def latest_i2_pressure(root: Path):
    summary = read_json(root / "data" / "i2" / "i2_corporate_fragility_propagation_summary_v1.json")
    if summary:
        return clamp01(summary.get("average_fragility_pressure"))
    return None


def cot_pressure(root: Path):
    candidates = list(root.glob("data/**/*cot*summary_v1.json")) + list(root.glob("data/**/*COT*summary_v1.json"))
    pressures = []

    for fp in candidates:
        s = read_json(fp)
        if not s:
            continue
        for k, v in s.items():
            if k.endswith("_pressure"):
                val = clamp01(v)
                if val is not None:
                    pressures.append(val)

    if pressures:
        return round(float(np.mean(pressures)), 4)

    return None


def rates_pressure(root: Path):
    candidates = list(root.glob("data/**/*rates*summary_v1.json")) + list(root.glob("data/**/*RATES*summary_v1.json"))
    pressures = []

    for fp in candidates:
        s = read_json(fp)
        if not s:
            continue
        for k, v in s.items():
            if k.endswith("_pressure"):
                val = clamp01(v)
                if val is not None:
                    pressures.append(val)

    if pressures:
        return round(float(np.mean(pressures)), 4)

    return None


def vinv_pressure(root: Path):
    candidates = list(root.glob("data/**/*vinv*summary_v1.json")) + list(root.glob("data/**/*debasement*summary_v1.json"))
    pressures = []

    for fp in candidates:
        s = read_json(fp)
        if not s:
            continue
        for k, v in s.items():
            if k.endswith("_pressure"):
                val = clamp01(v)
                if val is not None:
                    pressures.append(val)

    if pressures:
        return round(float(np.mean(pressures)), 4)

    return None


def geoscen_pressure(root: Path):
    candidates = list(root.glob("data/**/*geoscen*summary_v1.json")) + list(root.glob("data/**/*GeoScen*summary_v1.json"))
    pressures = []

    for fp in candidates:
        s = read_json(fp)
        if not s:
            continue
        for k, v in s.items():
            if k.endswith("_pressure"):
                val = clamp01(v)
                if val is not None:
                    pressures.append(val)

    if pressures:
        return round(float(np.mean(pressures)), 4)

    return None


def narrative_pressure(root: Path):
    summary = read_json(root / "data" / "narrative" / "institutional_semantic_runtime_summary_v1.json")
    if summary:
        return clamp01(summary.get("average_semantic_pressure"))
    return None


def runtime_pressure(root: Path):
    summary = read_json(root / "data" / "propagation" / "institutional_recursive_coordination_layer_summary_v1.json")
    if summary:
        return clamp01(summary.get("average_coordinated_pressure"))
    return None


def build_cross_engine_fusion_score_v1():
    root = Path.cwd()
    out = root / "data" / "fusion"
    out.mkdir(parents=True, exist_ok=True)

    raw = {
        "I2": latest_i2_pressure(root),
        "COT": cot_pressure(root),
        "RATES": rates_pressure(root),
        "VinV": vinv_pressure(root),
        "GeoScen": geoscen_pressure(root),
        "Narrative": narrative_pressure(root),
        "Runtime": runtime_pressure(root),
    }

    rows = []
    weighted_sum = 0.0
    active_weight = 0.0

    for engine, pressure in raw.items():
        weight = ENGINE_WEIGHTS[engine]
        available = pressure is not None

        if available:
            weighted_sum += pressure * weight
            active_weight += weight

        rows.append({
            "engine": engine,
            "pressure": pressure,
            "weight": weight,
            "available": available,
            "weighted_pressure": round(pressure * weight, 4) if available else None,
        })

    df = pd.DataFrame(rows)

    fusion_pressure = round(weighted_sum / active_weight, 4) if active_weight else None
    fusion_state = classify(fusion_pressure) if fusion_pressure is not None else "missing_cross_engine_pressure"

    df.to_parquet(out / "cross_engine_fusion_score_v1.parquet", index=False)
    df.to_json(out / "cross_engine_fusion_score_v1.json", orient="records", indent=2)

    summary = {
        "component": "cross_engine_fusion_score_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fusion_pressure": fusion_pressure,
        "fusion_state": fusion_state,
        "active_engine_count": int(df["available"].sum()),
        "engine_count": int(len(df)),
        "active_weight": round(float(active_weight), 4),
        "engine_weights": ENGINE_WEIGHTS,
        "status": "cross_engine_fusion_score_complete",
    }

    with open(out / "cross_engine_fusion_score_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(out / "cross_engine_fusion_score_report_v1.md", "w", encoding="utf-8") as f:
        f.write("# Cross-Engine Fusion Score\n\n")
        f.write(f"Generated: {summary['generated_at_utc']}\n\n")
        f.write(f"- Fusion pressure: {summary['fusion_pressure']}\n")
        f.write(f"- Fusion state: {summary['fusion_state']}\n")
        f.write(f"- Active engines: {summary['active_engine_count']} / {summary['engine_count']}\n")
        f.write(f"- Active weight: {summary['active_weight']}\n\n")
        f.write("## Engine Contributions\n\n")
        f.write(df.to_markdown(index=False))

    print("Cross-Engine Fusion Score complete")
    print("Fusion pressure:", summary["fusion_pressure"])
    print("Fusion state:", summary["fusion_state"])
    print("Active engines:", summary["active_engine_count"], "/", summary["engine_count"])
    print("OUTPUT:", out / "cross_engine_fusion_score_report_v1.md")


if __name__ == "__main__":
    build_cross_engine_fusion_score_v1()
