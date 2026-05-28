from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


TIER_MAP = {
    "tier1": "Tier 1",
    "tier2": "Tier 2",
    "tier3": "Tier 3",
    "tier4": "Tier 4",
}


def infer_tier(path: Path) -> str:
    p = str(path).lower()
    for key, label in TIER_MAP.items():
        if key in p:
            return label
    if "recursive" in p:
        return "Tier 1"
    return "Unclassified"


def infer_liveness(path: Path, summary: dict) -> str:
    text = " ".join([
        str(path).lower(),
        str(summary).lower(),
    ])

    live_terms = [
        "live",
        "streaming",
        "runtime",
        "weekly",
        "websocket",
        "scheduler",
        "monitoring",
        "api_gateway",
    ]

    if any(t in text for t in live_terms):
        return "live_or_runtime_scaffold"
    return "structural_scaffold"


def build_tier_status_registry_v1():
    root = Path.cwd()
    out = root / "data" / "registry"
    out.mkdir(parents=True, exist_ok=True)

    rows = []

    for fp in root.glob("data/**/*summary_v1.json"):
        try:
            summary = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            rows.append({
                "file": str(fp),
                "tier": infer_tier(fp),
                "component": fp.stem,
                "status": "read_error",
                "error": str(e),
                "complete": False,
            })
            continue

        component = summary.get("component", fp.stem)
        status = summary.get("status", "unknown")
        generated_at = summary.get("generated_at_utc")

        pressure_keys = [k for k in summary.keys() if k.endswith("_pressure")]
        state_keys = [k for k in summary.keys() if k.endswith("_state")]

        pressure = summary.get(pressure_keys[0]) if pressure_keys else None
        state = summary.get(state_keys[0]) if state_keys else None

        rows.append({
            "tier": infer_tier(fp),
            "component": component,
            "status": status,
            "complete": str(status).endswith("_complete"),
            "pressure": pressure,
            "state": state,
            "generated_at_utc": generated_at,
            "liveness_class": infer_liveness(fp, summary),
            "source_file": str(fp.relative_to(root)),
        })

    df = pd.DataFrame(rows)

    if not df.empty:

        # Normalize parquet-compatible typing
        object_cols = [
            "tier",
            "component",
            "status",
            "state",
            "generated_at_utc",
            "liveness_class",
            "source_file",
        ]

        for col in object_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        if "pressure" in df.columns:
            df["pressure"] = pd.to_numeric(df["pressure"], errors="coerce")

        if "complete" in df.columns:
            df["complete"] = df["complete"].fillna(False).astype(bool)

        df = df.sort_values(["tier", "component"]).reset_index(drop=True)

    summary = {
        "component": "tier_status_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "module_count": int(len(df)),
        "complete_count": int(df["complete"].sum()) if not df.empty and "complete" in df else 0,
        "tier_counts": df["tier"].value_counts().to_dict() if not df.empty else {},
        "liveness_counts": df["liveness_class"].value_counts().to_dict() if not df.empty and "liveness_class" in df else {},
        "status": "tier_status_registry_complete",
    }

    df.to_parquet(out / "tier_status_registry_v1.parquet", index=False)
    df.to_json(out / "tier_status_registry_v1.json", orient="records", indent=2)

    with open(out / "tier_status_registry_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Tier Status Registry complete")
    print("Modules:", summary["module_count"])
    print("Complete:", summary["complete_count"])
    print("SUMMARY:", out / "tier_status_registry_summary_v1.json")


if __name__ == "__main__":
    build_tier_status_registry_v1()
