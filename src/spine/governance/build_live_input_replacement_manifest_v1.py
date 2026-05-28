from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


LIVE_INPUT_RULES = {
    "i2": {
        "replacement_priority": "high",
        "target_live_inputs": [
            "updated SimFin statements",
            "quarterly financial filings",
            "statement normalizer refresh",
            "company-level deterioration deltas",
        ],
    },
    "cot": {
        "replacement_priority": "high",
        "target_live_inputs": [
            "weekly CFTC COT files",
            "positioning acceleration",
            "crowding percentile",
            "unwind probability",
        ],
    },
    "rates": {
        "replacement_priority": "high",
        "target_live_inputs": [
            "FRED rates",
            "Treasury curve",
            "policy-rate pressure",
            "China sovereign proxy",
        ],
    },
    "vinv": {
        "replacement_priority": "medium",
        "target_live_inputs": [
            "valuation panels",
            "inflation linkage",
            "commodity sensitivity",
            "debasement pressure",
        ],
    },
    "geoscen": {
        "replacement_priority": "medium",
        "target_live_inputs": [
            "cross-engine propagation outputs",
            "GeoScen routing registry",
            "IV[t] transition state",
            "escalation synchronization",
        ],
    },
    "narrative": {
        "replacement_priority": "low",
        "target_live_inputs": [
            "ISM commentary feed",
            "semantic pressure backtest",
            "validated narrative-state memory",
            "governed transformer inference only after approval",
        ],
    },
    "runtime": {
        "replacement_priority": "medium",
        "target_live_inputs": [
            "scheduler state",
            "runtime health",
            "monitoring events",
            "audit logs",
            "operator control state",
        ],
    },
    "adaptive": {
        "replacement_priority": "medium",
        "target_live_inputs": [
            "streaming state updates",
            "latency metrics",
            "cloud runtime health",
            "websocket events",
            "live deployment status",
        ],
    },
}


def read_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def infer_domain(component: str, source_file: str):
    text = f"{component} {source_file}".lower()
    for key in LIVE_INPUT_RULES:
        if key in text:
            return key
    return "unclassified"


def infer_replacement_status(domain: str, source_file: str, component: str):
    text = f"{domain} {source_file} {component}".lower()

    if domain == "narrative":
        if "semantic" in text or "transformer" in text:
            return "scaffold_governed_do_not_activate_without_review"

    if any(x in text for x in ["tier4", "adaptive", "runtime", "scaffold", "placeholder"]):
        return "needs_live_input_replacement"

    if any(x in text for x in ["i2", "cot", "rates", "vinv", "geoscen"]):
        return "partially_live_or_structural_input_available"

    return "review_required"


def build_live_input_replacement_manifest_v1():
    root = Path.cwd()
    out = root / "data" / "governance"
    out.mkdir(parents=True, exist_ok=True)

    registry_path = root / "data" / "registry" / "tier_status_registry_v1.parquet"

    if not registry_path.exists():
        raise FileNotFoundError(f"Missing registry: {registry_path}")

    registry = pd.read_parquet(registry_path).copy()

    rows = []

    for _, r in registry.iterrows():
        component = str(r.get("component", ""))
        source_file = str(r.get("source_file", ""))
        tier = str(r.get("tier", ""))
        state = str(r.get("state", ""))
        pressure = r.get("pressure", None)

        domain = infer_domain(component, source_file)
        rule = LIVE_INPUT_RULES.get(domain, {
            "replacement_priority": "review",
            "target_live_inputs": ["manual review required"],
        })

        rows.append({
            "component": component,
            "tier": tier,
            "domain": domain,
            "source_file": source_file,
            "current_state": state,
            "current_pressure": pressure,
            "replacement_status": infer_replacement_status(domain, source_file, component),
            "replacement_priority": rule["replacement_priority"],
            "target_live_inputs": rule["target_live_inputs"],
            "governance_note": "structural outputs remain source of truth; live inputs must be validated before replacing scaffold pressure",
        })

    manifest = pd.DataFrame(rows)

    priority_order = {
        "high": 1,
        "medium": 2,
        "low": 3,
        "review": 4,
    }

    manifest["priority_rank"] = manifest["replacement_priority"].map(priority_order).fillna(9).astype(int)

    manifest = manifest.sort_values(
        ["priority_rank", "domain", "component"]
    ).reset_index(drop=True)

    manifest.to_parquet(out / "live_input_replacement_manifest_v1.parquet", index=False)
    manifest.to_json(out / "live_input_replacement_manifest_v1.json", orient="records", indent=2)

    summary = {
        "component": "live_input_replacement_manifest_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "module_count": int(len(manifest)),
        "high_priority_count": int((manifest["replacement_priority"] == "high").sum()),
        "medium_priority_count": int((manifest["replacement_priority"] == "medium").sum()),
        "low_priority_count": int((manifest["replacement_priority"] == "low").sum()),
        "review_count": int((manifest["replacement_priority"] == "review").sum()),
        "needs_live_input_replacement_count": int((manifest["replacement_status"] == "needs_live_input_replacement").sum()),
        "governance_position": "live inputs may replace scaffold pressure only after validation and lineage registration",
        "status": "live_input_replacement_manifest_complete",
    }

    with open(out / "live_input_replacement_manifest_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    md_path = out / "live_input_replacement_manifest_v1.md"

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Live Input Replacement Manifest\n\n")
        f.write(f"Generated: {summary['generated_at_utc']}\n\n")
        f.write("## Summary\n\n")
        for k, v in summary.items():
            if k not in ["component", "status"]:
                f.write(f"- {k}: {v}\n")

        f.write("\n## Replacement Priorities\n\n")
        cols = [
            "component",
            "tier",
            "domain",
            "replacement_priority",
            "replacement_status",
            "current_pressure",
            "current_state",
        ]

        f.write(manifest[cols].to_markdown(index=False))

        f.write("\n\n## Governance Position\n\n")
        f.write("Structural outputs remain the source of truth. Live inputs may replace scaffold pressure only after validation, lineage registration, and governance review.\n")

    print("Live Input Replacement Manifest complete")
    print("Modules:", summary["module_count"])
    print("High priority:", summary["high_priority_count"])
    print("Needs replacement:", summary["needs_live_input_replacement_count"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_live_input_replacement_manifest_v1()
