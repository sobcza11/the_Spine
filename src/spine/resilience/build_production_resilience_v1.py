from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


REQUIRED_OUTPUTS = [
    "data/registry/tier_status_registry_summary_v1.json",
    "data/fusion/cross_engine_fusion_score_summary_v1.json",
    "data/geoscen/geoscen_executive_synthesis_integration_v1.json",
    "data/executive/executive_system_brief_v1.md",
    "data/executive/executive_monitoring_layer_v1.json",
    "data/propagation/persistent_recursive_state_memory_summary_v1.json",
    "data/propagation/cross_engine_dynamic_conditioning_summary_v1.json",
    "data/narrative/phase4_semantic_governance_note_v1.json",
    "data/semantic_validation/semantic_transformer_validation_summary_v1.json",
    "data/governance/live_input_replacement_manifest_summary_v1.json",
]


def file_status(root: Path, rel_path: str):
    fp = root / rel_path
    exists = fp.exists()

    return {
        "path": rel_path,
        "exists": exists,
        "size_bytes": fp.stat().st_size if exists else 0,
        "modified_utc": datetime.fromtimestamp(
            fp.stat().st_mtime,
            UTC
        ).isoformat() if exists else None,
    }


def read_json_safe(path: Path):
    try:
        if not path.exists():
            return None, "missing"
        return json.loads(path.read_text(encoding="utf-8")), "ok"
    except Exception as e:
        return None, f"read_error: {e}"


def build_production_resilience_v1():
    root = Path.cwd()
    out = root / "data" / "resilience"
    out.mkdir(parents=True, exist_ok=True)

    checks = []

    for rel in REQUIRED_OUTPUTS:
        checks.append(file_status(root, rel))

    checks_df = pd.DataFrame(checks)

    required_exists_rate = round(float(checks_df["exists"].mean()), 4)

    json_checks = []

    for rel in REQUIRED_OUTPUTS:
        if not rel.endswith(".json"):
            continue

        payload, status = read_json_safe(root / rel)

        json_checks.append({
            "path": rel,
            "json_status": status,
            "component": payload.get("component") if isinstance(payload, dict) else None,
            "status": payload.get("status") if isinstance(payload, dict) else None,
        })

    json_df = pd.DataFrame(json_checks)

    missing_count = int((checks_df["exists"] == False).sum())
    json_error_count = int((json_df["json_status"] != "ok").sum()) if not json_df.empty else 0

    resilience_state = (
        "production_resilience_ready"
        if missing_count == 0 and json_error_count == 0
        else "production_resilience_review_required"
    )

    summary = {
        "component": "production_resilience_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "required_output_count": int(len(checks_df)),
        "required_exists_rate": required_exists_rate,
        "missing_output_count": missing_count,
        "json_error_count": json_error_count,
        "resilience_state": resilience_state,
        "failover_status": "scaffold_registered",
        "recovery_status": "manual_recovery_ready",
        "audit_status": "required_outputs_checked",
        "status": "production_resilience_complete",
    }

    checks_df.to_parquet(out / "production_resilience_file_checks_v1.parquet", index=False)
    checks_df.to_json(out / "production_resilience_file_checks_v1.json", orient="records", indent=2)

    json_df.to_parquet(out / "production_resilience_json_checks_v1.parquet", index=False)
    json_df.to_json(out / "production_resilience_json_checks_v1.json", orient="records", indent=2)

    with open(out / "production_resilience_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    md = []
    md.append("# Production Resilience")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append("## Summary")
    md.append("")
    for k, v in summary.items():
        if k not in ["component", "status"]:
            md.append(f"- {k}: {v}")

    md.append("")
    md.append("## Required Output Checks")
    md.append("")
    md.append(checks_df.to_markdown(index=False))

    md.append("")
    md.append("## JSON Integrity Checks")
    md.append("")
    md.append(json_df.to_markdown(index=False))

    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append(
        "Production resilience is now tracked through required output checks, JSON integrity checks, manual recovery readiness, and audit visibility."
    )

    md_path = out / "production_resilience_v1.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    print("Production Resilience complete")
    print("Exists rate:", summary["required_exists_rate"])
    print("Missing:", summary["missing_output_count"])
    print("JSON errors:", summary["json_error_count"])
    print("State:", summary["resilience_state"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_production_resilience_v1()
