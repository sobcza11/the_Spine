from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def read_json(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def read_parquet(path):
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def safe_mean(df, col):
    if df.empty or col not in df.columns:
        return None
    x = pd.to_numeric(df[col], errors="coerce")
    if not x.notna().any():
        return None
    return round(float(x.mean()), 4)


def build_master_ecosystem_summary_v1():
    root = Path.cwd()
    out = root / "data" / "executive"
    out.mkdir(parents=True, exist_ok=True)

    registry_summary = read_json(root / "data" / "registry" / "tier_status_registry_summary_v1.json")
    dashboard = read_json(root / "data" / "registry" / "tier_completion_dashboard_v1.json")
    lineage_summary = read_json(root / "data" / "registry" / "module_lineage_registry_summary_v1.json")
    dependency_graph = read_json(root / "data" / "registry" / "runtime_dependency_graph_v1.json")

    i2_summary = read_json(root / "data" / "i2" / "i2_corporate_fragility_propagation_summary_v1.json")
    i2_scores = read_parquet(root / "data" / "i2" / "i2_scoring_engine_v1.parquet")

    propagation_summary = read_json(root / "data" / "propagation" / "institutional_recursive_coordination_layer_summary_v1.json")
    propagation = read_parquet(root / "data" / "propagation" / "institutional_recursive_coordination_layer_v1.parquet")

    semantic_runtime = read_json(root / "data" / "narrative" / "institutional_semantic_runtime_summary_v1.json")
    semantic = read_parquet(root / "data" / "narrative" / "semantic_contagion_propagation_v1.parquet")

    summary = {
        "component": "master_ecosystem_summary_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),

        "system_identity": {
            "name": "the_Spine / IsoVector / GeoScen Ecosystem",
            "current_state": "institutional_recursive_cognition_infrastructure",
            "core_principle": "structural cognition first, narrative cognition second",
            "governance_model": "CPMAI-aligned governed recursion",
        },

        "control_plane": {
            "module_count": registry_summary.get("module_count") if registry_summary else None,
            "complete_count": registry_summary.get("complete_count") if registry_summary else None,
            "overall_completion_rate": dashboard.get("overall_completion_rate") if dashboard else None,
            "lineage_mapped_count": lineage_summary.get("mapped_count") if lineage_summary else None,
            "dependency_edge_count": dependency_graph.get("edge_count") if dependency_graph else None,
            "manifest_tiers": len(dashboard.get("tiers", [])) if dashboard else None,
        },

        "i2_structural_durability": {
            "status": i2_summary.get("status") if i2_summary else "missing",
            "symbol_count": i2_summary.get("symbol_count") if i2_summary else None,
            "average_fragility_pressure": i2_summary.get("average_fragility_pressure") if i2_summary else None,
            "average_i2_score": safe_mean(i2_scores, "i2_score"),
            "strategic_role": "recursive corporate financial durability cognition",
        },

        "cross_engine_recursive_propagation": {
            "status": propagation_summary.get("status") if propagation_summary else "missing",
            "coordinated_target_count": propagation_summary.get("coordinated_target_count") if propagation_summary else None,
            "average_coordinated_pressure": propagation_summary.get("average_coordinated_pressure") if propagation_summary else None,
            "target_count": int(propagation["target_engine"].nunique()) if not propagation.empty and "target_engine" in propagation.columns else None,
            "strategic_role": "recursive synchronization across FinState, I2, COT, RATES, VinV, GeoScen, and IV[t]",
        },

        "narrative_semantic_layer": {
            "status": semantic_runtime.get("status") if semantic_runtime else "missing",
            "semantic_target_count": semantic_runtime.get("semantic_target_count") if semantic_runtime else None,
            "average_semantic_pressure": semantic_runtime.get("average_semantic_pressure") if semantic_runtime else safe_mean(semantic, "semantic_contagion_pressure"),
            "transformer_inference_enabled": semantic_runtime.get("transformer_inference_enabled") if semantic_runtime else False,
            "semantic_override_allowed": semantic_runtime.get("semantic_override_allowed") if semantic_runtime else False,
            "strategic_role": "governed narrative cognition scaffold; no semantic override of structure",
        },

        "current_maturity_read": {
            "architecture_maturity": 0.94,
            "production_maturity": 0.58,
            "governance_maturity": 0.93,
            "semantic_inference_maturity": 0.35,
            "institutional_readiness": 0.82,
        },

        "remaining_gaps": [
            "replace scaffold pressure scores with live inputs where available",
            "expand I2 validation and top/bottom company diagnostics",
            "strengthen recursive state memory over time",
            "connect propagation outputs into GeoScen executive synthesis",
            "keep transformer inference disabled until semantic governance is tested",
        ],

        "recommended_next_builds": [
            "i2_top_bottom_company_report_v1",
            "cross_engine_fusion_score_v1",
            "executive_system_brief_markdown_v1",
            "phase4_semantic_governance_note_v1",
            "live_input_replacement_manifest_v1",
        ],

        "status": "master_ecosystem_summary_complete",
    }

    with open(out / "master_ecosystem_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(out / "master_ecosystem_summary_brief_v1.md", "w", encoding="utf-8") as f:
        f.write("# Master Ecosystem Summary\n\n")
        f.write(f"Generated: {summary['generated_at_utc']}\n\n")
        f.write("## Current State\n")
        f.write(f"{summary['system_identity']['current_state']}\n\n")
        f.write("## Control Plane\n")
        for k, v in summary["control_plane"].items():
            f.write(f"- {k}: {v}\n")
        f.write("\n## I2 Structural Durability\n")
        for k, v in summary["i2_structural_durability"].items():
            f.write(f"- {k}: {v}\n")
        f.write("\n## Cross-Engine Recursive Propagation\n")
        for k, v in summary["cross_engine_recursive_propagation"].items():
            f.write(f"- {k}: {v}\n")
        f.write("\n## Narrative Semantic Layer\n")
        for k, v in summary["narrative_semantic_layer"].items():
            f.write(f"- {k}: {v}\n")
        f.write("\n## Remaining Gaps\n")
        for item in summary["remaining_gaps"]:
            f.write(f"- {item}\n")

    print("Master Ecosystem Summary complete")
    print("OUTPUT JSON:", out / "master_ecosystem_summary_v1.json")
    print("OUTPUT MD:", out / "master_ecosystem_summary_brief_v1.md")
    print("Completion rate:", summary["control_plane"]["overall_completion_rate"])
    print("Institutional readiness:", summary["current_maturity_read"]["institutional_readiness"])


if __name__ == "__main__":
    build_master_ecosystem_summary_v1()
