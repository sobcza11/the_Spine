from pathlib import Path
from datetime import datetime, UTC
import json


def read_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def build_executive_system_brief_v1():
    root = Path.cwd()
    out = root / "data" / "executive"
    out.mkdir(parents=True, exist_ok=True)

    master = read_json(out / "master_ecosystem_summary_v1.json")
    fusion = read_json(root / "data" / "fusion" / "cross_engine_fusion_score_summary_v1.json")
    registry = read_json(root / "data" / "registry" / "tier_status_registry_summary_v1.json")
    i2 = read_json(root / "data" / "i2" / "i2_top_bottom_company_report_summary_v1.json")
    narrative = read_json(root / "data" / "narrative" / "institutional_semantic_runtime_summary_v1.json")
    propagation = read_json(root / "data" / "propagation" / "institutional_recursive_coordination_layer_summary_v1.json")

    brief = []
    brief.append("# Executive System Brief")
    brief.append("")
    brief.append(f"Generated: {datetime.now(UTC).isoformat()}")
    brief.append("")
    brief.append("## Current Strategic Read")
    brief.append("")
    brief.append("the_Spine / IsoVector / GeoScen ecosystem has moved from advanced macro architecture into governed institutional recursive cognition infrastructure.")
    brief.append("")
    brief.append("The system now contains structural macro cognition, I2 financial durability cognition, cross-engine propagation, governed narrative scaffolding, and executive control-plane visibility.")
    brief.append("")
    brief.append("## Control Plane")
    brief.append("")
    brief.append(f"- Total modules: {registry.get('module_count')}")
    brief.append(f"- Complete modules: {registry.get('complete_count')}")
    brief.append(f"- Completion rate: {master.get('control_plane', {}).get('overall_completion_rate')}")
    brief.append(f"- Lineage mapped count: {master.get('control_plane', {}).get('lineage_mapped_count')}")
    brief.append(f"- Dependency edges: {master.get('control_plane', {}).get('dependency_edge_count')}")
    brief.append("")
    brief.append("## Cross-Engine Fusion")
    brief.append("")
    brief.append(f"- Fusion pressure: {fusion.get('fusion_pressure')}")
    brief.append(f"- Fusion state: {fusion.get('fusion_state')}")
    brief.append(f"- Active engines: {fusion.get('active_engine_count')} / {fusion.get('engine_count')}")
    brief.append("")
    brief.append("Interpretation: current cross-engine pressure is in watch mode, not elevated/systemic. This is consistent with a system where structure is active, narrative inference is governed/scaffolded, and several runtime layers remain controlled rather than live-autonomous.")
    brief.append("")
    brief.append("## I2 Structural Durability Layer")
    brief.append("")
    brief.append(f"- Symbol count: {i2.get('symbol_count')}")
    brief.append(f"- Average I2 score: {i2.get('average_i2_score')}")
    brief.append(f"- Average fragility pressure: {i2.get('average_fragility_pressure')}")
    brief.append("")
    brief.append("I2 now functions as the structural corporate durability layer. It anchors recursive escalation in accounting reality: solvency, liquidity, margin quality, cash-flow durability, deterioration pressure, and survivability.")
    brief.append("")
    brief.append("## Cross-Engine Recursive Propagation")
    brief.append("")
    brief.append(f"- Coordinated targets: {propagation.get('coordinated_target_count')}")
    brief.append(f"- Average coordinated pressure: {propagation.get('average_coordinated_pressure')}")
    brief.append("")
    brief.append("This layer moves the ecosystem beyond independent engines. FinState, I2, COT, RATES, VinV, GeoScen, and IV[t] can now begin recursive state coordination.")
    brief.append("")
    brief.append("## Narrative / Semantic Layer")
    brief.append("")
    brief.append(f"- Semantic targets: {narrative.get('semantic_target_count')}")
    brief.append(f"- Average semantic pressure: {narrative.get('average_semantic_pressure')}")
    brief.append(f"- Transformer inference enabled: {narrative.get('transformer_inference_enabled')}")
    brief.append(f"- Semantic override allowed: {narrative.get('semantic_override_allowed')}")
    brief.append("")
    brief.append("Narrative cognition is active as a governed scaffold only. Transformer inference remains disabled, and semantic outputs are not allowed to override structural signals.")
    brief.append("")
    brief.append("## Current Maturity")
    brief.append("")
    maturity = master.get("current_maturity_read", {})
    brief.append(f"- Architecture maturity: {maturity.get('architecture_maturity')}")
    brief.append(f"- Production maturity: {maturity.get('production_maturity')}")
    brief.append(f"- Governance maturity: {maturity.get('governance_maturity')}")
    brief.append(f"- Semantic inference maturity: {maturity.get('semantic_inference_maturity')}")
    brief.append(f"- Institutional readiness: {maturity.get('institutional_readiness')}")
    brief.append("")
    brief.append("## Remaining Priorities")
    brief.append("")
    for item in master.get("remaining_gaps", []):
        brief.append(f"- {item}")
    brief.append("")
    brief.append("## Recommended Next Builds")
    brief.append("")
    brief.append("- Phase 4 Semantic Governance Note")
    brief.append("- Live Input Replacement Manifest")
    brief.append("- GeoScen Executive Synthesis Integration")
    brief.append("- I2 Validation Diagnostics")
    brief.append("- Recursive State Memory History Expansion")
    brief.append("")
    brief.append("## Bottom Line")
    brief.append("")
    brief.append("The ecosystem is now structurally coherent, governance-aligned, and increasingly recursive. The next priority is no longer adding more isolated modules; it is validating outputs, replacing scaffolds with live inputs, and wiring fusion outputs into GeoScen executive synthesis.")

    output = "\n".join(brief)

    md_path = out / "executive_system_brief_v1.md"
    json_path = out / "executive_system_brief_summary_v1.json"

    md_path.write_text(output, encoding="utf-8")

    summary = {
        "component": "executive_system_brief_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "output_markdown": str(md_path.relative_to(root)),
        "fusion_pressure": fusion.get("fusion_pressure"),
        "fusion_state": fusion.get("fusion_state"),
        "completion_rate": master.get("control_plane", {}).get("overall_completion_rate"),
        "institutional_readiness": maturity.get("institutional_readiness"),
        "status": "executive_system_brief_complete",
    }

    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Executive System Brief complete")
    print("OUTPUT:", md_path)
    print("Fusion:", summary["fusion_pressure"], summary["fusion_state"])
    print("Institutional readiness:", summary["institutional_readiness"])


if __name__ == "__main__":
    build_executive_system_brief_v1()
