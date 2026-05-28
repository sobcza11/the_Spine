from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "full_platform_dependency_graph.json"


DEPENDENCY_GRAPH = [
    ["tier7_core", "phase3_intelligence_realization"],
    ["phase3_intelligence_realization", "phase4_truth_calibration"],
    ["phase4_truth_calibration", "phase5_autonomous_research"],
    ["phase5_autonomous_research", "phase6_cognitive_sovereignty"],
    ["phase6_cognitive_sovereignty", "constitutional_proof_layer"],
    ["constitutional_proof_layer", "release_candidate_control_plane"],
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "full-platform-dependency-graph",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "full_platform_dependency_graph_enabled": True,

        "dependency_graph": DEPENDENCY_GRAPH,
        "dependency_edge_count": len(DEPENDENCY_GRAPH),

        "graph_objective": (
            "Prove how the full platform connects from Tier 7 institutional cognition "
            "through intelligence realization, truth calibration, autonomous research, "
            "cognitive sovereignty, constitutional proof, and release-candidate control."
        ),

        "graph_contract": {
            "tier_to_phase_dependency_required": True,
            "truth_to_research_dependency_required": True,
            "sovereignty_to_constitution_dependency_required": True,
            "release_candidate_dependency_required": True,
            "human_review_required": True,
        },

        "governance": {
            "platform_dependency_graph_governed": True,
            "hidden_dependencies_forbidden": True,
            "dependency_visibility_required": True,
            "llm_writeback_allowed": False,
            "audit_trail_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
