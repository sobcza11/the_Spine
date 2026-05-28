from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "constitutional_registry.json"


CONSTITUTIONAL_SYSTEMS = [
    "evidence_admissibility_doctrine",
    "truth_hierarchy_engine",
    "claim_burden_of_proof_layer",
    "source_integrity_constitution",
    "evidence_decay_engine",
    "institutional_belief_registry",
    "belief_revision_protocol",
    "uncertainty_preservation_layer",
    "confidence_legitimacy_audit",
    "contradiction_preservation_doctrine",
    "advisory_only_enforcement_layer",
    "human_authority_boundary_map",
    "escalation_authority_protocol",
    "deployment_veto_system",
    "institutional_accountability_ledger",
    "failure_admission_protocol",
    "overconfidence_suppression_engine",
    "model_humility_layer",
    "post_mortem_doctrine_engine",
    "fragility_disclosure_layer",
    "narrative_capture_defense_system",
    "political_pressure_firewall",
    "leadership_transition_continuity_engine",
    "doctrine_drift_detection_system",
    "institutional_memory_preservation_layer",
    "operator_trust_calibration",
    "executive_comprehension_audit",
    "decision_usefulness_score",
    "cognitive_burden_monitor",
    "trust_reset_protocol",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    registry_hash = hashlib.sha256(
        json.dumps(CONSTITUTIONAL_SYSTEMS, sort_keys=True).encode("utf-8")
    ).hexdigest()

    payload = {
        "system": "IsoVector",
        "module": "constitutional-registry",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "constitutional_registry_enabled": True,

        "constitutional_systems": CONSTITUTIONAL_SYSTEMS,
        "constitutional_system_count": len(CONSTITUTIONAL_SYSTEMS),
        "constitutional_registry_hash": registry_hash,

        "registry_objective": (
            "Maintain canonical registration of all constitutional governance, truth, "
            "accountability, trust, memory, and doctrine systems."
        ),

        "registry_contract": {
            "canonical_registry_required": True,
            "registry_hash_required": True,
            "system_visibility_required": True,
            "registry_audit_required": True,
            "human_review_required": True,
        },

        "governance": {
            "constitutional_registry_governed": True,
            "unregistered_constitutional_systems_forbidden": True,
            "constitutional_registry_immutable": True,
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
