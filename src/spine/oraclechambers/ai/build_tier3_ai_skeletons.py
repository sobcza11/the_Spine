from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers_ai")


def write_payload(name: str, payload: dict):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{name}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {path}")


def governed_prompt_payload(module: str, prompt_type: str, inputs: list[str], output: str):
    return {
        "system": "OracleChambers",
        "module": module,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "prompt_type": prompt_type,
        "inputs": inputs,
        "output": output,
        "governance": {
            "ai_assisted": True,
            "llm_writeback_allowed": False,
            "measurement_source": "the_Spine",
            "interpretation_layer": "OracleChambers",
            "requires_human_review": True,
        },
    }


def main():
    write_payload(
        "prompt_rbl_synthesis",
        governed_prompt_payload(
            "prompt-engineered-rbl-synthesis",
            "executive_rbl",
            [
                "oc_rbl_local.json",
                "oc_final_metric_local.json",
                "runtime_health.json",
            ],
            "Executive synthesis pending live LLM integration. Deterministic signals remain authoritative.",
        ),
    )

    write_payload(
        "prompt_contradiction_synthesis",
        governed_prompt_payload(
            "prompt-engineered-contradiction-synthesis",
            "cross_asset_contradiction",
            [
                "oc_contradiction_local.json",
                "contradiction_heatmaps.json",
            ],
            "Contradiction synthesis pending live LLM integration. Cross-asset disagreement remains governed.",
        ),
    )

    write_payload(
        "prompt_geoscen_executive_briefs",
        governed_prompt_payload(
            "prompt-engineered-geoscen-executive-briefs",
            "sovereign_executive_brief",
            [
                "sovereign_canonical_layer.json",
                "sovereign_vector_engine.json",
                "regional_transmission_systems.json",
            ],
            "GeoScen executive brief pending live LLM integration. Sovereign vectors remain deterministic.",
        ),
    )

    write_payload(
        "controlled_rag_retrieval",
        {
            "system": "OracleChambers",
            "module": "controlled-rag-retrieval",
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "retrieval_scope": [
                "FOMC minutes",
                "SEP",
                "central bank speeches",
                "macro releases",
                "sovereign policy archives",
            ],
            "retrieval_rules": {
                "read_only": True,
                "source_required": True,
                "citation_required": True,
                "measurement_writeback_allowed": False,
                "ungrounded_answer_allowed": False,
            },
            "governance": {
                "ai_assisted": True,
                "llm_writeback_allowed": False,
                "requires_human_review": True,
            },
        },
    )

    write_payload(
        "narrative_drift_engine",
        {
            "system": "OracleChambers",
            "module": "narrative-drift-engine",
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "tracked_domains": [
                "inflation",
                "growth",
                "liquidity",
                "labor",
                "policy",
                "sovereign stress",
            ],
            "drift_metrics": [
                {"metric": "tone_shift", "status": "skeleton"},
                {"metric": "keyword_pressure", "status": "skeleton"},
                {"metric": "semantic_regime_shift", "status": "skeleton"},
                {"metric": "policy_language_divergence", "status": "skeleton"},
            ],
            "governance": {
                "ai_assisted": True,
                "deterministic_inputs_required": True,
                "llm_writeback_allowed": False,
                "requires_human_review": True,
            },
        },
    )


if __name__ == "__main__":
    main()
