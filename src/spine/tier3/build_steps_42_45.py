from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier3"
)


def write_payload(name: str, payload: dict):

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    path = OUT_DIR / f"{name}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {path}")


def governed_payload(
    module: str,
    capability: str,
    status: str,
    signals: list[dict],
):

    return {
        "system": "OracleChambers",
        "module": module,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "capability": capability,
        "status": status,
        "signals": signals,

        "governance": {
            "ai_assisted": True,
            "llm_writeback_allowed": False,
            "measurement_source": "the_Spine",
            "interpretation_layer": "OracleChambers",
            "requires_human_review": True,
        },
    }


def main():

    # 42 ? Fedspeak cognition
    write_payload(
        "fedspeak_cognition",
        governed_payload(
            "fedspeak-cognition",
            "central_bank_semantic_interpretation",
            "operational_skeleton",
            [
                {
                    "signal": "policy_tone",
                    "state": "restrictive_watch",
                    "score": 72,
                },
                {
                    "signal": "inflation_language",
                    "state": "persistent",
                    "score": 69,
                },
                {
                    "signal": "labor_language",
                    "state": "cooling_watch",
                    "score": 61,
                },
            ],
        ),
    )

    # 43 ? Earnings cognition
    write_payload(
        "earnings_cognition",
        governed_payload(
            "earnings-cognition",
            "executive_tone_diagnostics",
            "operational_skeleton",
            [
                {
                    "signal": "margin_pressure",
                    "state": "watch",
                    "score": 63,
                },
                {
                    "signal": "forward_guidance",
                    "state": "cautious",
                    "score": 66,
                },
                {
                    "signal": "demand_language",
                    "state": "mixed",
                    "score": 59,
                },
            ],
        ),
    )

    # 44 ? PMI semantic linkage
    write_payload(
        "pmi_semantic_linkage",
        governed_payload(
            "pmi-semantic-linkage",
            "narrative_economic_linkage",
            "operational_skeleton",
            [
                {
                    "signal": "new_orders_language",
                    "state": "mixed",
                    "score": 64,
                },
                {
                    "signal": "supplier_delivery_language",
                    "state": "normalizing",
                    "score": 58,
                },
                {
                    "signal": "prices_language",
                    "state": "watch",
                    "score": 67,
                },
            ],
        ),
    )

    # 45 ? Controlled RAG retrieval
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
                "earnings transcripts",
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


if __name__ == "__main__":
    main()
