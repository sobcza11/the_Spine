from pathlib import Path
from datetime import datetime, timezone
import json
import os


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

CONTEXT_PATH = ROOT / "rbl_agent" / "rbl_grounded_context_bundle.json"

OUT_PATH = ROOT / "rbl_agent" / "langroid_rbl_agent_output.json"

CACHE_PATH = ROOT / "rbl_agent" / "last_good_rbl_agent_output.json"


def load_json(path: Path) -> dict:

    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: dict):

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def build_prompt(context: dict) -> str:

    safe_context = json.dumps(context, indent=2)[:12000]

    return f"""
You are the OracleChambers RBL Agent.

Your task:
Produce an executive Read-Between-the-Lines synthesis.

Rules:
- Read-only interpretation only
- No score invention
- No writeback
- No certainty claims
- Highlight contradictions
- Highlight confidence limits
- Institutional tone
- JSON only

Return JSON with:
headline
synthesis
executive_attention
limitations

Governed context:
{safe_context}
"""


def fallback_synthesis(context: dict) -> dict:

    inputs = context.get("inputs", {})

    final_metric = inputs.get("final_metric", {})
    contradiction = inputs.get("contradiction", {})
    attention = inputs.get("attention", {})

    final_score = (
        final_metric.get("scores", {})
        .get("final_score", "n/a")
    )

    max_severity = contradiction.get(
        "max_severity",
        "n/a",
    )

    top_priority = attention.get(
        "top_priority",
        {},
    )

    return {
        "headline":
            "Fallback RBL synthesis active.",

        "synthesis": [
            f"System confidence remains conditional with a final score of {final_score}.",
            f"Contradiction severity remains elevated at {max_severity}.",
            "Cross-asset confirmation is incomplete.",
        ],

        "executive_attention": [
            f"Top priority: {top_priority.get('area', 'unknown')}",
            f"Final deployability score: {final_score}",
        ],

        "limitations": [
            "Fallback rule synthesis used.",
            "Live LLM synthesis unavailable.",
        ],
    }


def call_llm(prompt: str) -> dict:

    try:

        import ollama

        response = ollama.chat(
            model="llama3.1:8b",

            messages=[
                {
                    "role": "system",

                    "content":
                        (
                            "You are a governed institutional "
                            "macro cognition agent.\n"
                            "Return ONLY valid JSON.\n"
                            "No markdown.\n"
                            "No code fences.\n"
                            "No explanation.\n"
                            "Required keys:\n"
                            "headline\n"
                            "synthesis\n"
                            "executive_attention\n"
                            "limitations"
                        ),
                },

                {
                    "role": "user",
                    "content": prompt,
                },
            ],
        )

        text = response["message"]["content"]

        # =========================================
        # HARD JSON EXTRACTION
        # =========================================

        start = text.find("{")
        end = text.rfind("}")

        if start == -1 or end == -1:

            raise RuntimeError(
                f"No JSON found in Ollama response: {text}"
            )

        json_text = text[start:end + 1]

        parsed = json.loads(json_text)

        parsed["_live_success"] = True

        return parsed

    except Exception as exc:

        return {
            "_live_success": False,
            "_fallback_reason": str(exc),
        }


def build_output(
    result: dict,
    mode: str,
    fallback_reason=None,
):

    return {
        "system": "OracleChambers",

        "module":
            "langroid-rbl-agent-output",

        "generated_utc":
            datetime.now(timezone.utc).isoformat(),

        "agent_mode": mode,

        "headline":
            result.get("headline", ""),

        "synthesis":
            result.get("synthesis", []),

        "executive_attention":
            result.get(
                "executive_attention",
                [],
            ),

        "limitations":
            result.get(
                "limitations",
                [],
            ),

        "fallback_reason":
            fallback_reason,

        "governance": {
            "agent_generated": True,
            "read_only": True,
            "llm_writeback_allowed": False,
            "the_spine_mutation_allowed": False,
            "deterministic_payloads_untouched": True,
            "requires_human_review": True,
        },
    }


def main():

    context = load_json(CONTEXT_PATH)

    prompt = build_prompt(context)

    llm_result = call_llm(prompt)

    # =====================================================
    # CASE 1 ? LIVE SUCCESS
    # =====================================================

    if llm_result.get("_live_success"):

        output = build_output(
            result=llm_result,
            mode="live_llm_read_only_synthesis",
        )

        save_json(OUT_PATH, output)

        save_json(CACHE_PATH, output)

        print(
            "Agent mode -> live_llm_read_only_synthesis"
        )

        print(
            f"Updated cache -> {CACHE_PATH}"
        )

        return

    # =====================================================
    # CASE 2 ? CACHE EXISTS
    # =====================================================

    if CACHE_PATH.exists():

        cached = load_json(CACHE_PATH)

        cached["agent_mode"] = (
            "cached_llm_synthesis"
        )

        cached["generated_utc"] = (
            datetime.now(timezone.utc)
            .isoformat()
        )

        cached["fallback_reason"] = (
            llm_result.get(
                "_fallback_reason"
            )
        )

        save_json(OUT_PATH, cached)

        print(
            "Agent mode -> cached_llm_synthesis"
        )

        print(
            f"Fallback reason -> "
            f"{llm_result.get('_fallback_reason')}"
        )

        return

    # =====================================================
    # CASE 3 ? RULE FALLBACK
    # =====================================================

    fallback = fallback_synthesis(context)

    output = build_output(
        result=fallback,
        mode="fallback_rule_synthesis",
        fallback_reason=llm_result.get(
            "_fallback_reason"
        ),
    )

    save_json(OUT_PATH, output)

    print(
        "Agent mode -> fallback_rule_synthesis"
    )

    print(
        f"Fallback reason -> "
        f"{llm_result.get('_fallback_reason')}"
    )


if __name__ == "__main__":
    main()
