from pathlib import Path
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

AGENT_OUTPUT = ROOT / "rbl_agent" / "langroid_rbl_agent_output.json"
DASHBOARD = ROOT / "offline_render" / "rbl_agent_cognitive_dashboard.html"


def test_llm_rbl_agent():
    assert AGENT_OUTPUT.exists()
    assert DASHBOARD.exists()

    agent = json.loads(AGENT_OUTPUT.read_text(encoding="utf-8"))

    assert agent["module"] == "langroid-rbl-agent-output"
    assert agent["governance"]["live_llm_attempted"] is True
    assert agent["governance"]["read_only"] is True
    assert agent["governance"]["llm_writeback_allowed"] is False
    assert agent["governance"]["the_spine_mutation_allowed"] is False
    assert agent["governance"]["deterministic_payloads_untouched"] is True
    assert len(agent["synthesis"]) > 0
    assert len(agent["source_payloads"]) > 0

    assert agent["agent_mode"] in [
        "live_llm_read_only_synthesis",
        "fallback_rule_synthesis",
    ]

    html = DASHBOARD.read_text(encoding="utf-8")

    assert "RBL Agent Cognitive Dashboard" in html
    assert "Agent RBL Synthesis" in html
