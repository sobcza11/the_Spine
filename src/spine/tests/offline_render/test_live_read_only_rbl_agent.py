from pathlib import Path
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

AGENT_OUTPUT = ROOT / "rbl_agent" / "langroid_rbl_agent_output.json"
DASHBOARD = ROOT / "offline_render" / "rbl_agent_cognitive_dashboard.html"

DETERMINISTIC_FILES = [
    ROOT / "oraclechambers" / "oc_rbl_local.json",
    ROOT / "oraclechambers" / "oc_final_metric_local.json",
    ROOT / "oraclechambers" / "oc_contradiction_local.json",
    ROOT / "oraclechambers" / "oc_attention_routing_local.json",
]


def test_live_read_only_rbl_agent():
    assert AGENT_OUTPUT.exists()
    assert DASHBOARD.exists()

    agent = json.loads(AGENT_OUTPUT.read_text(encoding="utf-8"))

    assert agent["module"] == "langroid-rbl-agent-output"
    assert agent["agent_mode"] == "live_read_only_execution_stub"
    assert agent["governance"]["read_only"] is True
    assert agent["governance"]["llm_writeback_allowed"] is False
    assert agent["governance"]["the_spine_mutation_allowed"] is False
    assert agent["governance"]["deterministic_payloads_untouched"] is True
    assert agent["governance"]["requires_human_review"] is True
    assert len(agent["synthesis"]) > 0
    assert len(agent["source_payloads"]) > 0

    for path in DETERMINISTIC_FILES:
        assert path.exists(), f"Missing deterministic source payload: {path}"

    html = DASHBOARD.read_text(encoding="utf-8")

    assert "RBL Agent Cognitive Dashboard" in html
    assert "Agent RBL Synthesis" in html
    assert "Human" in html or "human" in html
