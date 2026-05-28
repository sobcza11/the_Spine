from pathlib import Path
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

FILES = [
    ROOT / "rbl_agent" / "rbl_agent_input_contract.json",
    ROOT / "rbl_agent" / "rbl_grounded_context_bundle.json",
    ROOT / "rbl_agent" / "langroid_rbl_agent_output.json",
    ROOT / "rbl_agent" / "rbl_agent_saved_output_record.json",
    ROOT / "offline_render" / "rbl_agent_cognitive_dashboard.html",
]


def test_rbl_agent_ready_upgrade():
    missing = [str(p) for p in FILES if not p.exists()]
    assert not missing, f"Missing RBL agent artifacts: {missing}"

    agent_path = ROOT / "rbl_agent" / "langroid_rbl_agent_output.json"
    agent = json.loads(agent_path.read_text(encoding="utf-8"))

    assert agent["module"] == "langroid-rbl-agent-output"
    assert agent["governance"]["read_only"] is True
    assert agent["governance"]["llm_writeback_allowed"] is False
    assert agent["governance"]["the_spine_mutation_allowed"] is False
    assert len(agent["synthesis"]) > 0
    assert len(agent["source_payloads"]) > 0

    html = (ROOT / "offline_render" / "rbl_agent_cognitive_dashboard.html").read_text(encoding="utf-8")
    assert "RBL Agent Cognitive Dashboard" in html
    assert "Agent RBL Synthesis" in html
    assert "Blocked Actions" in html
