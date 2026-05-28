from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_multi_user_governance_layer.json"
)

def test_tier7_multi_user_governance_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-multi-user-governance-layer"
    assert d["multi_user_governance_enabled"] is True
    assert d["user_role_count"] >= 4

    assert d["user_roles"]["viewer"]["can_mutate"] is False
    assert d["user_roles"]["analyst"]["can_approve"] is False
    assert d["user_roles"]["executive_reviewer"]["can_approve"] is True
    assert d["user_roles"]["governance_admin"]["can_mutate"] is True

    assert d["permission_contract"]["least_privilege_required"] is True
    assert d["permission_contract"]["approval_separated_from_mutation"] is True

    assert d["governance"]["multi_user_governance_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
