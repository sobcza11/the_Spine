from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_deployment_governance_os.json"
)

def test_institutional_deployment_governance_os():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-deployment-governance-os"
    assert d["deployment_governance_enabled"] is True
    assert d["deployment_governance_domain_count"] > 0

    assert d["deployment_governance_contract"]["release_readiness_required"] is True
    assert d["deployment_governance_contract"]["rollback_supported"] is True
    assert d["deployment_governance_contract"]["runtime_controls_required"] is True

    assert d["governance"]["institutional_deployment_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["mutation_requires_authorization"] is True
