from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier5\cicd_deployment_governance.json"
)

def test_cicd_deployment_governance():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cicd-deployment-governance"

    assert d["deployment_governance_enabled"] is True

    assert d["pipeline_stage_count"] > 0

    assert len(d["pipeline_stages"]) > 0

    assert d["governance"]["rollback_supported"] is True
