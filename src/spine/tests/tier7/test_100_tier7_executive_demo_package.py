from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_executive_demo_package.json"
)

def test_tier7_executive_demo_package():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-executive-demo-package"
    assert d["executive_demo_package_enabled"] is True

    assert d["required_demo_artifact_count"] == 8
    assert d["existing_demo_artifact_count"] == 8
    assert d["demo_package_complete"] is True

    assert d["demo_contract"]["summary_artifacts_available"] is True
    assert d["demo_contract"]["demo_readiness_supported"] is True
    assert d["demo_contract"]["executive_review_ready"] is True

    assert d["governance"]["demo_package_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
