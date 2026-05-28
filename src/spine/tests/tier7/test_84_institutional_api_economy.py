from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_api_economy.json"
)

def test_institutional_api_economy():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-api-economy"
    assert d["api_economy_enabled"] is True
    assert d["api_domain_count"] > 0

    assert d["api_contract"]["read_only_default"] is True
    assert d["api_contract"]["schema_validation_required"] is True
    assert d["api_contract"]["access_control_required"] is True

    assert d["governance"]["api_economy_governed"] is True
    assert d["governance"]["write_access_restricted"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
