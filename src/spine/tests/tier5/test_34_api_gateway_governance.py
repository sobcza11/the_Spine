from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier5\api_gateway_governance.json"
)

def test_api_gateway_governance():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "api-gateway-governance"

    assert d["gateway_governance_enabled"] is True

    assert d["endpoint_count"] > 0

    assert len(d["endpoints"]) > 0

    assert d["governance"]["rate_limiting_enabled"] is True
