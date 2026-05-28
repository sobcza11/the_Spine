from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier5\rbac_authentication.json"
)

def test_rbac_authentication():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "rbac-authentication"

    assert d["authentication_enabled"] is True

    assert len(d["roles"]) > 0

    assert d["governance"]["role_based_access_control"] is True
