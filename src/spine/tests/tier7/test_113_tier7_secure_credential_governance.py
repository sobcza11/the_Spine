from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_secure_credential_governance.json"
)

def test_tier7_secure_credential_governance():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-secure-credential-governance"
    assert d["secure_credential_governance_enabled"] is True
    assert d["secret_domain_count"] >= 7

    assert "api_keys" in d["secret_domains"]
    assert "cloud_storage_credentials" in d["secret_domains"]
    assert "deployment_environment_secrets" in d["secret_domains"]

    assert d["credential_contract"]["plaintext_secrets_forbidden"] is True
    assert d["credential_contract"]["vault_integration_required_for_production"] is True

    assert d["secret_policy"]["store_secrets_in_repo"] is False
    assert d["secret_policy"]["print_secrets_to_logs"] is False
    assert d["secret_policy"]["use_runtime_injection"] is True

    assert d["governance"]["credential_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
