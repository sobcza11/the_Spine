from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

TIER7_DIR = ROOT / "tier7"
OUT_PATH = TIER7_DIR / "tier7_secure_credential_governance.json"


SECRET_DOMAINS = [
    "api_keys",
    "database_credentials",
    "cloud_storage_credentials",
    "llm_provider_credentials",
    "market_data_vendor_credentials",
    "service_account_tokens",
    "deployment_environment_secrets",
]


def main():
    TIER7_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "tier7-secure-credential-governance",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "secure_credential_governance_enabled": True,

        "secret_domains": SECRET_DOMAINS,

        "secret_domain_count": len(SECRET_DOMAINS),

        "credential_objective": (
            "Define vaulted runtime secret management for API keys, databases, cloud "
            "storage, LLM providers, market data vendors, service accounts, and "
            "deployment environments."
        ),

        "credential_contract": {
            "plaintext_secrets_forbidden": True,
            "environment_variable_loading_required": True,
            "vault_integration_required_for_production": True,
            "credential_rotation_required": True,
            "secret_access_auditing_required": True,
        },

        "secret_policy": {
            "store_secrets_in_repo": False,
            "print_secrets_to_logs": False,
            "commit_env_files": False,
            "use_runtime_injection": True,
            "least_privilege_required": True,
        },

        "governance": {
            "credential_governance_enabled": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
            "audit_trail_required": True,
            "fail_closed_default": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
