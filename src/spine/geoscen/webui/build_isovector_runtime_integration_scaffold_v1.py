from pathlib import Path
from datetime import datetime, UTC
import json


def build_isovector_runtime_integration_scaffold_v1():

    repo_root = Path.cwd()

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "isovector"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    scaffold = {

        "component": "isovector_runtime_integration_scaffold_v1",

        "generated_at_utc": datetime.now(UTC).isoformat(),

        "deployment_targets": [

            "offline_web_runtime",
            "institutional_dashboarding",
            "executive_monitoring",
            "evidence_linked_runtime",
            "cross_domain_recursive_monitoring",
            "recursive_alerting",
            "api_serving_layer",
            "cloud_runtime_deployment",
        ],

        "future_api_targets": [

            "/api/runtime",
            "/api/dashboard",
            "/api/timeline",
            "/api/regime",
            "/api/alerts",
            "/api/governance",
            "/api/scenario_tree",
        ],

        "future_cloud_targets": [

            "AWS ECS",
            "AWS Lambda",
            "Docker Runtime",
            "CloudWatch",
            "FastAPI Runtime",
            "Institutional API Gateway",
        ],

        "governance_rules": {

            "structural_outputs_are_source_of_truth": True,
            "llm_may_explain_not_override": True,
            "recursive_governance_required": True,
            "evidence_linking_required": True,
            "human_override_allowed": True,
        },

        "status": "isovector_runtime_scaffold_complete",
    }

    json_path = (
        out_dir
        / "isovector_runtime_integration_scaffold_v1.json"
    )

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            scaffold,
            f,
            indent=2,
        )

    print("Isovector runtime integration scaffold complete")
    print("JSON:", json_path)

    return scaffold


if __name__ == "__main__":
    build_isovector_runtime_integration_scaffold_v1()
