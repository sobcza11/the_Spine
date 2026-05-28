from pathlib import Path
from datetime import datetime, timezone
import json


REPO_ROOT = Path(__file__).resolve().parents[5]
ROOT = REPO_ROOT / "_offline_site" / "oc_ai_components"

PAYLOADS = [
    ("fx", "fx_ai_components_v1.json"),
    ("rates", "rates_ai_components_v1.json"),
    ("c_flow", "cflow_ai_components_v1.json"),
    ("equities_sector", "equities_sector_ai_components_v1.json"),
    ("equities_industry", "equities_industry_ai_components_v1.json"),
]


def main() -> None:
    failures = []

    for site, file in PAYLOADS:
        path = ROOT / site / "payloads" / file
        payload = json.loads(path.read_text(encoding="utf-8"))

        for key in [
            "analytics",
            "feature_vector",
            "historical_analogs",
            "executive_decision_layer",
        ]:
            if key not in payload:
                failures.append(f"{site}:missing_{key}")

        if not payload.get("client_visible_intelligence"):
            failures.append(f"{site}:not_client_visible")

    for file in [
        "client_dashboard.html",
        "client_dashboard.css",
        "client_dashboard.js",
    ]:
        if not (ROOT / file).exists():
            failures.append(f"missing_dashboard_file:{file}")

    result = {
        "artifact": "test_ai_component_analytics_dashboard_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
    }

    print(json.dumps(result, indent=2))

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    