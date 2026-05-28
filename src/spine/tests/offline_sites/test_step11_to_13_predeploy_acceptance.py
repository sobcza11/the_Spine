from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine"
)

PKG = ROOT / "data" / "deploy_static" / "isovector_offline_approved"

SITES = [
    "equities-industry",
    "equities-sector",
    "c-flow",
    "fx",
    "rates",
]


def test_step_11_static_package_export():

    assert PKG.exists()
    assert (PKG / "index.html").exists()
    assert (PKG / "app.js").exists()
    assert (PKG / "styles.css").exists()
    assert (PKG / "deployment_manifest.json").exists()

    for site in SITES:
        assert (PKG / site / "index.html").exists()

    d = json.loads(
        (PKG / "deployment_manifest.json").read_text(
            encoding="utf-8"
        )
    )

    assert d["module"] == "approved-static-export"
    assert d["deployment_target"] == "isovector.io"
    assert d["site_count"] == 5
    assert d["governance"]["ready_for_static_handoff"] is True


def test_step_12_predeploy_smoke_test():

    p = PKG / "predeploy_smoke_test.json"

    assert p.exists()

    d = json.loads(
        p.read_text(encoding="utf-8")
    )

    assert d["module"] == "predeploy-smoke-test"
    assert d["predeploy_smoke_test_enabled"] is True
    assert d["master_index_exists"] is True
    assert d["site_count"] == 5
    assert d["passed_site_count"] == 5

    assert d["smoke_test_contract"]["secret_scan_required"] is True
    assert d["governance"]["deployment_blocked_on_failure"] is True


def test_step_13_deployment_hold_gate():

    p = ROOT / "data" / "deploy_static" / "predeploy_acceptance.json"

    assert p.exists()

    d = json.loads(
        p.read_text(encoding="utf-8")
    )

    assert d["module"] == "deployment-hold-gate"
    assert d["offline_visual_acceptance"] is True
    assert d["all_5_sites_rendered"] is True
    assert d["human_review_complete"] is True
    assert d["ready_for_isovector_io"] is True

    assert d["hold_gate_contract"]["deployment_hold_until_acceptance"] is True
    assert d["governance"]["deploy_first_govern_later_forbidden"] is True
