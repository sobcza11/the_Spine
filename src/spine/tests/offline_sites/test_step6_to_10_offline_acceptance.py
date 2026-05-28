from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\offline_sites"
)


SITES = [
    "equities-industry",
    "equities-sector",
    "c-flow",
    "fx",
    "rates",
]


def test_steps_6_to_8_site_content():

    for site in SITES:

        p = ROOT / site / "index.html"

        assert p.exists()

        html = p.read_text(
            encoding="utf-8"
        )

        assert "OFFLINE REVIEW MODE" in html
        assert "writeback_allowed: false" in html
        assert "ai_generated: true" in html
        assert "source_payloads_required: true" in html
        assert "human_review_required: true" in html
        assert "deployment_target: isovector.io" in html

        assert "Source Payload Coverage" in html
        assert "Visual Acceptance Checklist" in html

        assert "[ ] Z? renders" in html
        assert "[ ] RBL renders" in html
        assert "[ ] Ready for export" in html


def test_step_9_master_index():

    p = ROOT / "index.html"

    assert p.exists()

    html = p.read_text(
        encoding="utf-8"
    )

    assert "IsoVector Offline Site Index" in html
    assert "equities-industry/index.html" in html
    assert "equities-sector/index.html" in html
    assert "c-flow/index.html" in html
    assert "fx/index.html" in html
    assert "rates/index.html" in html


def test_step_10_offline_site_validator():

    p = ROOT / "offline_site_validation.json"

    assert p.exists()

    d = json.loads(
        p.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-site-validator"
    assert d["offline_site_validator_enabled"] is True
    assert d["site_count"] == 5
    assert d["passed_site_count"] == 5
    assert d["master_index_exists"] is True

    assert d["validation_contract"]["secret_scan_required"] is True
    assert d["governance"]["deployment_blocked_on_failure"] is True
