from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\executive_offline_render_package.json"
)

def test_executive_offline_render_package():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "executive-offline-render-package"
    assert d["executive_offline_render_package_enabled"] is True
    assert d["render_artifact_count"] >= 6

    assert "executive_pdf_packet" in d["render_artifacts"]

    assert d["render_contract"]["offline_pdf_render_required"] is True
    assert d["governance"]["offline_distribution_required"] is True
