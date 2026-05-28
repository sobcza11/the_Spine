from pathlib import Path


HTML_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\offline_render\offline_executive_dashboard.html"
)


def test_offline_executive_dashboard():

    assert HTML_PATH.exists()

    text = HTML_PATH.read_text(encoding="utf-8")

    assert "IsoVector Executive Dashboard" in text
    assert "Offline Institutional Cognition Rendering" in text
    assert "Governance" in text
    assert "Human Review Required" in text
