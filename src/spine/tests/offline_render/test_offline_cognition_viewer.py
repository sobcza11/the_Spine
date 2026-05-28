from pathlib import Path


HTML_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\offline_render\offline_cognition_viewer.html"
)


def test_offline_cognition_viewer():
    assert HTML_PATH.exists(), f"Missing viewer: {HTML_PATH}"

    text = HTML_PATH.read_text(encoding="utf-8")

    assert "IsoVector Offline Cognition Viewer" in text
    assert "Offline only" in text
    assert "card" in text
    assert "oracle" in text.lower() or "isovector" in text.lower()
