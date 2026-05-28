from pathlib import Path


HTML_PATH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\offline_render\rbl_cognitive_dashboard.html"
)


def test_rbl_cognitive_dashboard():
    assert HTML_PATH.exists()

    text = HTML_PATH.read_text(encoding="utf-8")

    assert "OracleChambers RBL Cognitive Dashboard" in text
    assert "Read Between the Lines" in text
    assert "Executive Attention" in text
    assert "Equities" in text or "EQUITIES" in text
    assert "Limitations" in text
