from pathlib import Path


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\offline_sites"
)


DIRS = [
    "equities-industry",
    "equities-sector",
    "c-flow",
    "fx",
    "rates",
]


def test_offline_site_renderers():

    for d in DIRS:

        p = ROOT / d / "index.html"

        assert p.exists()

        html = p.read_text(
            encoding="utf-8"
        )

        assert "OFFLINE REVIEW MODE" in html
        assert "writeback_allowed: false" in html
        assert "deployment_target: isovector.io" in html

        assert "Z?" in html
        assert "RBL" in html
