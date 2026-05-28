from pathlib import Path


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

OUT_DIR = ROOT / "data" / "offline_sites" / "rates"

OUT_PATH = OUT_DIR / "index.html"


HTML = """
<html>
<head>
<title>IsoVector ? RATES</title>
</head>

<body>

<h1>RATES</h1>

<h2>OFFLINE REVIEW MODE</h2>

<ul>
<li>writeback_allowed: false</li>
<li>human_review_required: true</li>
<li>deployment_target: isovector.io</li>
</ul>

<hr>

<h2>Z? Panel</h2>
<p>Z? ? Bond Market ? zeitgeist</p>

<h2>RBL Panel</h2>
<p>RBL ? Bond Mrkt (Systemic) ? OC</p>

<h2>Systemic Panel</h2>
<p>RBL ? Bond Mrkt (Systemic) ? OC</p>

</body>
</html>
"""


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    OUT_PATH.write_text(
        HTML,
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
