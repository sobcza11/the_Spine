from pathlib import Path


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

OUT_PATH = ROOT / "data" / "offline_sites" / "index.html"


SITES = [
    ("Equities Industry", "equities-industry/index.html"),
    ("Equities Sector", "equities-sector/index.html"),
    ("C_FLOW", "c-flow/index.html"),
    ("FX", "fx/index.html"),
    ("Rates", "rates/index.html"),
]


def main():
    links = "\n".join(
        [
            f'<li><a href="{href}">{label}</a></li>'
            for label, href in SITES
        ]
    )

    html = f"""
<html>
<head>
<title>IsoVector ? Offline Site Index</title>
</head>

<body>

<h1>IsoVector Offline Site Index</h1>

<h2>OFFLINE REVIEW MODE</h2>

<ul>
    <li>writeback_allowed: false</li>
    <li>source_payloads_required: true</li>
    <li>human_review_required: true</li>
    <li>deployment_target: isovector.io</li>
</ul>

<hr>

<h2>Offline Operational Cognition Sites</h2>

<ul>
{links}
</ul>

</body>
</html>
"""

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    OUT_PATH.write_text(
        html,
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
