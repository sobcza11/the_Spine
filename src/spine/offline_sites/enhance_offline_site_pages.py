from pathlib import Path
from datetime import datetime, timezone


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

SITE_ROOT = ROOT / "data" / "offline_sites"


SITES = {
    "equities-industry": {
        "title": "EQUITIES ? INDUSTRY",
        "zt": "Z? ? Equity Mrkt ? zeitgeist",
        "rbl": "RBL ? Equity Mrkt ? OC",
        "systemic": "RBL ? Equity Mrkt (Systemic) ? OC",
    },
    "equities-sector": {
        "title": "EQUITIES ? SECTOR",
        "zt": "Z? ? Equity - Sector ? zeitgeist",
        "rbl": "RBL ? Equity Sectors ? OC",
        "systemic": None,
    },
    "c-flow": {
        "title": "C_FLOW",
        "zt": "Z? ? Commodity Flow ? zeitgeist",
        "rbl": "RBL ? Commodities ? OC",
        "systemic": "RBL ? Commodities (Systemic) ? OC",
    },
    "fx": {
        "title": "FX",
        "zt": "Z? ? FX zeitgeist",
        "rbl": "RBL ? FX (Systemic) ? OC",
        "systemic": "RBL ? FX (Systemic) ? OC",
    },
    "rates": {
        "title": "RATES",
        "zt": "Z? ? Bond Market ? zeitgeist",
        "rbl": "RBL ? Bond Mrkt (Systemic) ? OC",
        "systemic": "RBL ? Bond Mrkt (Systemic) ? OC",
    },
}


def source_row(label, artifact):
    return f"""
    <tr>
        <td>{label}</td>
        <td>{artifact}</td>
        <td>{datetime.now(timezone.utc).isoformat()}</td>
        <td>data/offline_sites/source_payloads/{artifact}</td>
        <td>required</td>
    </tr>
    """


def build_html(slug, spec):
    systemic_block = ""

    if spec["systemic"]:
        systemic_block = f"""
        <section>
            <h2>Systemic RBL Panel</h2>
            <p>{spec["systemic"]}</p>
        </section>
        """

    source_rows = source_row(
        "Z? artifact",
        f"{slug}_zt_payload.json",
    )

    source_rows += source_row(
        "RBL artifact",
        f"{slug}_rbl_payload.json",
    )

    if spec["systemic"]:
        source_rows += source_row(
            "Systemic artifact",
            f"{slug}_systemic_payload.json",
        )

    return f"""
<html>
<head>
<title>IsoVector ? {spec["title"]}</title>
</head>

<body>

<header>
    <h1>{spec["title"]}</h1>
    <h2>OFFLINE REVIEW MODE</h2>

    <ul>
        <li>writeback_allowed: false</li>
        <li>ai_generated: true</li>
        <li>source_payloads_required: true</li>
        <li>human_review_required: true</li>
        <li>deployment_target: isovector.io</li>
    </ul>
</header>

<hr>

<section>
    <h2>Z? Panel</h2>
    <p>{spec["zt"]}</p>
</section>

<section>
    <h2>RBL Panel</h2>
    <p>{spec["rbl"]}</p>
</section>

{systemic_block}

<hr>

<section>
    <h2>Source Payload Coverage</h2>
    <table border="1">
        <tr>
            <th>Payload Type</th>
            <th>Artifact</th>
            <th>Timestamp</th>
            <th>Local Source Path</th>
            <th>Status</th>
        </tr>
        {source_rows}
    </table>
</section>

<hr>

<section>
    <h2>Visual Acceptance Checklist</h2>
    <ul>
        <li>[ ] Z? renders</li>
        <li>[ ] RBL renders</li>
        <li>[ ] Systemic panel renders, if required</li>
        <li>[ ] Sources visible</li>
        <li>[ ] Governance visible</li>
        <li>[ ] No local secrets</li>
        <li>[ ] No broken links</li>
        <li>[ ] Ready for export</li>
    </ul>
</section>

</body>
</html>
"""


def main():
    for slug, spec in SITES.items():
        out_dir = SITE_ROOT / slug
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "index.html"

        out_path.write_text(
            build_html(slug, spec),
            encoding="utf-8",
        )

        print(f"Wrote -> {out_path}")


if __name__ == "__main__":
    main()
