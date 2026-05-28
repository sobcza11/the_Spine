from pathlib import Path
from datetime import datetime, timezone
import json

ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")
OUT = ROOT / "data" / "review_runtime"

RATES = {
    "AU": "Australia",
    "CA": "Canada",
    "DE": "Germany",
    "JP": "Japan",
    "UK": "United Kingdom",
    "CH": "Switzerland",
    "IT": "Italy",
    "US": "United States",
    "CN": "China",
}

FX = {
    "AUDUSD": "AUD/USD",
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDCAD": "USD/CAD",
    "USDCHF": "USD/CHF",
    "USDJPY": "USD/JPY",
}

EQUITIES_INDEX = [
    "SPY", "QQQ", "DIA", "ITOT", "MDY", "IWM"
]

EQUITIES_SECTOR = [
    "XLB", "XLC", "XLE", "XLF", "XLI",
    "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"
]

CSS = """
body{margin:0;background:#03070d;color:#e6ecf5;font-family:Inter,Arial,sans-serif}
.shell{padding:32px}
.header,.card{border:1px solid #1f2a38;background:#08121d;border-radius:14px;padding:20px;margin-bottom:18px}
.kicker{color:#7c5cff;font-size:12px;letter-spacing:.14em;text-transform:uppercase}
h1{margin:8px 0;font-size:34px}
h2{margin:8px 0 12px;color:#f2d8a7}
h3{margin:0 0 10px;color:#7fd7ff;font-size:15px}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}
.list-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}
.item{border:1px solid #223347;background:#050c14;border-radius:10px;padding:10px}
.item a{color:#e6ecf5;text-decoration:none;font-weight:600}
.item span{display:block;color:#7e899b;font-size:11px;margin-top:4px}
p,li{line-height:1.55;color:#a5b0c1}
.meta{margin-top:14px;color:#7e899b;font-size:12px;border-top:1px solid #223347;padding-top:10px}
.section-gap{margin-top:26px}
"""

def build_dropdown_pages():

    for code, country in RATES.items():

        d = OUT / "rates" / code
        d.mkdir(parents=True, exist_ok=True)

        html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<link rel="stylesheet" href="../../review.css">
<title>{country} Rates Review</title>
</head>
<body>
<main class="shell">
<header class="header">
<div class="kicker">RATES REVIEW</div>
<h1>{country}</h1>
<p>Offline cognition review for {country} rates runtime.</p>
</header>

<div class="grid">

<section class="card">
<h2>Zₜ • Bond Market • zeitgeist</h2>
<ul>
<li>Curve structure & policy pressure under review.</li>
<li>Assess if rates cognition compresses naturally.</li>
<li>Review graph pressure before reallocating RESERVED space.</li>
</ul>
</section>

<section class="card">
<h2>RBL • Bond Mrkt (Systemic) • OC</h2>
<ul>
<li>Systemic sovereign interpretation review.</li>
<li>Observe whether policy context requires larger interpretation region.</li>
<li>Determine if contradiction space should expand.</li>
</ul>
</section>

</div>

<div class="meta">
writeback_allowed: false | layout_locked: false
</div>

</main>
</body>
</html>
"""
        (d / "index.html").write_text(html, encoding="utf-8")


    for sym, pair in FX.items():

        d = OUT / "fx" / sym
        d.mkdir(parents=True, exist_ok=True)

        html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<link rel="stylesheet" href="../../review.css">
<title>{pair} FX Review</title>
</head>
<body>
<main class="shell">
<header class="header">
<div class="kicker">FX REVIEW</div>
<h1>{pair}</h1>
<p>Offline cognition review for {pair}.</p>
</header>

<div class="grid">

<section class="card">
<h2>Zₜ • FX zeitgeist</h2>
<ul>
<li>Liquidity & currency pressure review.</li>
<li>Observe whether metrics or graphs dominate.</li>
<li>Review systemic transmission visibility.</li>
</ul>
</section>

<section class="card">
<h2>RBL • FX (Systemic) • OC</h2>
<ul>
<li>Cross-border liquidity interpretation.</li>
<li>Review whether systemic cognition requires larger layout authority.</li>
<li>Observe contradiction density.</li>
</ul>
</section>

</div>

<div class="meta">
writeback_allowed: false | layout_locked: false
</div>

</main>
</body>
</html>
"""
        (d / "index.html").write_text(html, encoding="utf-8")


def item(path, title, subtitle):
    return f'''
<div class="item">
<a href="{path}">{title}</a>
<span>{subtitle}</span>
</div>
'''


def main():

    OUT.mkdir(parents=True, exist_ok=True)

    (OUT / "review.css").write_text(CSS, encoding="utf-8")

    build_dropdown_pages()

    rates_items = "".join([
        item(
            f"rates/{k}/index.html",
            k,
            v
        )
        for k, v in RATES.items()
    ])

    fx_items = "".join([
        item(
            f"fx/{k}/index.html",
            v,
            k
        )
        for k, v in FX.items()
    ])

    eq_index_items = "".join([
        item(
            "#",
            x,
            "Equities Index Runtime"
        )
        for x in EQUITIES_INDEX
    ])

    eq_sector_items = "".join([
        item(
            "#",
            x,
            "Equities Sector Runtime"
        )
        for x in EQUITIES_SECTOR
    ])

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>IsoVector — Offline Cognition Review</title>
<link rel="stylesheet" href="review.css">
</head>
<body>

<main class="shell">

<header class="header">
<div class="kicker">COGNITION-FIRST LAYOUT DISCOVERY</div>

<h1>IsoVector Offline Review Runtime</h1>

<p>
Review actual cognition density before reallocating RESERVED panels.
Observe graph pressure, narrative pressure, contradiction density & executive scan-speed.
</p>
</header>


<section class="card">
<h2>RATES</h2>

<div class="list-grid">
{rates_items}
</div>
</section>


<section class="card section-gap">
<h2>FX</h2>

<div class="list-grid">
{fx_items}
</div>
</section>


<section class="card section-gap">
<h2>EQUITIES — INDEX</h2>

<div class="list-grid">
{eq_index_items}
</div>
</section>


<section class="card section-gap">
<h2>EQUITIES — SECTOR</h2>

<div class="list-grid">
{eq_sector_items}
</div>
</section>


<section class="card section-gap">
<h2>C_FLOW</h2>

<ul>
<li>Commodity cognition runtime pending live population.</li>
<li>Review future contradiction & flow-density behavior.</li>
<li>Reserved graph areas remain reallocatable.</li>
</ul>
</section>


<div class="meta">
generated_utc: {datetime.now(timezone.utc).isoformat()}
<br>
writeback_allowed: false | human_review_required: true | layout_locked: false
</div>

</main>

</body>
</html>
"""

    (OUT / "index.html").write_text(
        html,
        encoding="utf-8"
    )

    print(f"Wrote -> {OUT / 'index.html'}")


if __name__ == "__main__":
    main()
