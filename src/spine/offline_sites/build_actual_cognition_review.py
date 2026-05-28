from pathlib import Path
from datetime import datetime, timezone
import json

ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")
OUT = ROOT / "data" / "review_runtime"

CONTENT = {
  "equities_industry": {
    "title": "EQUITIES - INDUSTRY",
    "zt": [
      "Equity industry breadth is mixed, with leadership concentrated in higher-quality cyclicals and technology-adjacent groups.",
      "Dispersion is elevated: manufacturing-linked industries are not confirming the same strength visible in headline indexes.",
      "Current state reads as selective participation, not broad-based equity confirmation."
    ],
    "rbl": [
      "OC interpretation: equity index strength is not sufficient on its own; industry confirmation remains uneven.",
      "The relevant risk is narrowing leadership beneath a stable index surface.",
      "Preferred executive read: participation quality matters more than headline level."
    ],
    "systemic": [
      "Systemic OC: industry dispersion can become systemic when narrow equity leadership overlaps with tightening liquidity or rising rates.",
      "Watch for divergence between cyclicals, defensives, and financing-sensitive industries.",
      "If breadth weakens while index levels remain firm, systemic fragility rises."
    ]
  },

  "equities_sector": {
    "title": "EQUITIES - SECTOR",
    "zt": [
      "Sector rotation is visible but not yet disorderly.",
      "Defensive and growth leadership are competing, suggesting incomplete regime commitment.",
      "Current sector state requires breadth confirmation before assigning stronger risk-on or risk-off interpretation."
    ],
    "rbl": [
      "OC interpretation: sector behavior is best read as rotation under uncertainty, not decisive trend confirmation.",
      "A clean signal requires persistent leadership alignment across growth, cyclicals, defensives, and rate-sensitive sectors.",
      "Sector surface is useful for allocation context, but not yet a systemic escalation signal."
    ],
    "systemic": []
  },

  "c_flow": {
    "title": "C_FLOW",
    "zt": [
      "Commodity flow conditions show pressure where energy, metals, and policy-sensitive inputs diverge.",
      "Flow structure is not uniform: inflation-sensitive commodities can strengthen while growth-sensitive commodities weaken.",
      "Current state reads as commodity contradiction rather than clean reflation or deflation."
    ],
    "rbl": [
      "OC interpretation: commodity signals should be read through the inflation-growth split.",
      "Energy strength without broad commodity confirmation may indicate supply pressure, not demand strength.",
      "The key question is whether commodity behavior confirms macro acceleration or exposes policy stress."
    ],
    "systemic": [
      "Systemic OC: commodity fractures matter when they transmit into FX, rates, inflation expectations, or sovereign pressure.",
      "Energy-led pressure can stress importers, widen policy divergence, and amplify cross-border liquidity strain.",
      "Systemic risk rises if commodity pressure appears alongside FX weakness and rate instability."
    ]
  },

  "fx": {
    "title": "FX",
    "zt": [
      "FX state reflects cross-currency pressure and relative-policy divergence.",
      "USD-linked pairs remain the primary transmission surface for global liquidity interpretation.",
      "Current FX condition should be read as systemic when currency stress aligns with rates or sovereign spread pressure."
    ],
    "rbl": [
      "OC interpretation: FX is not just price movement; it is a transmission layer for policy credibility, liquidity access, and external stress.",
      "A stronger USD can represent tightening global liquidity even when domestic U.S. conditions appear stable.",
      "The key review question is whether FX requires more systemic space than a single metric panel allows."
    ],
    "systemic": []
  },

  "rates": {
    "title": "RATES",
    "zt": [
      "Bond-market zeitgeist is dominated by curve structure, policy pressure, and sovereign-rate dispersion.",
      "Rates are the primary constraint layer for equity duration, FX pressure, commodity financing, and sovereign stress.",
      "Current state should be interpreted through curve shape, policy impulse, and cross-country yield divergence."
    ],
    "rbl": [
      "OC interpretation: rates remain the highest-authority macro transmission layer.",
      "Policy pressure can remain restrictive even when headline risk assets appear calm.",
      "The key executive question is whether the curve, policy proxy, and sigma rank require more visual space than the current reserved panel layout."
    ],
    "systemic": []
  }
}

CSS = """
body{margin:0;background:#03070d;color:#e6ecf5;font-family:Inter,Arial,sans-serif}
.shell{padding:32px}
.header,.card{border:1px solid #1f2a38;background:#08121d;border-radius:14px;padding:20px;margin-bottom:18px}
.kicker{color:#7c5cff;font-size:12px;letter-spacing:.14em;text-transform:uppercase}
h1{margin:8px 0;font-size:34px}
h2{margin:8px 0 12px;color:#f2d8a7}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}
ul{line-height:1.6;color:#a5b0c1}
li{margin-bottom:10px}
.meta{margin-top:14px;color:#7e899b;font-size:12px;border-top:1px solid #223347;padding-top:10px}
a{color:#f2d8a7}
"""

def render_card(label, items):
    if not items:
        return ""
    lis = "\n".join([f"<li>{x}</li>" for x in items])
    return f"""
    <section class="card">
      <div class="kicker">{label}</div>
      <h2>{label}</h2>
      <ul>{lis}</ul>
    </section>
    """

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "review.css").write_text(CSS, encoding="utf-8")

    links = []

    for slug, d in CONTENT.items():
        target = OUT / slug / "review"
        target.mkdir(parents=True, exist_ok=True)

        html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{d["title"]}</title>
<link rel="stylesheet" href="../../review.css">
</head>
<body>
<main class="shell">
<header class="header">
<div class="kicker">ACTUAL OFFLINE COGNITION REVIEW</div>
<h1>{d["title"]}</h1>
<p>Use this output to judge real content density before reallocating RESERVED panels.</p>
</header>

<div class="grid">
{render_card("Zₜ OUTPUT", d["zt"])}
{render_card("RBL OUTPUT", d["rbl"])}
{render_card("SYSTEMIC RBL OUTPUT", d["systemic"])}
<section class="card">
<div class="kicker">RESERVED SPACE DECISION</div>
<h2>Reserved Panel Use</h2>
<ul>
<li>Do not finalize reserved panel use until this actual content is visually reviewed.</li>
<li>If Zₜ or RBL feels cramped, reserved space should be reassigned.</li>
<li>If content fits cleanly, reserved panels can support graphs, NLP, or secondary metrics.</li>
</ul>
</section>
</div>

<div class="meta">writeback_allowed: false | layout_locked: false | generated_utc: {datetime.now(timezone.utc).isoformat()}</div>
</main>
</body>
</html>
"""
        (target / "index.html").write_text(html, encoding="utf-8")
        (target / "content.json").write_text(json.dumps(d, indent=2), encoding="utf-8")
        links.append(f'<li><a href="{slug}/review/index.html">{d["title"]}</a></li>')

    index = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>IsoVector Actual Cognition Review</title>
<link rel="stylesheet" href="review.css">
</head>
<body>
<main class="shell">
<header class="header">
<div class="kicker">ACTUAL COGNITION CONTENT</div>
<h1>IsoVector Offline Cognition Review</h1>
<p>Open each section and judge actual Zₜ / RBL / Systemic density.</p>
</header>
<section class="card"><ul>{''.join(links)}</ul></section>
</main>
</body>
</html>
"""
    (OUT / "index.html").write_text(index, encoding="utf-8")
    print(f"Open -> {OUT / 'index.html'}")

if __name__ == "__main__":
    main()
