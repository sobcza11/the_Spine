from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")
OUT = ROOT / "data" / "review_runtime"

DOMAINS = {
    "equities_industry": {
        "title": "EQUITIES - INDUSTRY",
        "zt": "Z? ? Equity Mrkt ? zeitgeist",
        "rbl": "RBL ? Equity Mrkt ? OC",
        "systemic": "RBL ? Equity Mrkt (Systemic) ? OC",
        "reserved_action": "Evaluate whether RESERVED equity metric should become systemic RBL expansion space."
    },
    "equities_sector": {
        "title": "EQUITIES - SECTOR",
        "zt": "Z? ? Equity - Sector ? zeitgeist",
        "rbl": "RBL ? Equity Sectors ? OC",
        "systemic": None,
        "reserved_action": "Evaluate whether RESERVED sector space should support breadth / rotation graphing."
    },
    "c_flow": {
        "title": "C_FLOW",
        "zt": "Z? ? Commodity Flow ? zeitgeist",
        "rbl": "RBL ? Commodities ? OC",
        "systemic": "RBL ? Commodities (Systemic) ? OC",
        "reserved_action": "Evaluate whether RESERVED commodity graph area should support flow contradiction display."
    },
    "fx": {
        "title": "FX",
        "zt": "Z? ? FX zeitgeist",
        "rbl": "RBL ? FX (Systemic) ? OC",
        "systemic": None,
        "reserved_action": "Evaluate whether RESERVED FX metric should become systemic FX transmission space."
    },
    "rates": {
        "title": "RATES",
        "zt": "Z? ? Bond Market ? zeitgeist",
        "rbl": "RBL ? Bond Mrkt (Systemic) ? OC",
        "systemic": None,
        "reserved_action": "Evaluate whether RESERVED rates metric should support policy pressure / graph expansion."
    },
}


def block(title, label, body):
    return f"""
    <section class="review-card">
      <div class="kicker">{label}</div>
      <h2>{title}</h2>
      <p>{body}</p>
      <div class="density-grid">
        <div><span>Content Density</span><strong>REVIEW</strong></div>
        <div><span>Space Need</span><strong>UNKNOWN</strong></div>
        <div><span>Layout Action</span><strong>OBSERVE</strong></div>
      </div>
    </section>
    """


def build_domain(slug, spec):
    systemic = ""
    if spec["systemic"]:
        systemic = block(
            spec["systemic"],
            "SYSTEMIC RBL OUTPUT",
            "Systemic interpretation deposit. Use this area to observe whether cross-asset/systemic cognition needs dedicated expansion space."
        )

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{spec["title"]} ? Offline Cognition Review</title>
  <link rel="stylesheet" href="../review.css">
</head>
<body>
  <main class="shell">
    <header class="header">
      <div class="kicker">OFFLINE COGNITION REVIEW</div>
      <h1>{spec["title"]}</h1>
      <p>Purpose: review real Z? / RBL / Systemic output size, density & layout pressure before reallocating RESERVED panels.</p>
    </header>

    <div class="grid">
      {block(spec["zt"], "Z? OUTPUT", "Zeitgeist cognition deposit. Review whether this compresses naturally or requires chart/metric expansion.")}
      {block(spec["rbl"], "RBL OUTPUT", "OracleChambers cognition deposit. Review whether interpretation stays compact or needs additional vertical/wide space.")}
      {systemic}
      {block("RESERVED SPACE REVIEW", "LAYOUT DISCOVERY", spec["reserved_action"])}
    </div>

    <footer>
      writeback_allowed: false | human_review_required: true | layout_locked: false
    </footer>
  </main>
</body>
</html>
"""
    d = OUT / slug / "review"
    d.mkdir(parents=True, exist_ok=True)
    (d / "index.html").write_text(html, encoding="utf-8")

    payload = {
        "domain": slug,
        "title": spec["title"],
        "zt": spec["zt"],
        "rbl": spec["rbl"],
        "systemic": spec["systemic"],
        "layout_locked": False,
        "reserved_panels_reallocatable": True,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    (d / "review_payload.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main():
    OUT.mkdir(parents=True, exist_ok=True)

    (OUT / "review.css").write_text("""
body { margin:0; background:#03070d; color:#e6ecf5; font-family:Inter,Arial,sans-serif; }
.shell { padding:32px; }
.header { border:1px solid #1f2a38; background:#08121d; border-radius:14px; padding:22px; margin-bottom:18px; }
.kicker { color:#7c5cff; font-size:12px; letter-spacing:.14em; text-transform:uppercase; }
h1 { margin:8px 0; font-size:34px; }
h2 { margin:8px 0 10px; color:#f2d8a7; }
p { color:#a5b0c1; line-height:1.55; }
.grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:18px; }
.review-card { border:1px solid #1f2a38; background:#08121d; border-radius:14px; padding:18px; min-height:220px; }
.density-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:10px; margin-top:18px; }
.density-grid div { border:1px solid #223347; border-radius:10px; padding:10px; background:#050c14; }
.density-grid span { display:block; color:#7e899b; font-size:11px; text-transform:uppercase; }
.density-grid strong { display:block; margin-top:5px; color:#e6ecf5; }
footer { margin-top:22px; color:#7e899b; font-size:12px; }
""", encoding="utf-8")

    links = []

    for slug, spec in DOMAINS.items():
        build_domain(slug, spec)
        links.append(f'<li><a href="{slug}/review/index.html">{spec["title"]}</a></li>')

    index = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>IsoVector ? Cognition Review Outputs</title>
  <link rel="stylesheet" href="review.css">
</head>
<body>
  <main class="shell">
    <header class="header">
      <div class="kicker">COGNITION-FIRST LAYOUT DISCOVERY</div>
      <h1>IsoVector Offline Review Outputs</h1>
      <p>Open each domain to judge Z? / RBL / Systemic space needs before using RESERVED areas.</p>
    </header>
    <section class="review-card">
      <h2>Review Domains</h2>
      <ul>
        {''.join(links)}
      </ul>
    </section>
  </main>
</body>
</html>
"""
    (OUT / "index.html").write_text(index, encoding="utf-8")

    print(f"Wrote review outputs -> {OUT / 'index.html'}")


if __name__ == "__main__":
    main()
