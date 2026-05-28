from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")
OUT = ROOT / "data" / "review_runtime"

PAYLOADS = {
    "rates": {
        "US": {
            "title": "United States",
            "zt": ["Curve regime remains the core macro constraint.", "Policy pressure frames equity duration, USD strength, & liquidity conditions.", "Watch whether 2Y/10Y behavior confirms easing expectations or rejects them."],
            "rbl": ["US rates are the anchor layer for global discount rates.", "Systemic risk rises when curve relief appears but policy pressure stays restrictive.", "Reserved space likely needs policy-path context plus curve graph support."]
        },
        "CN": {
            "title": "China",
            "zt": ["China is a hybrid rates case, not a clean 2Y/10Y sovereign curve read.", "RBCNBIS functions as long-sovereign proxy; INTDSRCNM193N functions as liquidity/policy-pressure proxy.", "Do not force China into standard curve logic."],
            "rbl": ["China requires separate interpretive treatment because policy liquidity & sovereign yield proxies carry different meanings.", "Systemic relevance is high through FX, commodities, global demand, & EM pressure.", "Reserved space should likely support explanatory doctrine, not just charting."]
        },
        "JP": {
            "title": "Japan",
            "zt": ["Japan rates cognition centers on yield-control legacy, JPY pressure, & global carry sensitivity.", "Small rate moves can create large FX transmission.", "Watch whether sovereign stability masks currency instability."],
            "rbl": ["Japan is systemic through funding, carry, & global duration behavior.", "RBL likely needs more room when JPY stress overlaps with US yield strength.", "Reserved area may need FX-rate linkage context."]
        },
        "DE": {
            "title": "Germany",
            "zt": ["Germany functions as the euro-area rates anchor.", "Curve movement should be read against ECB stance, growth softness, & energy sensitivity.", "Domestic curve stress can transmit into EUR confidence."],
            "rbl": ["German rates are less isolated than they appear; they proxy euro-area funding pressure.", "Systemic interpretation should emphasize ECB credibility, peripheral spread risk, & EUR transmission.", "Reserved space may support EU core/periphery contrast."]
        },
        "IT": {
            "title": "Italy",
            "zt": ["Italy is primarily a sovereign spread fragility lens.", "Rates cognition should emphasize spread widening, funding confidence, & ECB backstop sensitivity.", "Watch divergence from Germany more than absolute yield alone."],
            "rbl": ["Italy has higher systemic relevance when spreads widen while ECB policy remains restrictive.", "RBL needs sovereign-risk framing, not generic curve language.", "Reserved space may need spread-stress visualization."]
        },
        "UK": {
            "title": "United Kingdom",
            "zt": ["UK rates reflect policy credibility, inflation persistence, & gilts-market sensitivity.", "Curve behavior can shift quickly when fiscal credibility or inflation expectations move.", "Watch GBP linkage alongside policy pressure."],
            "rbl": ["UK systemic interpretation should connect gilts, GBP, mortgage sensitivity, & BOE credibility.", "RBL may need more space during inflation-policy conflict regimes.", "Reserved space can support policy credibility notes."]
        },
        "AU": {
            "title": "Australia",
            "zt": ["Australia rates are tied to China demand, commodities, housing, & RBA policy path.", "Curve pressure should be read through domestic housing sensitivity plus external demand.", "Watch AUD linkage when rates & commodity signals diverge."],
            "rbl": ["Australia is a cross-asset bridge between commodities, China, FX, & rates.", "RBL should emphasize transmission rather than domestic rates alone.", "Reserved space may support commodity/FX linkage."]
        },
        "CA": {
            "title": "Canada",
            "zt": ["Canada rates combine housing sensitivity, oil exposure, & BoC policy stance.", "Curve pressure should be read alongside CAD & crude behavior.", "Watch whether domestic softness conflicts with commodity support."],
            "rbl": ["Canada is systemic mainly through housing leverage, energy, CAD, & US spillover.", "RBL may need compact but explicit cross-border linkage.", "Reserved space can support oil/rates/CAD triangle."]
        },
        "CH": {
            "title": "Switzerland",
            "zt": ["Switzerland rates act as safe-haven & policy-stability context.", "Low-rate behavior should be read through CHF strength, inflation control, & capital preservation.", "Watch CHF stress when global risk rises."],
            "rbl": ["Switzerland is systemic through safe-haven flows rather than yield pressure alone.", "RBL should stay compact unless CHF behavior becomes central.", "Reserved space likely stays available for graph or FX linkage."]
        },
    },

    "fx": {
        "EURUSD": {
            "title": "EUR/USD",
            "zt": ["EUR/USD reflects Fed-ECB divergence, euro-area growth pressure, & USD liquidity demand.", "Pair weakness can signal tighter global dollar conditions.", "Watch Germany/Italy spread context alongside EUR behavior."],
            "rbl": ["EUR/USD is a policy-divergence & regional confidence channel.", "Systemic read strengthens when EUR weakness aligns with sovereign spread stress.", "Reserved space may need rates-spread overlay."]
        },
        "USDJPY": {
            "title": "USD/JPY",
            "zt": ["USD/JPY is the cleanest FX stress-transmission pair in this universe.", "JPY weakness can indicate yield-differential pressure, carry stress, & liquidity imbalance.", "Watch Japan rates plus US yields together."],
            "rbl": ["USD/JPY deserves systemic attention because small rate changes can produce large currency effects.", "RBL likely needs extra space for carry, policy, & intervention context.", "Reserved FX space may be best used here."]
        },
        "GBPUSD": {
            "title": "GBP/USD",
            "zt": ["GBP/USD reflects UK policy credibility, inflation persistence, & USD pressure.", "Weakness can signal domestic credibility stress or broad dollar strength.", "Watch gilts behavior beside GBP moves."],
            "rbl": ["GBP/USD interpretation should connect BOE policy, fiscal credibility, & global USD regime.", "Systemic risk rises if gilt instability returns.", "Reserved space may support UK rates linkage."]
        },
        "USDCAD": {
            "title": "USD/CAD",
            "zt": ["USD/CAD combines USD liquidity, oil exposure, & Canada housing sensitivity.", "CAD weakness during oil strength is a contradiction worth flagging.", "Watch BoC policy pressure plus crude behavior."],
            "rbl": ["USD/CAD is useful for commodity-policy divergence detection.", "RBL should emphasize oil/CAD/rates conflict.", "Reserved space may support commodity overlay."]
        },
        "AUDUSD": {
            "title": "AUD/USD",
            "zt": ["AUD/USD is a China-demand, commodity-beta, & risk-appetite lens.", "Weak AUD during commodity strength can indicate China or liquidity concern.", "Watch Australia rates plus China proxy signals."],
            "rbl": ["AUD/USD is systemic when China, commodities, & USD liquidity align negatively.", "RBL needs cross-domain context more than pure FX commentary.", "Reserved space may need China/commodity linkage."]
        },
        "USDCHF": {
            "title": "USD/CHF",
            "zt": ["USD/CHF reflects safe-haven behavior, USD liquidity, & Swiss policy stability.", "CHF strength can signal defensive capital behavior.", "Watch divergence from broader USD trend."],
            "rbl": ["USD/CHF is most valuable as a risk-aversion confirmation lens.", "RBL can remain compact unless CHF decouples sharply.", "Reserved space likely remains available."]
        },
    },

    "equities_index": {
        "SPY": ["Broad-market baseline; use as equity system reference, not final confirmation.", "If SPY holds while breadth weakens, systemic fragility rises."],
        "QQQ": ["Growth/duration-sensitive equity lens; highly exposed to rates pressure.", "Needs rates linkage when yield pressure rises."],
        "DIA": ["Legacy-quality/value lens; useful for defensive industrial confirmation.", "Can hide broader market weakness if narrow leadership persists."],
        "ITOT": ["Total-market participation lens; best for broad confirmation.", "If ITOT diverges from SPY, breadth risk increases."],
        "MDY": ["Mid-cap domestic cyclicality lens; sensitive to credit & demand conditions.", "Useful bridge between large-cap resilience & small-cap fragility."],
        "IWM": ["Small-cap financial-conditions lens; high sensitivity to liquidity & credit stress.", "Likely needs more interpretive space during tightening regimes."]
    },

    "equities_sector": {
        "XLE": ["Energy sector links equities to commodity pressure.", "Key contradiction: energy strength with broad equity weakness."],
        "XLF": ["Financials reflect curve, credit, & liquidity transmission.", "Needs rates linkage when curve stress rises."],
        "XLK": ["Technology is duration-sensitive & leadership-heavy.", "Needs breadth check when index strength is tech-concentrated."],
        "XLI": ["Industrials reflect cyclical demand confirmation.", "Useful for validating real-economy participation."],
        "XLP": ["Staples serve as defensive rotation signal.", "Strength here can indicate risk-off migration."],
        "XLU": ["Utilities reflect defensiveness & rate sensitivity.", "Can conflict with higher-yield environments."],
        "XLV": ["Healthcare acts as defensive-quality equity ballast.", "Useful in late-cycle rotation review."],
        "XLY": ["Consumer discretionary reflects household confidence & rate sensitivity.", "Weakness can confirm demand stress."],
        "XLB": ["Materials connect equities to commodity & industrial demand.", "Useful for inflation-growth contradiction."],
        "XLC": ["Communication services mix growth, media, & platform exposure.", "Needs index-leadership context."],
        "XLRE": ["Real estate is rates-sensitive & financing-dependent.", "Likely needs expanded RBL when rates pressure rises."]
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
.list-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}
.item{border:1px solid #223347;background:#050c14;border-radius:10px;padding:10px}
.item a{color:#e6ecf5;text-decoration:none;font-weight:600}
.item span{display:block;color:#7e899b;font-size:11px;margin-top:4px}
p,li{line-height:1.55;color:#a5b0c1}
.meta{margin-top:14px;color:#7e899b;font-size:12px;border-top:1px solid #223347;padding-top:10px}
"""

def card(title, rows):
    return f"<section class='card'><h2>{title}</h2><ul>{''.join(f'<li>{x}</li>' for x in rows)}</ul></section>"

def page(path, kicker, title, zt_rows, rbl_rows):
    path.mkdir(parents=True, exist_ok=True)
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><link rel="stylesheet" href="../../review.css"><title>{title}</title></head>
<body><main class="shell">
<header class="header"><div class="kicker">{kicker}</div><h1>{title}</h1><p>Actual cognition comments for layout-density review.</p></header>
<div class="grid">
{card("Zₜ Output", zt_rows)}
{card("RBL Output", rbl_rows)}
{card("Reserved Space Decision", ["Reallocate only after visual review.", "Use reserved area if cognition feels cramped.", "Keep reserved panel if Zₜ & RBL scan cleanly."])}
</div>
<div class="meta">generated_utc: {datetime.now(timezone.utc).isoformat()} | writeback_allowed: false | layout_locked: false</div>
</main></body></html>"""
    (path / "index.html").write_text(html, encoding="utf-8")

def item(path, title, sub):
    return f"<div class='item'><a href='{path}'>{title}</a><span>{sub}</span></div>"

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "review.css").write_text(CSS, encoding="utf-8")

    for code, d in PAYLOADS["rates"].items():
        page(OUT / "rates" / code, "RATES COGNITION REVIEW", d["title"], d["zt"], d["rbl"])

    for sym, d in PAYLOADS["fx"].items():
        page(OUT / "fx" / sym, "FX COGNITION REVIEW", d["title"], d["zt"], d["rbl"])

    for sym, rows in PAYLOADS["equities_index"].items():
        page(OUT / "equities_index" / sym, "EQUITIES INDEX COGNITION REVIEW", sym, rows, [
            "Review whether index behavior is confirmed by breadth, sector rotation, & rates pressure.",
            "RBL should identify when headline index behavior is misleading."
        ])

    for sym, rows in PAYLOADS["equities_sector"].items():
        page(OUT / "equities_sector" / sym, "EQUITIES SECTOR COGNITION REVIEW", sym, rows, [
            "Review whether sector behavior confirms or contradicts market-level state.",
            "RBL should identify rotation, defensiveness, cyclicality, & systemic linkage."
        ])

    index = f"""<!doctype html>
<html><head><meta charset="utf-8"><link rel="stylesheet" href="review.css"><title>IsoVector Review Runtime</title></head>
<body><main class="shell">
<header class="header"><div class="kicker">ACTUAL COGNITION REVIEW</div><h1>IsoVector Offline Review Runtime</h1><p>Instrument-level cognitive comments for RESERVED-space decisions.</p></header>

<section class="card"><h2>RATES</h2><div class="list-grid">
{''.join(item(f"rates/{k}/index.html", k, v["title"]) for k,v in PAYLOADS["rates"].items())}
</div></section>

<section class="card"><h2>FX</h2><div class="list-grid">
{''.join(item(f"fx/{k}/index.html", v["title"], k) for k,v in PAYLOADS["fx"].items())}
</div></section>

<section class="card"><h2>EQUITIES — INDEX</h2><div class="list-grid">
{''.join(item(f"equities_index/{k}/index.html", k, "Index cognition") for k in PAYLOADS["equities_index"])}
</div></section>

<section class="card"><h2>EQUITIES — SECTOR</h2><div class="list-grid">
{''.join(item(f"equities_sector/{k}/index.html", k, "Sector cognition") for k in PAYLOADS["equities_sector"])}
</div></section>

</main></body></html>"""
    (OUT / "index.html").write_text(index, encoding="utf-8")
    print(f"Wrote -> {OUT / 'index.html'}")

if __name__ == "__main__":
    main()
