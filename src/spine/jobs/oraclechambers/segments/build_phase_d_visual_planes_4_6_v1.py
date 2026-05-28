from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()
SEGMENT_ROOT = REPO_ROOT / "_offline_site" / "oc_segments"
SOURCE_PAYLOAD = REPO_ROOT / "data" / "serving" / "oraclechambers" / "oc_local_site_hydration_v1.json"

PLANES = [
    {
        "site_name": "oc-liquidity-stress-map-local",
        "domain": "LIQUIDITY_STRESS_MAP",
        "title": "Liquidity Stress Map",
        "subtitle": "Visualizes FX, rates, funding, commodity, and equity liquidity pressure.",
        "mode": "liquidity",
    },
    {
        "site_name": "oc-cb-divergence-map-local",
        "domain": "CB_DIVERGENCE_MAP",
        "title": "Central Bank Divergence Map",
        "subtitle": "Visualizes Fed, ECB, BoJ, China, and liquidity-policy divergence.",
        "mode": "cb",
    },
    {
        "site_name": "oc-executive-dashboard-local",
        "domain": "EXECUTIVE_DASHBOARD",
        "title": "Executive Dashboard Plane",
        "subtitle": "Unified command surface for regime, confidence, contradiction, historical memory, and deployment gate.",
        "mode": "dashboard",
    },
]


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def load_payload() -> dict:
    return json.loads(SOURCE_PAYLOAD.read_text(encoding="utf-8"))


def build_html(plane: dict) -> str:
    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{plane["title"]}</title>
<link rel="stylesheet" href="./styles.css">
</head>
<body>
<main class="shell">
<header class="hero">
<div class="eyebrow">ORACLECHAMBERS | PHASE D VISUAL INTELLIGENCE</div>
<h1>{plane["title"]}</h1>
<p>{plane["subtitle"]}</p>
</header>

<section class="grid">
<article class="card"><h2>Regime</h2><p id="regime">Loading...</p></article>
<article class="card"><h2>Confidence</h2><p id="confidence">Loading...</p></article>
<article class="card"><h2>Visual Gate</h2><p id="gate">Loading...</p></article>
</section>

<section class="card wide">
<h2>Visual Intelligence Output</h2>
<div id="visual"></div>
</section>

<section class="card wide">
<h2>Executive Interpretation</h2>
<pre id="output">Loading...</pre>
</section>

<script>
window.OC_PLANE_MODE = "{plane["mode"]}";
window.OC_DOMAIN = "{plane["domain"]}";
</script>
<script src="./app.js"></script>
</main>
</body>
</html>
"""


def build_css() -> str:
    return """body{margin:0;background:#020617;color:#e5e7eb;font-family:Arial,sans-serif}
.shell{padding:32px}
.hero{background:#0f172a;border:1px solid #334155;border-radius:18px;padding:24px;margin-bottom:24px}
.eyebrow{font-size:12px;letter-spacing:.12em;color:#94a3b8;text-transform:uppercase}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
.card{background:#111827;border:1px solid #334155;border-radius:18px;padding:20px}
.wide{margin-top:16px}
pre{white-space:pre-wrap;line-height:1.5;color:#cbd5e1}
.map{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}
.tile{border-radius:14px;padding:18px;border:1px solid #334155;background:#172554}
.tile.low{background:#064e3b}
.tile.mid{background:#78350f}
.tile.high{background:#7f1d1d}
.stack{display:grid;grid-template-columns:repeat(2,1fr);gap:14px}
.panel{border-radius:14px;padding:18px;border:1px solid #334155;background:#0f172a}
.panel.focus{border-color:#f59e0b;background:#1e293b}
"""


def build_js() -> str:
    return """async function loadVisualPlane(){
const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r=>r.json());
const sitePayload = payload.site_payload || {};
const headline = sitePayload.headline || {};
const narrative = sitePayload.narrative || {};
const historical = sitePayload.historical_memory || {};
const routing = payload.routing || {};

document.getElementById("regime").textContent = headline.regime || "Unknown";
document.getElementById("confidence").textContent = headline.confidence ?? "Unknown";
document.getElementById("gate").textContent = "Online transition blocked";

const visual = document.getElementById("visual");
const mode = window.OC_PLANE_MODE;

if(mode === "liquidity"){
  const nodes = [
    ["USD Funding","high","Elevated pressure"],
    ["Rates Liquidity","high","Duration instability"],
    ["FX Carry","mid","Carry stress active"],
    ["Energy Flow","mid","Monitoring active"],
    ["Equity Breadth","mid","Confirmation incomplete"]
  ];
  visual.className = "map";
  visual.innerHTML = nodes.map(n =>
    `<div class="tile ${n[1]}"><h3>${n[0]}</h3><p>${n[2]}</p></div>`
  ).join("");
}

if(mode === "cb"){
  const banks = [
    ["Fed","high","Restrictive"],
    ["ECB","mid","Moderately restrictive"],
    ["BoJ","low","Ultra-accommodative"],
    ["China","mid","Liquidity-sensitive"],
    ["Global Liquidity","high","Fragmented"]
  ];
  visual.className = "map";
  visual.innerHTML = banks.map(b =>
    `<div class="tile ${b[1]}"><h3>${b[0]}</h3><p>${b[2]}</p></div>`
  ).join("");
}

if(mode === "dashboard"){
  const panels = [
    ["RBL Focus", narrative.rbl_summary || "Awaiting RBL summary", "focus"],
    ["Regime", headline.regime || "Unknown", ""],
    ["Confidence", String(headline.confidence ?? "Unknown"), ""],
    ["Historical Match", historical.top_match?.historical_regime || "Unknown", ""],
    ["Deployment Gate", routing.online_runtime_ready ? "Online ready" : "Online blocked", "focus"],
    ["AI Dependency", routing.ai_dependency ? "AI dependency active" : "No AI dependency", ""]
  ];
  visual.className = "stack";
  visual.innerHTML = panels.map(p =>
    `<div class="panel ${p[2]}"><h3>${p[0]}</h3><p>${p[1]}</p></div>`
  ).join("");
}

document.getElementById("output").textContent = JSON.stringify({
  visual_plane: window.OC_DOMAIN,
  executive_summary: narrative.executive_summary,
  regime: headline.regime,
  confidence: headline.confidence,
  conviction: headline.conviction,
  macro_temperature: headline.macro_temperature,
  top_historical_match: historical.top_match,
  online_transition_allowed: false,
  interpretation:
    "Visual intelligence plane renders governed cognition as segmented offline executive intelligence before online transition."
}, null, 2);
}

loadVisualPlane().catch(error=>{
document.getElementById("output").textContent = "Visual plane failed: " + error.message;
});
"""


def build_plane(plane: dict, payload: dict) -> dict:
    site_dir = SEGMENT_ROOT / plane["site_name"]

    manifest = {
        "artifact": "phase_d_visual_plane_manifest_v1",
        "site_name": plane["site_name"],
        "domain": plane["domain"],
        "title": plane["title"],
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_site_ready": True,
        "online_transition_allowed": False,
        "phase": "PHASE_D_VISUAL_INTELLIGENCE",
    }

    write(site_dir / "index.html", build_html(plane))
    write(site_dir / "styles.css", build_css())
    write(site_dir / "app.js", build_js())
    write(site_dir / "manifest.json", json.dumps(manifest, indent=2))
    write(site_dir / "payloads" / "oc_local_site_hydration_v1.json", json.dumps(payload, indent=2))

    return {
        "site_name": plane["site_name"],
        "domain": plane["domain"],
        "ready": True,
        "path": str(site_dir),
    }


def main() -> None:
    payload = load_payload()
    results = [build_plane(plane, payload) for plane in PLANES]

    print(json.dumps({
        "artifact": "build_phase_d_visual_planes_4_6_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "site_count": len(results),
        "all_sites_ready": all(r["ready"] for r in results),
        "results": results,
    }, indent=2))


if __name__ == "__main__":
    main()

    