from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()
SEGMENT_ROOT = REPO_ROOT / "_offline_site" / "oc_segments"
SOURCE_PAYLOAD = REPO_ROOT / "data" / "serving" / "oraclechambers" / "oc_local_site_hydration_v1.json"

PLANES = [
    {
        "site_name": "oc-macro-heatmap-local",
        "domain": "MACRO_HEATMAP",
        "title": "Macro Heatmap Plane",
        "subtitle": "Cross-asset visual regime heatmap across FX, RATES, C_FLOW, EQUITIES_INDEX, and EQUITIES_SECTOR.",
        "mode": "heatmap",
    },
    {
        "site_name": "oc-regime-timeline-local",
        "domain": "REGIME_TIMELINE",
        "title": "Regime Timeline Plane",
        "subtitle": "Visual timeline of current regime posture, historical analogs, and transition pressure.",
        "mode": "timeline",
    },
    {
        "site_name": "oc-contradiction-matrix-local",
        "domain": "CONTRADICTION_MATRIX",
        "title": "Contradiction Matrix Plane",
        "subtitle": "Visual matrix of cross-domain disagreement, instability, and confirmation gaps.",
        "mode": "matrix",
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
.heatmap{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}
.tile{border-radius:14px;padding:18px;border:1px solid #334155;background:#172554}
.tile.warn{background:#422006}
.tile.risk{background:#450a0a}
.timeline{display:flex;gap:12px;align-items:stretch}
.node{flex:1;border-radius:14px;padding:18px;border:1px solid #334155;background:#0f172a}
.node.active{background:#1e293b;border-color:#f59e0b}
.matrix{display:grid;grid-template-columns:160px repeat(5,1fr);gap:8px}
.cell{padding:12px;border:1px solid #334155;border-radius:10px;background:#0f172a;text-align:center}
.cell.header{background:#1e293b;color:#cbd5e1}
.cell.hot{background:#7f1d1d}
.cell.mid{background:#78350f}
.cell.low{background:#064e3b}
"""


def build_js() -> str:
    return """async function loadVisualPlane(){
const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r=>r.json());
const sitePayload = payload.site_payload || {};
const headline = sitePayload.headline || {};
const narrative = sitePayload.narrative || {};
const historical = sitePayload.historical_memory || {};

document.getElementById("regime").textContent = headline.regime || "Unknown";
document.getElementById("confidence").textContent = headline.confidence ?? "Unknown";
document.getElementById("gate").textContent = "Online transition blocked";

const visual = document.getElementById("visual");
const mode = window.OC_PLANE_MODE;

if(mode === "heatmap"){
  const domains = [
    ["FX","risk","Liquidity stress"],
    ["RATES","risk","Duration pressure"],
    ["C_FLOW","warn","Mixed inflation"],
    ["EQUITIES_INDEX","warn","Breadth incomplete"],
    ["EQUITIES_SECTOR","warn","Rotation unstable"]
  ];
  visual.className = "heatmap";
  visual.innerHTML = domains.map(d =>
    `<div class="tile ${d[1]}"><h3>${d[0]}</h3><p>${d[2]}</p></div>`
  ).join("");
}

if(mode === "timeline"){
  const nodes = [
    ["1970s Inflation Shock","Prior analog"],
    ["1998 FX Contagion","Top match"],
    ["2008 Liquidity Stress","Stress reference"],
    ["2022 Tightening Cycle","Policy reference"],
    [headline.regime || "Current Regime","Current"]
  ];
  visual.className = "timeline";
  visual.innerHTML = nodes.map(n =>
    `<div class="node ${n[1] === "Current" ? "active" : ""}"><h3>${n[0]}</h3><p>${n[1]}</p></div>`
  ).join("");
}

if(mode === "matrix"){

  const domains = [
    "FX",
    "RATES",
    "C_FLOW",
    "EQ_INDEX",
    "EQ_SECTOR"
  ];

  const values = [
    ["-",   "",    "",    "",    ""],
    ["hot", "-",   "",    "",    ""],
    ["mid", "mid", "-",   "",    ""],
    ["mid", "hot", "mid", "-",   ""],
    ["mid", "mid", "mid", "mid", "-"]
  ];

  visual.className = "matrix";

  let html =
    `<div class="cell header"></div>` +
    domains.map(d =>
      `<div class="cell header">${d}</div>`
    ).join("");

  for(let i = 0; i < domains.length; i++){

    html += `<div class="cell header">${domains[i]}</div>`;

    for(let j = 0; j < domains.length; j++){

      const value = values[i][j];

      if(value === ""){
        html += `<div class="cell"></div>`;
      }

      else if(value === "-"){
        html += `<div class="cell header">•</div>`;
      }

      else{
        html += `<div class="cell ${value}">${value}</div>`;
      }
    }
  }

  visual.innerHTML = html;
}

document.getElementById("output").textContent = JSON.stringify({
  visual_plane: window.OC_DOMAIN,
  executive_summary: narrative.executive_summary,
  regime: headline.regime,
  confidence: headline.confidence,
  top_historical_match: historical.top_match,
  interpretation:
    "Visual intelligence plane converts governed cognition into executive-readable visual structure while keeping online transition blocked."
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

    return {"site_name": plane["site_name"], "domain": plane["domain"], "ready": True, "path": str(site_dir)}


def main() -> None:
    payload = load_payload()
    results = [build_plane(plane, payload) for plane in PLANES]

    print(json.dumps({
        "artifact": "build_phase_d_visual_planes_1_3_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "site_count": len(results),
        "all_sites_ready": all(r["ready"] for r in results),
        "results": results,
    }, indent=2))


if __name__ == "__main__":
    main()

