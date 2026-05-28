from pathlib import Path
import json
from datetime import datetime, timezone


REPO_ROOT = Path(__file__).resolve().parents[5]
ROOT = REPO_ROOT / "_offline_site" / "oc_ai_components"

VIEWERS = [
    ("fx", "FX", "fx_ai_components_v1.json"),
    ("rates", "RATES", "rates_ai_components_v1.json"),
    ("c_flow", "C_FLOW", "cflow_ai_components_v1.json"),
    ("equities_sector", "EQUITIES SECTOR", "equities_sector_ai_components_v1.json"),
    ("equities_industry", "EQUITIES INDUSTRY", "equities_industry_ai_components_v1.json"),
]


def write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def css() -> str:
    return """body{margin:0;background:#020617;color:#e5e7eb;font-family:Arial,sans-serif}
.shell{padding:28px}.hero{background:#0f172a;border:1px solid #334155;border-radius:18px;padding:24px;margin-bottom:20px}
.eyebrow{font-size:12px;letter-spacing:.12em;color:#94a3b8;text-transform:uppercase}h1{margin:8px 0}p{color:#cbd5e1}
.grid{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}.card{background:#111827;border:1px solid #334155;border-radius:16px;padding:16px}
.card h2{font-size:16px;margin:0 0 10px}.score{font-size:28px;font-weight:bold;color:#bfdbfe}.label{color:#94a3b8;font-size:12px}
.section{margin-top:16px;background:#111827;border:1px solid #334155;border-radius:18px;padding:20px}
table{width:100%;border-collapse:collapse}td,th{border-bottom:1px solid #334155;padding:10px;text-align:left}th{color:#93c5fd}
.badge{display:inline-block;padding:4px 8px;border:1px solid #334155;border-radius:999px;background:#1e293b;color:#bfdbfe}
"""


def js() -> str:
    return """const PLANES = [
  ["fx","FX","fx_ai_components_v1.json"],
  ["rates","RATES","rates_ai_components_v1.json"],
  ["c_flow","C_FLOW","cflow_ai_components_v1.json"],
  ["equities_sector","EQUITIES SECTOR","equities_sector_ai_components_v1.json"],
  ["equities_industry","EQUITIES INDUSTRY","equities_industry_ai_components_v1.json"]
];

async function load(){
  const payloads = [];
  for(const [site,label,file] of PLANES){
    const payload = await fetch(`./${site}/payloads/${file}`).then(r=>r.json());
    payloads.push({site,label,payload});
  }

  document.getElementById("cards").innerHTML = payloads.map(x => {
    const a = x.payload.analytics || {};
    const d = x.payload.executive_decision_layer || {};
    return `<div class="card">
      <h2>${x.label}</h2>
      <div class="label">Zₜ Composite</div>
      <div class="score">${a.z_t_composite ?? "N/A"}</div>
      <p><span class="badge">${a.signal_strength || "N/A"}</span></p>
      <p>${d.action_bias || ""}</p>
    </div>`;
  }).join("");

  document.getElementById("analytics-table").innerHTML =
    `<tr><th>Plane</th><th>Zₜ</th><th>Stress</th><th>Dispersion</th><th>Contradiction</th><th>Decision</th></tr>` +
    payloads.map(x => {
      const a = x.payload.analytics || {};
      const d = x.payload.executive_decision_layer || {};
      return `<tr>
        <td>${x.label}</td>
        <td>${a.z_t_composite}</td>
        <td>${a.stress_score}</td>
        <td>${a.dispersion_score}</td>
        <td>${a.contradiction_score}</td>
        <td>${d.action_bias}</td>
      </tr>`;
    }).join("");

  document.getElementById("feature-table").innerHTML =
    `<tr><th>Plane</th><th>Top Feature</th><th>Weight</th><th>Direction</th></tr>` +
    payloads.map(x => {
      const f = (x.payload.feature_vector || [])[0] || {};
      return `<tr>
        <td>${x.label}</td>
        <td>${f.feature || "N/A"}</td>
        <td>${f.weight ?? "N/A"}</td>
        <td>${f.direction || "N/A"}</td>
      </tr>`;
    }).join("");

  document.getElementById("analog-table").innerHTML =
    `<tr><th>Plane</th><th>Top Analog</th><th>Similarity</th><th>Why</th></tr>` +
    payloads.map(x => {
      const h = (x.payload.historical_analogs || [])[0] || {};
      return `<tr>
        <td>${x.label}</td>
        <td>${h.regime || "N/A"}</td>
        <td>${h.similarity ?? "N/A"}</td>
        <td>${h.why || "N/A"}</td>
      </tr>`;
    }).join("");
}

load().catch(err => {
  document.body.innerHTML = "Dashboard failed: " + err.message;
});
"""


def html() -> str:
    return """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>OracleChambers Client Analytics Dashboard</title>
<link rel="stylesheet" href="./client_dashboard.css">
</head>
<body>
<main class="shell">
<header class="hero">
<div class="eyebrow">ORACLECHAMBERS | CLIENT-VISIBLE AI ANALYTICS</div>
<h1>Core Market Intelligence Dashboard</h1>
<p>Zₜ analytics, RBL decisions, feature drivers, contradiction scores & historical analogs across the five institutional planes.</p>
</header>

<section id="cards" class="grid"></section>

<section class="section">
<h2>Cross-Plane Analytics</h2>
<table id="analytics-table"></table>
</section>

<section class="section">
<h2>Top Feature Drivers</h2>
<table id="feature-table"></table>
</section>

<section class="section">
<h2>Historical Analogs</h2>
<table id="analog-table"></table>
</section>
</main>
<script src="./client_dashboard.js"></script>
</body>
</html>
"""


def main() -> None:
    write(ROOT / "client_dashboard.html", html())
    write(ROOT / "client_dashboard.css", css())
    write(ROOT / "client_dashboard.js", js())

    print(json.dumps({
        "artifact": "build_ai_component_client_dashboard_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "client_dashboard_ready": True,
        "path": str(ROOT / "client_dashboard.html"),
    }, indent=2))


if __name__ == "__main__":
    main()

