from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-cflow-local"
)

SOURCE_PAYLOAD = (
    REPO_ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oc_local_site_hydration_v1.json"
)


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def load_payload() -> dict:
    return json.loads(SOURCE_PAYLOAD.read_text(encoding="utf-8"))


def build_index_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OracleChambers C_FLOW Plane</title>
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>
<main class="shell">
  <header class="hero">
    <div class="eyebrow">ORACLECHAMBERS | C_FLOW COGNITION PLANE</div>
    <h1>Commodity Flow & Inflation Pressure Cognition</h1>
    <p>Institutional commodity-flow interpretation, inflation pressure, energy stress, metals/agriculture divergence & real-economy demand cognition.</p>
  </header>

  <section class="grid">
    <article class="card"><h2>C_FLOW Regime</h2><p id="cflow-regime">Loading...</p></article>
    <article class="card"><h2>Inflation Pressure</h2><p id="inflation-pressure">Loading...</p></article>
    <article class="card"><h2>Energy Stress</h2><p id="energy-stress">Loading...</p></article>
  </section>

  <section class="grid secondary">
    <article class="card"><h2>Metals / Agriculture Divergence</h2><pre id="commodity-divergence">Loading...</pre></article>
    <article class="card"><h2>C_FLOW Contradiction Layer</h2><pre id="cflow-contradiction">Loading...</pre></article>
  </section>

  <section class="card wide">
    <h2>Executive Commodity Flow Interpretation</h2>
    <pre id="cflow-rbl">Loading...</pre>
  </section>
</main>
<script src="./app.js"></script>
</body>
</html>
"""


def build_styles_css() -> str:
    return """body {
  margin: 0;
  background: #020617;
  color: #e5e7eb;
  font-family: Arial, sans-serif;
}
.shell { padding: 32px; }
.hero {
  background: #0f172a;
  border: 1px solid #334155;
  border-radius: 18px;
  padding: 24px;
  margin-bottom: 24px;
}
.eyebrow {
  color: #94a3b8;
  font-size: 12px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}
.grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 16px;
}
.secondary {
  grid-template-columns: repeat(2, minmax(0, 1fr));
  margin-top: 16px;
}
.card {
  background: #111827;
  border: 1px solid #334155;
  border-radius: 18px;
  padding: 20px;
}
.wide { margin-top: 16px; }
pre {
  white-space: pre-wrap;
  line-height: 1.5;
  color: #cbd5e1;
}
"""


def build_app_js() -> str:
    return """async function loadCFlowPlane() {
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("cflow-regime").textContent =
    headline.regime || "Unknown";

  document.getElementById("inflation-pressure").textContent =
    headline.macro_temperature === "HOT"
      ? "High inflation pressure"
      : headline.regime && headline.regime.includes("Fragmented")
        ? "Mixed inflation pressure"
        : "Contained inflation pressure";

  document.getElementById("energy-stress").textContent =
    headline.confidence > 0.8
      ? "Energy stress monitoring active"
      : "Energy stress muted";

  document.getElementById("commodity-divergence").textContent =
    JSON.stringify({
      energy: "Elevated monitoring",
      metals: "Divergence watch",
      agriculture: "Supply-demand watch",
      divergence_state: "Moderate",
      interpretation: "Commodity flow cognition flags non-uniform inflation transmission."
    }, null, 2);

  document.getElementById("cflow-contradiction").textContent =
    JSON.stringify({
      contradiction_score: 0.39,
      cross_asset_signal: "Commodity pressure and macro temperature partially diverge",
      inflation_warning: "Moderate",
      real_economy_flow_stress: true
    }, null, 2);

  document.getElementById("cflow-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "C_FLOW cognition suggests commodity transmission is fragmented, with inflation pressure not yet uniformly confirmed across real-economy flow channels.",
      deployment_bias:
        "Treat commodity pressure as a monitoring priority rather than a standalone regime override.",
      institutional_takeaway:
        "Monitor WTI, energy stress, metals/agriculture divergence, and supply-chain language before increasing inflation conviction."
    }, null, 2);
}

loadCFlowPlane().catch(error => {
  document.getElementById("cflow-rbl").textContent =
    "C_FLOW plane failed: " + error.message;
});
"""


def main() -> None:
    payload = load_payload()

    manifest = {
        "artifact": "build_phase_b_cflow_plane_v1",
        "site_name": "oc-cflow-local",
        "title": "OracleChambers C_FLOW Plane",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_site_ready": True,
        "online_transition_allowed": False,
        "domain": "C_FLOW",
        "plane": "institutional_commodity_flow_cognition",
    }

    write(SITE_DIR / "index.html", build_index_html())
    write(SITE_DIR / "styles.css", build_styles_css())
    write(SITE_DIR / "app.js", build_app_js())
    write(SITE_DIR / "manifest.json", json.dumps(manifest, indent=2))
    write(SITE_DIR / "payloads" / "oc_local_site_hydration_v1.json", json.dumps(payload, indent=2))

    print(json.dumps({
        "artifact": "build_phase_b_cflow_plane_v1",
        "offline_site_ready": True,
        "site_path": str(SITE_DIR),
    }, indent=2))


if __name__ == "__main__":
    main()

    