from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-equities-index-local"
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
  <title>OracleChambers EQUITIES_INDEX Plane</title>
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>
<main class="shell">
  <header class="hero">
    <div class="eyebrow">ORACLECHAMBERS | EQUITIES INDEX COGNITION PLANE</div>
    <h1>Equity Index, Breadth & Beta Cognition</h1>
    <p>Institutional equity-index interpretation, broad beta posture, breadth participation, leadership concentration & systemic market tone.</p>
  </header>

  <section class="grid">
    <article class="card"><h2>Equity Regime</h2><p id="equity-regime">Loading...</p></article>
    <article class="card"><h2>Broad Beta Posture</h2><p id="beta-posture">Loading...</p></article>
    <article class="card"><h2>Breadth Condition</h2><p id="breadth-condition">Loading...</p></article>
  </section>

  <section class="grid secondary">
    <article class="card"><h2>Leadership Concentration</h2><pre id="leadership-concentration">Loading...</pre></article>
    <article class="card"><h2>Equity / Rates Contradiction</h2><pre id="equity-contradiction">Loading...</pre></article>
  </section>

  <section class="card wide">
    <h2>Executive Equity Index Interpretation</h2>
    <pre id="equity-rbl">Loading...</pre>
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
    return """async function loadEquitiesIndexPlane() {
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("equity-regime").textContent =
    headline.regime || "Unknown";

  document.getElementById("beta-posture").textContent =
    headline.risk_posture === "Constructive"
      ? "Constructive but fragmented beta posture"
      : "Defensive beta posture";

  document.getElementById("breadth-condition").textContent =
    headline.regime && headline.regime.includes("Fragmented")
      ? "Breadth confirmation incomplete"
      : "Breadth confirmation stable";

  document.getElementById("leadership-concentration").textContent =
    JSON.stringify({
      mega_cap_leadership: "Elevated watch",
      equal_weight_confirmation: "Incomplete",
      participation_state: "Narrow-to-mixed",
      interpretation: "Equity index cognition flags possible leadership concentration beneath constructive headline posture."
    }, null, 2);

  document.getElementById("equity-contradiction").textContent =
    JSON.stringify({
      contradiction_score: 0.44,
      cross_asset_signal: "Constructive equity posture conflicts with rates and FX fragmentation",
      breadth_warning: "Moderate",
      systemic_market_tone: "Constructive but unstable"
    }, null, 2);

  document.getElementById("equity-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "EQUITIES_INDEX cognition suggests headline risk appetite remains constructive, but breadth and cross-asset confirmation are incomplete.",
      deployment_bias:
        "Treat equity strength as conditional until breadth, rates, and FX confirmation improve.",
      institutional_takeaway:
        "Monitor index breadth, leadership concentration, equal-weight confirmation, and equity/rates contradiction."
    }, null, 2);
}

loadEquitiesIndexPlane().catch(error => {
  document.getElementById("equity-rbl").textContent =
    "EQUITIES_INDEX plane failed: " + error.message;
});
"""


def main() -> None:
    payload = load_payload()

    manifest = {
        "artifact": "build_phase_b_equities_index_plane_v1",
        "site_name": "oc-equities-index-local",
        "title": "OracleChambers EQUITIES_INDEX Plane",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_site_ready": True,
        "online_transition_allowed": False,
        "domain": "EQUITIES_INDEX",
        "plane": "institutional_equity_index_cognition",
    }

    write(SITE_DIR / "index.html", build_index_html())
    write(SITE_DIR / "styles.css", build_styles_css())
    write(SITE_DIR / "app.js", build_app_js())
    write(SITE_DIR / "manifest.json", json.dumps(manifest, indent=2))
    write(SITE_DIR / "payloads" / "oc_local_site_hydration_v1.json", json.dumps(payload, indent=2))

    print(json.dumps({
        "artifact": "build_phase_b_equities_index_plane_v1",
        "offline_site_ready": True,
        "site_path": str(SITE_DIR),
    }, indent=2))


if __name__ == "__main__":
    main()

    