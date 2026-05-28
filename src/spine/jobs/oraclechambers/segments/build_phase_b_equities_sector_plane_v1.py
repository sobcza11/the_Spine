from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-equities-sector-local"
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
  <title>OracleChambers EQUITIES_SECTOR Plane</title>
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>
<main class="shell">
  <header class="hero">
    <div class="eyebrow">ORACLECHAMBERS | EQUITIES SECTOR COGNITION PLANE</div>
    <h1>Sector Rotation & Internal Market Structure Cognition</h1>
    <p>Institutional sector interpretation, rotation instability, leadership/laggard structure, internal breadth & dispersion cognition.</p>
  </header>

  <section class="grid">
    <article class="card"><h2>Sector Regime</h2><p id="sector-regime">Loading...</p></article>
    <article class="card"><h2>Rotation Tone</h2><p id="rotation-tone">Loading...</p></article>
    <article class="card"><h2>Internal Breadth</h2><p id="internal-breadth">Loading...</p></article>
  </section>

  <section class="grid secondary">
    <article class="card"><h2>Leadership / Laggard Map</h2><pre id="leadership-map">Loading...</pre></article>
    <article class="card"><h2>Sector Dispersion Layer</h2><pre id="sector-dispersion">Loading...</pre></article>
  </section>

  <section class="card wide">
    <h2>Executive Sector Interpretation</h2>
    <pre id="sector-rbl">Loading...</pre>
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
    return """async function loadEquitiesSectorPlane() {
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("sector-regime").textContent =
    headline.regime || "Unknown";

  document.getElementById("rotation-tone").textContent =
    headline.regime && headline.regime.includes("Fragmented")
      ? "Rotation unstable"
      : "Rotation orderly";

  document.getElementById("internal-breadth").textContent =
    headline.risk_posture === "Constructive"
      ? "Constructive but uneven internal breadth"
      : "Defensive internal breadth";

  document.getElementById("leadership-map").textContent =
    JSON.stringify({
      leading_groups: ["Technology", "Industrials", "Financials"],
      lagging_groups: ["Utilities", "Staples", "Real Estate"],
      leadership_concentration: "Moderate-to-elevated",
      interpretation: "Sector cognition flags uneven leadership rather than broad uniform participation."
    }, null, 2);

  document.getElementById("sector-dispersion").textContent =
    JSON.stringify({
      sector_breadth: "Mixed",
      rotation_instability: "Elevated",
      dispersion_state: "Moderate",
      defensive_cyclical_balance: "Cyclical tilt with defensive undercurrent",
      earnings_regime_tone: "Constructive but fragile"
    }, null, 2);

  document.getElementById("sector-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "EQUITIES_SECTOR cognition suggests internal market structure is constructive but uneven, with rotation instability and leadership concentration requiring monitoring.",
      deployment_bias:
        "Avoid treating index strength as broad confirmation until sector breadth and dispersion stabilize.",
      institutional_takeaway:
        "Monitor leadership concentration, cyclical/defensive balance, sector breadth, dispersion, and earnings tone."
    }, null, 2);
}

loadEquitiesSectorPlane().catch(error => {
  document.getElementById("sector-rbl").textContent =
    "EQUITIES_SECTOR plane failed: " + error.message;
});
"""


def main() -> None:
    payload = load_payload()

    manifest = {
        "artifact": "build_phase_b_equities_sector_plane_v1",
        "site_name": "oc-equities-sector-local",
        "title": "OracleChambers EQUITIES_SECTOR Plane",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_site_ready": True,
        "online_transition_allowed": False,
        "domain": "EQUITIES_SECTOR",
        "plane": "institutional_equity_sector_cognition",
    }

    write(SITE_DIR / "index.html", build_index_html())
    write(SITE_DIR / "styles.css", build_styles_css())
    write(SITE_DIR / "app.js", build_app_js())
    write(SITE_DIR / "manifest.json", json.dumps(manifest, indent=2))
    write(SITE_DIR / "payloads" / "oc_local_site_hydration_v1.json", json.dumps(payload, indent=2))

    print(json.dumps({
        "artifact": "build_phase_b_equities_sector_plane_v1",
        "offline_site_ready": True,
        "site_path": str(SITE_DIR),
    }, indent=2))


if __name__ == "__main__":
    main()

    