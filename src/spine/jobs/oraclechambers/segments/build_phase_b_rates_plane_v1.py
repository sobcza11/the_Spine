from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-rates-local"
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
  <title>OracleChambers RATES Plane</title>
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>
<main class="shell">
  <header class="hero">
    <div class="eyebrow">ORACLECHAMBERS | RATES COGNITION PLANE</div>
    <h1>Rates, Curve & Duration Cognition</h1>
    <p>Institutional rates interpretation, yield curve pressure, duration instability, real yield stress & policy divergence.</p>
  </header>

  <section class="grid">
    <article class="card"><h2>Rates Regime</h2><p id="rates-regime">Loading...</p></article>
    <article class="card"><h2>Curve Stress</h2><p id="curve-stress">Loading...</p></article>
    <article class="card"><h2>Duration Instability</h2><p id="duration-instability">Loading...</p></article>
  </section>

  <section class="grid secondary">
    <article class="card"><h2>Policy Divergence</h2><pre id="policy-divergence">Loading...</pre></article>
    <article class="card"><h2>Rates Contradiction Layer</h2><pre id="rates-contradiction">Loading...</pre></article>
  </section>

  <section class="card wide">
    <h2>Executive Rates Interpretation</h2>
    <pre id="rates-rbl">Loading...</pre>
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
    return """async function loadRatesPlane() {
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("rates-regime").textContent =
    headline.regime || "Unknown";

  document.getElementById("curve-stress").textContent =
    headline.regime && headline.regime.includes("Fragmented")
      ? "Curve stress elevated"
      : "Curve stress contained";

  document.getElementById("duration-instability").textContent =
    headline.confidence > 0.8
      ? "Duration instability active"
      : "Duration instability muted";

  document.getElementById("policy-divergence").textContent =
    JSON.stringify({
      fed_policy_pressure: "Restrictive",
      china_policy_pressure: "Liquidity-sensitive",
      ecb_policy_pressure: "Moderately restrictive",
      divergence_state: "Elevated",
      interpretation: "Rates cognition flags cross-policy divergence as a core regime input."
    }, null, 2);

  document.getElementById("rates-contradiction").textContent =
    JSON.stringify({
      contradiction_score: 0.48,
      cross_asset_signal: "Rates, FX, and equity signals partially misaligned",
      curve_warning: "Moderate",
      systemic_duration_pressure: true
    }, null, 2);

  document.getElementById("rates-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "Rates cognition indicates fragmented duration posture with policy divergence and curve stress requiring executive attention.",
      deployment_bias:
        "Avoid overconfidence in broad risk posture until duration pressure and curve stress stabilize.",
      institutional_takeaway:
        "Monitor curve stress, real yield pressure, China policy pressure, and Fed/liquidity divergence."
    }, null, 2);
}

loadRatesPlane().catch(error => {
  document.getElementById("rates-rbl").textContent =
    "Rates plane failed: " + error.message;
});
"""


def main() -> None:
    payload = load_payload()

    manifest = {
        "artifact": "build_phase_b_rates_plane_v1",
        "site_name": "oc-rates-local",
        "title": "OracleChambers RATES Plane",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_site_ready": True,
        "online_transition_allowed": False,
        "domain": "RATES",
        "plane": "institutional_rates_cognition",
    }

    write(SITE_DIR / "index.html", build_index_html())
    write(SITE_DIR / "styles.css", build_styles_css())
    write(SITE_DIR / "app.js", build_app_js())
    write(SITE_DIR / "manifest.json", json.dumps(manifest, indent=2))
    write(SITE_DIR / "payloads" / "oc_local_site_hydration_v1.json", json.dumps(payload, indent=2))

    print(json.dumps({
        "artifact": "build_phase_b_rates_plane_v1",
        "offline_site_ready": True,
        "site_path": str(SITE_DIR),
    }, indent=2))


if __name__ == "__main__":
    main()
    