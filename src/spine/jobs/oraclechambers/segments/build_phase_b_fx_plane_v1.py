from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

OFFLINE_ROOT = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
)

SITE_DIR = OFFLINE_ROOT / "oc-fx-local"

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
    return json.loads(
        SOURCE_PAYLOAD.read_text(encoding="utf-8")
    )


def build_manifest() -> dict:
    return {
        "artifact": "build_phase_b_fx_plane_v1",
        "site_name": "oc-fx-local",
        "title": "OracleChambers FX Plane",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_site_ready": True,
        "online_transition_allowed": False,
        "domain": "FX",
        "plane": "institutional_fx_cognition",
    }


def build_index_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OracleChambers FX Plane</title>
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>

<main class="shell">

  <header class="hero">
    <div class="eyebrow">
      ORACLECHAMBERS | FX COGNITION PLANE
    </div>

    <h1>FX Liquidity & Cross-Border Cognition</h1>

    <p>
      Institutional FX regime interpretation,
      liquidity stress,
      carry instability,
      & central-bank divergence cognition.
    </p>
  </header>

  <section class="grid">

    <article class="card">
      <h2>FX Regime</h2>
      <p id="fx-regime">Loading...</p>
    </article>

    <article class="card">
      <h2>Dollar Pressure</h2>
      <p id="dollar-pressure">Loading...</p>
    </article>

    <article class="card">
      <h2>Carry Stress</h2>
      <p id="carry-stress">Loading...</p>
    </article>

  </section>

  <section class="grid secondary">

    <article class="card">
      <h2>CB Divergence</h2>
      <pre id="cb-divergence">Loading...</pre>
    </article>

    <article class="card">
      <h2>FX Contradiction Layer</h2>
      <pre id="fx-contradiction">Loading...</pre>
    </article>

  </section>

  <section class="card wide">
    <h2>Executive FX Interpretation</h2>
    <pre id="fx-rbl">Loading...</pre>
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

.shell {
  padding: 32px;
}

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

.wide {
  margin-top: 16px;
}

pre {
  white-space: pre-wrap;
  line-height: 1.5;
  color: #cbd5e1;
}
"""


def build_app_js() -> str:
    return """async function loadFXPlane() {

  const payload = await fetch(
    "./payloads/oc_local_site_hydration_v1.json"
  ).then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("fx-regime").textContent =
    headline.regime || "Unknown";

  const dollarPressure =
    headline.regime.includes("Fragmented")
      ? "Elevated Dollar Liquidity Stress"
      : "Contained";

  document.getElementById("dollar-pressure").textContent =
    dollarPressure;

  const carryStress =
    headline.confidence > 0.8
      ? "Carry instability elevated"
      : "Carry conditions stable";

  document.getElementById("carry-stress").textContent =
    carryStress;

  document.getElementById("cb-divergence").textContent =
    JSON.stringify({
      fed: "Restrictive",
      boj: "Ultra-accommodative",
      ecb: "Moderately restrictive",
      divergence_state: "Elevated"
    }, null, 2);

  document.getElementById("fx-contradiction").textContent =
    JSON.stringify({
      contradiction_score: 0.42,
      cross_asset_signal: "FX & rates disagreement elevated",
      liquidity_fragmentation: true,
      systemic_warning: "Moderate"
    }, null, 2);

  document.getElementById("fx-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "FX cognition suggests fragmented liquidity posture with elevated cross-border divergence and unstable carry conditions.",
      deployment_bias:
        "Prefer defensive FX positioning until contradiction pressure stabilizes.",
      institutional_takeaway:
        "Monitor USD funding stress, CB divergence, and cross-asset liquidity fractures closely."
    }, null, 2);
}

loadFXPlane().catch(error => {
  document.getElementById("fx-rbl").textContent =
    "FX plane failed: " + error.message;
});
"""


def main() -> None:

    payload = load_payload()

    write(
        SITE_DIR / "index.html",
        build_index_html(),
    )

    write(
        SITE_DIR / "styles.css",
        build_styles_css(),
    )

    write(
        SITE_DIR / "app.js",
        build_app_js(),
    )

    write(
        SITE_DIR / "manifest.json",
        json.dumps(build_manifest(), indent=2),
    )

    write(
        SITE_DIR / "payloads" / "oc_local_site_hydration_v1.json",
        json.dumps(payload, indent=2),
    )

    print(json.dumps({
        "artifact": "build_phase_b_fx_plane_v1",
        "offline_site_ready": True,
        "site_path": str(SITE_DIR),
    }, indent=2))


if __name__ == "__main__":
    main()

    