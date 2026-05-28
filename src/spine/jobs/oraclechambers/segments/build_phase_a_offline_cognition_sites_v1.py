from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()
OFFLINE_ROOT = REPO_ROOT / "_offline_site" / "oc_segments"
SOURCE_PAYLOAD = REPO_ROOT / "data" / "serving" / "oraclechambers" / "oc_local_site_hydration_v1.json"

PHASE_A_SITES = [
    ("oc-rbl-local", "RBL Executive Interpretation", "Renders executive RBL summary, regime explanation, & decision bias."),
    ("oc-contradiction-local", "Contradiction Cognition", "Renders fragmentation, disagreement posture, & cross-asset conflict."),
    ("oc-historical-memory-local", "Historical Memory", "Renders historical analogs, top match, & similarity structure."),
    ("oc-final-metric-local", "Final Metric", "Renders confidence, conviction, deployability, & macro temperature."),
    ("oc-attention-routing-local", "Attention Routing", "Renders executive focus priority & escalation logic."),
    ("oc-governance-local", "Governance Intelligence", "Renders provenance, AI boundary, offline gate, & source governance."),
]


def read_payload() -> dict:
    if not SOURCE_PAYLOAD.exists():
        raise FileNotFoundError(f"Missing payload: {SOURCE_PAYLOAD}")
    return json.loads(SOURCE_PAYLOAD.read_text(encoding="utf-8"))


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def index_html(title: str, description: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>
  <main class="shell">
    <header class="hero">
      <div class="eyebrow">ORACLECHAMBERS | PHASE A OFFLINE COGNITION</div>
      <h1>{title}</h1>
      <p>{description}</p>
    </header>

    <section class="grid">
      <article class="card">
        <h2>Status</h2>
        <p id="status">Loading...</p>
      </article>
      <article class="card">
        <h2>Regime</h2>
        <p id="regime">Loading...</p>
      </article>
      <article class="card">
        <h2>Gate</h2>
        <p id="gate">Loading...</p>
      </article>
    </section>

    <section class="card wide">
      <h2>Cognition Output</h2>
      <pre id="output">Loading...</pre>
    </section>
  </main>

  <script src="./app.js"></script>
</body>
</html>
"""


def styles_css() -> str:
    return """body {
  margin: 0;
  background: #020617;
  color: #e5e7eb;
  font-family: Arial, sans-serif;
}
.shell { padding: 32px; }
.hero {
  border: 1px solid #334155;
  border-radius: 18px;
  padding: 24px;
  background: #0f172a;
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
.card {
  border: 1px solid #334155;
  border-radius: 18px;
  padding: 20px;
  background: #111827;
}
.wide { margin-top: 16px; }
pre {
  white-space: pre-wrap;
  color: #cbd5e1;
  line-height: 1.5;
}
"""


def app_js(site_name: str) -> str:
    return f"""async function loadSite() {{
  const manifest = await fetch("./manifest.json").then(r => r.json());
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {{}};
  const headline = sitePayload.headline || {{}};
  const narrative = sitePayload.narrative || {{}};
  const historical = sitePayload.historical_memory || {{}};
  const routing = payload.routing || {{}};
  const provenance = payload.provenance || {{}};

  document.getElementById("status").textContent =
    manifest.offline_site_ready ? "Offline cognition site ready" : "Not ready";

  document.getElementById("regime").textContent =
    headline.regime || "Unknown regime";

  document.getElementById("gate").textContent =
    manifest.online_transition_allowed
      ? "Online transition allowed"
      : "Online transition blocked";

  let output = {{}};

  if (manifest.site_name === "oc-rbl-local") {{
    output = {{
      layer: "RBL Executive Interpretation",
      rbl_summary: narrative.rbl_summary,
      executive_summary: narrative.executive_summary,
      risk_posture: headline.risk_posture,
      decision_bias: headline.decision_bias
    }};
  }}

  if (manifest.site_name === "oc-contradiction-local") {{
    output = {{
      layer: "Contradiction Cognition",
      regime: headline.regime,
      contradiction_posture: headline.regime && headline.regime.includes("Fragmented")
        ? "Fragmented cross-asset disagreement detected"
        : "No elevated contradiction detected",
      confidence: headline.confidence,
      governance_note: "Contradiction is rendered as governed cognition, not AI-owned truth."
    }};
  }}

  if (manifest.site_name === "oc-historical-memory-local") {{
    output = {{
      layer: "Historical Memory",
      top_match: historical.top_match,
      matches: historical.matches
    }};
  }}

  if (manifest.site_name === "oc-final-metric-local") {{
    output = {{
      layer: "Final Metric",
      confidence: headline.confidence,
      conviction: headline.conviction,
      macro_temperature: headline.macro_temperature,
      risk_posture: headline.risk_posture,
      decision_bias: headline.decision_bias
    }};
  }}

  if (manifest.site_name === "oc-attention-routing-local") {{
    output = {{
      layer: "Executive Attention Routing",
      focus_panel: sitePayload.dashboard?.header?.focus_panel,
      dashboard_mode: sitePayload.dashboard?.header?.dashboard_mode,
      attention_route: headline.regime && headline.regime.includes("Fragmented")
        ? "Prioritize contradiction and historical memory review"
        : "Prioritize RBL and final metric review"
    }};
  }}

  if (manifest.site_name === "oc-governance-local") {{
    output = {{
      layer: "Governance Intelligence",
      offline_first: routing.offline_first,
      frontend_hydration_ready: routing.frontend_hydration_ready,
      executive_dashboard_ready: routing.executive_dashboard_ready,
      ai_dependency: routing.ai_dependency,
      provenance: provenance
    }};
  }}

  document.getElementById("output").textContent =
    JSON.stringify(output, null, 2);
}}

loadSite().catch(error => {{
  document.getElementById("output").textContent =
    "Site load failed: " + error.message;
}});
"""


def build_site(site_name: str, title: str, description: str, payload: dict) -> dict:
    site_dir = OFFLINE_ROOT / site_name
    payload_dir = site_dir / "payloads"

    manifest = {
        "artifact": "phase_a_offline_cognition_site_manifest_v1",
        "site_name": site_name,
        "title": title,
        "description": description,
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_site_ready": True,
        "online_transition_allowed": False,
        "phase": "PHASE_A_CORE_COGNITION_RENDERING",
    }

    write(site_dir / "index.html", index_html(title, description))
    write(site_dir / "styles.css", styles_css())
    write(site_dir / "app.js", app_js(site_name))
    write(site_dir / "manifest.json", json.dumps(manifest, indent=2))
    write(payload_dir / "oc_local_site_hydration_v1.json", json.dumps(payload, indent=2))

    return {
        "site_name": site_name,
        "path": str(site_dir),
        "ready": True,
    }


def main() -> None:
    payload = read_payload()
    results = [build_site(*site, payload) for site in PHASE_A_SITES]

    print(json.dumps({
        "artifact": "build_phase_a_offline_cognition_sites_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "site_count": len(results),
        "all_sites_ready": all(item["ready"] for item in results),
        "results": results,
    }, indent=2))


if __name__ == "__main__":
    main()

    