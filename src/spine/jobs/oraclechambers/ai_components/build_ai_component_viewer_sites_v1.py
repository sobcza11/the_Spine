from pathlib import Path
import json
from datetime import datetime, timezone


REPO_ROOT = Path(__file__).resolve().parents[5]
VIEWER_ROOT = REPO_ROOT / "_offline_site" / "oc_ai_components"

VIEWERS = [
    {
        "site": "fx",
        "title": "FX AI Components",
        "subtitle": "Zₜ • FX zeitgeist + RBL • FX • Interpretation",
        "payload": "fx_ai_components_v1.json",
    },
    {
        "site": "rates",
        "title": "RATES AI Components",
        "subtitle": "Zₜ • Bond Market • zeitgeist + RBL • Bond Mrkt • Interpretation",
        "payload": "rates_ai_components_v1.json",
    },
    {
        "site": "c_flow",
        "title": "C_FLOW AI Components",
        "subtitle": "Zₜ • Commodity Flow • zeitgeist + RBL • Commodities • Interpretation",
        "payload": "cflow_ai_components_v1.json",
    },
    {
        "site": "equities_sector",
        "title": "EQUITIES SECTOR AI Components",
        "subtitle": "Zₜ • Equity Sector • zeitgeist + RBL • Equity Sectors • Interpretation",
        "payload": "equities_sector_ai_components_v1.json",
    },
    {
        "site": "equities_industry",
        "title": "EQUITIES INDUSTRY AI Components",
        "subtitle": "Zₜ • Equity Industry • zeitgeist + RBL • Equity Mrkt • Interpretation",
        "payload": "equities_industry_ai_components_v1.json",
    },
]


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_html(viewer: dict) -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{viewer["title"]}</title>
  <link rel="stylesheet" href="./styles.css">
</head>
<body>
  <main class="shell">
    <header class="hero">
      <div class="eyebrow">ORACLECHAMBERS | CORE AI COMPONENT REVIEW</div>
      <h1>{viewer["title"]}</h1>
      <p>{viewer["subtitle"]}</p>
      <a class="back" href="../index.html">← Back to AI Component Index</a>
    </header>

    <section class="grid">
      <article class="card">
        <h2>Z_t Zeitgeist</h2>
        <div id="zt-summary" class="summary">Loading...</div>
        <pre id="zt-json">Loading...</pre>
      </article>

      <article class="card">
        <h2>RBL Interpretation</h2>
        <div id="rbl-summary" class="summary">Loading...</div>
        <pre id="rbl-json">Loading...</pre>
      </article>
    </section>

    <section class="card wide">
      <h2>Review Notes</h2>
      <p>
        Edit the source Python Z_t or RBL artifact, rerun the exporter, then refresh this page.
        This viewer is for output inspection only; it does not mutate runtime truth.
      </p>
    </section>

    <script>
      window.OC_PAYLOAD = "./payloads/{viewer["payload"]}";
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
h1{margin:8px 0 6px 0}
p{color:#cbd5e1;line-height:1.5}
.back{display:inline-block;margin-top:10px;color:#93c5fd;text-decoration:none}
.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:16px}
.card{background:#111827;border:1px solid #334155;border-radius:18px;padding:20px}
.wide{margin-top:16px}
.summary{background:#0f172a;border:1px solid #334155;border-radius:12px;padding:14px;margin-bottom:14px;line-height:1.5;color:#dbeafe}
pre{white-space:pre-wrap;line-height:1.45;color:#cbd5e1;font-size:13px}
.badge{display:inline-block;padding:4px 8px;border-radius:999px;background:#1e293b;border:1px solid #334155;margin-right:6px;color:#bfdbfe}
"""


def build_js() -> str:
    return """async function loadAIComponents(){
  const payload = await fetch(window.OC_PAYLOAD).then(r => r.json());

  const zt = payload.zt || {};
  const rbl = payload.rbl || {};
  const zeitgeist = zt.zeitgeist || {};
  const interpretation = rbl.interpretation || {};

  document.getElementById("zt-summary").innerHTML = `
    <div><span class="badge">${zt.plane || "UNKNOWN"}</span><span class="badge">${zt.component || "Z_t"}</span></div>
    <p><strong>Label:</strong> ${zt.label || "N/A"}</p>
    <p><strong>Regime:</strong> ${zeitgeist.regime || "N/A"}</p>
    <p><strong>Confidence:</strong> ${zeitgeist.confidence ?? "N/A"}</p>
    <p><strong>Conviction:</strong> ${zeitgeist.conviction ?? "N/A"}</p>
  `;

  document.getElementById("rbl-summary").innerHTML = `
    <div><span class="badge">${rbl.plane || "UNKNOWN"}</span><span class="badge">${rbl.component || "RBL"}</span></div>
    <p><strong>Label:</strong> ${rbl.label || "N/A"}</p>
    <p><strong>Risk Posture:</strong> ${interpretation.risk_posture || "N/A"}</p>
    <p><strong>Decision Bias:</strong> ${interpretation.decision_bias || "N/A"}</p>
    <p><strong>Summary:</strong> ${interpretation.summary || "N/A"}</p>
  `;

  document.getElementById("zt-json").textContent = JSON.stringify(zt, null, 2);
  document.getElementById("rbl-json").textContent = JSON.stringify(rbl, null, 2);
}

loadAIComponents().catch(error => {
  document.getElementById("zt-json").textContent = "Viewer failed: " + error.message;
  document.getElementById("rbl-json").textContent = "Viewer failed: " + error.message;
});
"""


def build_index_html() -> str:
    links = "\n".join(
        f"""<a class="tile" href="./{viewer["site"]}/index.html">
  <h2>{viewer["title"]}</h2>
  <p>{viewer["subtitle"]}</p>
</a>"""
        for viewer in VIEWERS
    )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>OracleChambers AI Component Index</title>
  <link rel="stylesheet" href="./index.css">
</head>
<body>
  <main class="shell">
    <header class="hero">
      <div class="eyebrow">ORACLECHAMBERS | CORE AI COMPONENTS</div>
      <h1>AI Component Review Index</h1>
      <p>Review Z_t + RBL outputs across FX, RATES, C_FLOW, EQUITIES_SECTOR, and EQUITIES_INDUSTRY.</p>
    </header>

    <section class="grid">
      {links}
    </section>
  </main>
</body>
</html>
"""


def build_index_css() -> str:
    return """body{margin:0;background:#020617;color:#e5e7eb;font-family:Arial,sans-serif}
.shell{padding:32px}
.hero{background:#0f172a;border:1px solid #334155;border-radius:18px;padding:24px;margin-bottom:24px}
.eyebrow{font-size:12px;letter-spacing:.12em;color:#94a3b8;text-transform:uppercase}
.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:16px}
.tile{display:block;background:#111827;border:1px solid #334155;border-radius:18px;padding:20px;text-decoration:none;color:#e5e7eb}
.tile:hover{border-color:#93c5fd}
.tile p{color:#cbd5e1;line-height:1.5}
"""


def main() -> None:
    results = []

    for viewer in VIEWERS:
        site_dir = VIEWER_ROOT / viewer["site"]

        write(site_dir / "index.html", build_html(viewer))
        write(site_dir / "styles.css", build_css())
        write(site_dir / "app.js", build_js())

        manifest = {
            "artifact": "oc_ai_component_viewer_manifest_v1",
            "site": viewer["site"],
            "title": viewer["title"],
            "payload": viewer["payload"],
            "run_ts": datetime.now(timezone.utc).isoformat(),
            "viewer_ready": True,
            "online_transition_allowed": False,
        }

        write(site_dir / "manifest.json", json.dumps(manifest, indent=2))

        results.append({
            "site": viewer["site"],
            "viewer_ready": True,
            "path": str(site_dir),
        })

    write(VIEWER_ROOT / "index.html", build_index_html())
    write(VIEWER_ROOT / "index.css", build_index_css())

    print(json.dumps({
        "artifact": "build_ai_component_viewer_sites_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "viewer_count": len(results),
        "all_viewers_ready": True,
        "index_path": str(VIEWER_ROOT / "index.html"),
        "results": results,
    }, indent=2))


if __name__ == "__main__":
    main()

