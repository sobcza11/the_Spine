from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-narrative-drift-local"
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


payload = json.loads(
    SOURCE_PAYLOAD.read_text(encoding="utf-8")
)

manifest = {
    "artifact": "build_phase_c_narrative_drift_plane_v1",
    "site_name": "oc-narrative-drift-local",
    "domain": "NARRATIVE_DRIFT",
    "offline_site_ready": True,
    "online_transition_allowed": False,
    "run_ts": datetime.now(timezone.utc).isoformat(),
}

index_html = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Narrative Drift Plane</title>
<link rel="stylesheet" href="./styles.css">
</head>
<body>
<main class="shell">

<header class="hero">
<div class="eyebrow">
ORACLECHAMBERS | NARRATIVE DRIFT COGNITION
</div>

<h1>Narrative Drift & Semantic Regime Cognition</h1>

<p>
Tracks semantic drift,
regime-language instability,
macro narrative fragmentation,
& executive communication pressure.
</p>
</header>

<section class="grid">
<article class="card">
<h2>Narrative Regime</h2>
<p id="regime">Loading...</p>
</article>

<article class="card">
<h2>Drift Pressure</h2>
<p id="drift">Loading...</p>
</article>

<article class="card">
<h2>Semantic Stability</h2>
<p id="semantic">Loading...</p>
</article>
</section>

<section class="card wide">
<h2>Narrative Drift Layer</h2>
<pre id="output">Loading...</pre>
</section>

<script src="./app.js"></script>

</main>
</body>
</html>
"""

styles_css = """body{
margin:0;
background:#020617;
color:#e5e7eb;
font-family:Arial,sans-serif;
}
.shell{padding:32px;}
.hero{
background:#0f172a;
border:1px solid #334155;
border-radius:18px;
padding:24px;
margin-bottom:24px;
}
.eyebrow{
font-size:12px;
letter-spacing:.12em;
color:#94a3b8;
}
.grid{
display:grid;
grid-template-columns:repeat(3,1fr);
gap:16px;
}
.card{
background:#111827;
border:1px solid #334155;
border-radius:18px;
padding:20px;
}
.wide{margin-top:16px;}
pre{
white-space:pre-wrap;
line-height:1.5;
}
"""

app_js = """async function loadPlane(){

const payload = await fetch(
"./payloads/oc_local_site_hydration_v1.json"
).then(r=>r.json());

const headline = payload.site_payload.headline || {};
const narrative = payload.site_payload.narrative || {};

document.getElementById("regime").textContent =
headline.regime || "Unknown";

document.getElementById("drift").textContent =
"Moderate narrative fragmentation";

document.getElementById("semantic").textContent =
"Semantic coherence weakening";

document.getElementById("output").textContent =
JSON.stringify({
executive_summary:narrative.executive_summary,
drift_state:"Elevated",
narrative_fragmentation:true,
communication_pressure:"Moderate",
interpretation:
"Narrative cognition suggests macro language is becoming increasingly fragmented across policy, growth, and liquidity channels."
},null,2);

}

loadPlane();
"""

write(SITE_DIR / "index.html", index_html)
write(SITE_DIR / "styles.css", styles_css)
write(SITE_DIR / "app.js", app_js)

write(
    SITE_DIR / "manifest.json",
    json.dumps(manifest, indent=2)
)

write(
    SITE_DIR / "payloads" / "oc_local_site_hydration_v1.json",
    json.dumps(payload, indent=2)
)

print(json.dumps({
    "artifact":"build_phase_c_narrative_drift_plane_v1",
    "offline_site_ready":True,
    "site_path":str(SITE_DIR),
}, indent=2))

