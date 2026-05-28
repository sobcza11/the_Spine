from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-fedspeak-local"
)

SOURCE_PAYLOAD = (
    REPO_ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oc_local_site_hydration_v1.json"
)

payload = json.loads(
    SOURCE_PAYLOAD.read_text(encoding="utf-8")
)

manifest = {
    "artifact":"build_phase_c_fedspeak_plane_v1",
    "site_name":"oc-fedspeak-local",
    "domain":"FEDSPEAK",
    "offline_site_ready":True,
    "online_transition_allowed":False,
    "run_ts":datetime.now(timezone.utc).isoformat(),
}

SITE_DIR.mkdir(parents=True, exist_ok=True)
(SITE_DIR / "payloads").mkdir(parents=True, exist_ok=True)

(SITE_DIR / "index.html").write_text("""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Fedspeak Plane</title>
<link rel="stylesheet" href="./styles.css">
</head>
<body>

<main class="shell">

<header class="hero">
<div class="eyebrow">
ORACLECHAMBERS | FEDSPEAK COGNITION
</div>

<h1>Federal Reserve Semantic Cognition</h1>

<p>
Tracks central-bank language,
policy tone drift,
hawkish/dovish instability,
& semantic policy pressure.
</p>

</header>

<section class="grid">

<article class="card">
<h2>Fed Tone</h2>
<p id="tone">Loading...</p>
</article>

<article class="card">
<h2>Policy Pressure</h2>
<p id="pressure">Loading...</p>
</article>

<article class="card">
<h2>Semantic Drift</h2>
<p id="drift">Loading...</p>
</article>

</section>

<section class="card wide">
<h2>Fed Semantic Interpretation</h2>
<pre id="output">Loading...</pre>
</section>

<script src="./app.js"></script>

</main>

</body>
</html>
""", encoding="utf-8")

(SITE_DIR / "styles.css").write_text("""
body{
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
""", encoding="utf-8")

(SITE_DIR / "app.js").write_text("""
async function loadPlane(){

const payload = await fetch(
"./payloads/oc_local_site_hydration_v1.json"
).then(r=>r.json());

const narrative = payload.site_payload.narrative || {};

document.getElementById("tone").textContent =
"Moderately hawkish";

document.getElementById("pressure").textContent =
"Restrictive liquidity posture";

document.getElementById("drift").textContent =
"Fed language becoming less synchronized";

document.getElementById("output").textContent =
JSON.stringify({
executive_summary:narrative.executive_summary,
hawkishness:"Moderate",
policy_instability:"Elevated",
communication_alignment:"Weakening",
interpretation:
"Fed semantic cognition indicates restrictive language remains dominant but internal coherence is deteriorating."
},null,2);

}

loadPlane();
""", encoding="utf-8")

(SITE_DIR / "manifest.json").write_text(
    json.dumps(manifest, indent=2),
    encoding="utf-8"
)

(SITE_DIR / "payloads" / "oc_local_site_hydration_v1.json").write_text(
    json.dumps(payload, indent=2),
    encoding="utf-8"
)

print(json.dumps({
    "artifact":"build_phase_c_fedspeak_plane_v1",
    "offline_site_ready":True,
    "site_path":str(SITE_DIR),
}, indent=2))

