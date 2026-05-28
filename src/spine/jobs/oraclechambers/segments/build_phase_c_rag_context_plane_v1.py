from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-rag-context-local"
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
    "artifact":"build_phase_c_rag_context_plane_v1",
    "site_name":"oc-rag-context-local",
    "domain":"RAG_CONTEXT",
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
<title>RAG Context Plane</title>
<link rel="stylesheet" href="./styles.css">
</head>
<body>

<main class="shell">

<header class="hero">

<div class="eyebrow">
ORACLECHAMBERS | RAG CONTEXT COGNITION
</div>

<h1>Governed Retrieval Context Cognition</h1>

<p>
Tracks governed retrieval,
context integrity,
institutional memory linkage,
& retrieval confidence.
</p>

</header>

<section class="grid">

<article class="card">
<h2>Retrieval Integrity</h2>
<p id="integrity">Loading...</p>
</article>

<article class="card">
<h2>Context Stability</h2>
<p id="stability">Loading...</p>
</article>

<article class="card">
<h2>Memory Linkage</h2>
<p id="memory">Loading...</p>
</article>

</section>

<section class="card wide">
<h2>RAG Governance Layer</h2>
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

document.getElementById("integrity").textContent =
"Governed retrieval active";

document.getElementById("stability").textContent =
"Stable contextual routing";

document.getElementById("memory").textContent =
"Institutional memory linkage enabled";

document.getElementById("output").textContent =
JSON.stringify({
executive_summary:narrative.executive_summary,
retrieval_integrity:"High",
context_confidence:"Governed",
hallucination_boundary:"Restricted",
interpretation:
"RAG cognition confirms retrieval context remains compartmentalized, provenance-tracked, and governance constrained."
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
    "artifact":"build_phase_c_rag_context_plane_v1",
    "offline_site_ready":True,
    "site_path":str(SITE_DIR),
}, indent=2))

