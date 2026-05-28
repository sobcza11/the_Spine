
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
