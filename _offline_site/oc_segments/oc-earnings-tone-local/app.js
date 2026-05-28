
async function loadPlane(){

const payload = await fetch(
"./payloads/oc_local_site_hydration_v1.json"
).then(r=>r.json());

const narrative = payload.site_payload.narrative || {};

document.getElementById("tone").textContent =
"Cautiously constructive";

document.getElementById("guidance").textContent =
"Forward guidance weakening";

document.getElementById("margin").textContent =
"Moderate margin pressure";

document.getElementById("output").textContent =
JSON.stringify({
executive_summary:narrative.executive_summary,
earnings_regime:"Constructive but fragile",
corporate_stress:"Moderate",
guidance_quality:"Declining",
interpretation:
"Corporate cognition indicates management tone remains constructive, but forward guidance stability is weakening beneath the surface."
},null,2);

}

loadPlane();
