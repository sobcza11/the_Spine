async function loadPlane(){

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
