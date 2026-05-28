
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
