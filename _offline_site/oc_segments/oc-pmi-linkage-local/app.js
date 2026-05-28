
async function loadPlane(){

const payload = await fetch(
"./payloads/oc_local_site_hydration_v1.json"
).then(r=>r.json());

const narrative = payload.site_payload.narrative || {};

document.getElementById("growth").textContent =
"Moderating growth";

document.getElementById("manufacturing").textContent =
"Manufacturing pressure elevated";

document.getElementById("services").textContent =
"Services resilience intact";

document.getElementById("output").textContent =
JSON.stringify({
executive_summary:narrative.executive_summary,
growth_signal:"Moderating",
manufacturing_state:"Weakening",
services_state:"Stable",
inflation_linkage:"Mixed",
interpretation:
"PMI cognition indicates uneven macro demand conditions with manufacturing weakening faster than services."
},null,2);

}

loadPlane();
