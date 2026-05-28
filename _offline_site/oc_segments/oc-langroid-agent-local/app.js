
async function loadPlane(){

const payload = await fetch(
"./payloads/oc_local_site_hydration_v1.json"
).then(r=>r.json());

const narrative = payload.site_payload.narrative || {};

document.getElementById("status").textContent =
"Reserved / governed";

document.getElementById("boundary").textContent =
"AI may not own orchestration";

document.getElementById("scope").textContent =
"Retrieval & semantic assistance only";

document.getElementById("output").textContent =
JSON.stringify({
executive_summary:narrative.executive_summary,
agent_status:"Reserved",
routing_authority:"Prohibited",
governance_mode:"CPMAI bounded execution",
interpretation:
"Langroid cognition remains governance constrained and may assist retrieval, summarization, and semantic analysis only."
},null,2);

}

loadPlane();
