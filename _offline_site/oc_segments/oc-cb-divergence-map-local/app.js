async function loadVisualPlane(){
const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r=>r.json());
const sitePayload = payload.site_payload || {};
const headline = sitePayload.headline || {};
const narrative = sitePayload.narrative || {};
const historical = sitePayload.historical_memory || {};
const routing = payload.routing || {};

document.getElementById("regime").textContent = headline.regime || "Unknown";
document.getElementById("confidence").textContent = headline.confidence ?? "Unknown";
document.getElementById("gate").textContent = "Online transition blocked";

const visual = document.getElementById("visual");
const mode = window.OC_PLANE_MODE;

if(mode === "liquidity"){
  const nodes = [
    ["USD Funding","high","Elevated pressure"],
    ["Rates Liquidity","high","Duration instability"],
    ["FX Carry","mid","Carry stress active"],
    ["Energy Flow","mid","Monitoring active"],
    ["Equity Breadth","mid","Confirmation incomplete"]
  ];
  visual.className = "map";
  visual.innerHTML = nodes.map(n =>
    `<div class="tile ${n[1]}"><h3>${n[0]}</h3><p>${n[2]}</p></div>`
  ).join("");
}

if(mode === "cb"){
  const banks = [
    ["Fed","high","Restrictive"],
    ["ECB","mid","Moderately restrictive"],
    ["BoJ","low","Ultra-accommodative"],
    ["China","mid","Liquidity-sensitive"],
    ["Global Liquidity","high","Fragmented"]
  ];
  visual.className = "map";
  visual.innerHTML = banks.map(b =>
    `<div class="tile ${b[1]}"><h3>${b[0]}</h3><p>${b[2]}</p></div>`
  ).join("");
}

if(mode === "dashboard"){
  const panels = [
    ["RBL Focus", narrative.rbl_summary || "Awaiting RBL summary", "focus"],
    ["Regime", headline.regime || "Unknown", ""],
    ["Confidence", String(headline.confidence ?? "Unknown"), ""],
    ["Historical Match", historical.top_match?.historical_regime || "Unknown", ""],
    ["Deployment Gate", routing.online_runtime_ready ? "Online ready" : "Online blocked", "focus"],
    ["AI Dependency", routing.ai_dependency ? "AI dependency active" : "No AI dependency", ""]
  ];
  visual.className = "stack";
  visual.innerHTML = panels.map(p =>
    `<div class="panel ${p[2]}"><h3>${p[0]}</h3><p>${p[1]}</p></div>`
  ).join("");
}

document.getElementById("output").textContent = JSON.stringify({
  visual_plane: window.OC_DOMAIN,
  executive_summary: narrative.executive_summary,
  regime: headline.regime,
  confidence: headline.confidence,
  conviction: headline.conviction,
  macro_temperature: headline.macro_temperature,
  top_historical_match: historical.top_match,
  online_transition_allowed: false,
  interpretation:
    "Visual intelligence plane renders governed cognition as segmented offline executive intelligence before online transition."
}, null, 2);
}

loadVisualPlane().catch(error=>{
document.getElementById("output").textContent = "Visual plane failed: " + error.message;
});
