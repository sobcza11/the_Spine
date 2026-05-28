async function loadVisualPlane(){
const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r=>r.json());
const sitePayload = payload.site_payload || {};
const headline = sitePayload.headline || {};
const narrative = sitePayload.narrative || {};
const historical = sitePayload.historical_memory || {};

document.getElementById("regime").textContent = headline.regime || "Unknown";
document.getElementById("confidence").textContent = headline.confidence ?? "Unknown";
document.getElementById("gate").textContent = "Online transition blocked";

const visual = document.getElementById("visual");
const mode = window.OC_PLANE_MODE;

if(mode === "heatmap"){
  const domains = [
    ["FX","risk","Liquidity stress"],
    ["RATES","risk","Duration pressure"],
    ["C_FLOW","warn","Mixed inflation"],
    ["EQUITIES_INDEX","warn","Breadth incomplete"],
    ["EQUITIES_SECTOR","warn","Rotation unstable"]
  ];
  visual.className = "heatmap";
  visual.innerHTML = domains.map(d =>
    `<div class="tile ${d[1]}"><h3>${d[0]}</h3><p>${d[2]}</p></div>`
  ).join("");
}

if(mode === "timeline"){
  const nodes = [
    ["1970s Inflation Shock","Prior analog"],
    ["1998 FX Contagion","Top match"],
    ["2008 Liquidity Stress","Stress reference"],
    ["2022 Tightening Cycle","Policy reference"],
    [headline.regime || "Current Regime","Current"]
  ];
  visual.className = "timeline";
  visual.innerHTML = nodes.map(n =>
    `<div class="node ${n[1] === "Current" ? "active" : ""}"><h3>${n[0]}</h3><p>${n[1]}</p></div>`
  ).join("");
}

if(mode === "matrix"){

  const domains = [
    "FX",
    "RATES",
    "C_FLOW",
    "EQ_INDEX",
    "EQ_SECTOR"
  ];

  const values = [
    ["-",   "",    "",    "",    ""],
    ["hot", "-",   "",    "",    ""],
    ["mid", "mid", "-",   "",    ""],
    ["mid", "hot", "mid", "-",   ""],
    ["mid", "mid", "mid", "mid", "-"]
  ];

  visual.className = "matrix";

  let html =
    `<div class="cell header"></div>` +
    domains.map(d =>
      `<div class="cell header">${d}</div>`
    ).join("");

  for(let i = 0; i < domains.length; i++){

    html += `<div class="cell header">${domains[i]}</div>`;

    for(let j = 0; j < domains.length; j++){

      const value = values[i][j];

      if(value === ""){
        html += `<div class="cell"></div>`;
      }

      else if(value === "-"){
        html += `<div class="cell header">•</div>`;
      }

      else{
        html += `<div class="cell ${value}">${value}</div>`;
      }
    }
  }

  visual.innerHTML = html;
}

document.getElementById("output").textContent = JSON.stringify({
  visual_plane: window.OC_DOMAIN,
  executive_summary: narrative.executive_summary,
  regime: headline.regime,
  confidence: headline.confidence,
  top_historical_match: historical.top_match,
  interpretation:
    "Visual intelligence plane converts governed cognition into executive-readable visual structure while keeping online transition blocked."
}, null, 2);
}

loadVisualPlane().catch(error=>{
document.getElementById("output").textContent = "Visual plane failed: " + error.message;
});
