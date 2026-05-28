async function loadAIComponents(){
  const payload = await fetch(window.OC_PAYLOAD).then(r => r.json());

  const zt = payload.zt || {};
  const rbl = payload.rbl || {};
  const zeitgeist = zt.zeitgeist || {};
  const interpretation = rbl.interpretation || {};

  document.getElementById("zt-summary").innerHTML = `
    <div><span class="badge">${zt.plane || "UNKNOWN"}</span><span class="badge">${zt.component || "Z_t"}</span></div>
    <p><strong>Label:</strong> ${zt.label || "N/A"}</p>
    <p><strong>Regime:</strong> ${zeitgeist.regime || "N/A"}</p>
    <p><strong>Confidence:</strong> ${zeitgeist.confidence ?? "N/A"}</p>
    <p><strong>Conviction:</strong> ${zeitgeist.conviction ?? "N/A"}</p>
  `;

  document.getElementById("rbl-summary").innerHTML = `
    <div><span class="badge">${rbl.plane || "UNKNOWN"}</span><span class="badge">${rbl.component || "RBL"}</span></div>
    <p><strong>Label:</strong> ${rbl.label || "N/A"}</p>
    <p><strong>Risk Posture:</strong> ${interpretation.risk_posture || "N/A"}</p>
    <p><strong>Decision Bias:</strong> ${interpretation.decision_bias || "N/A"}</p>
    <p><strong>Summary:</strong> ${interpretation.summary || "N/A"}</p>
  `;

  document.getElementById("zt-json").textContent = JSON.stringify(zt, null, 2);
  document.getElementById("rbl-json").textContent = JSON.stringify(rbl, null, 2);
}

loadAIComponents().catch(error => {
  document.getElementById("zt-json").textContent = "Viewer failed: " + error.message;
  document.getElementById("rbl-json").textContent = "Viewer failed: " + error.message;
});
