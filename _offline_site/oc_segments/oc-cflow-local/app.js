async function loadCFlowPlane() {
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("cflow-regime").textContent =
    headline.regime || "Unknown";

  document.getElementById("inflation-pressure").textContent =
    headline.macro_temperature === "HOT"
      ? "High inflation pressure"
      : headline.regime && headline.regime.includes("Fragmented")
        ? "Mixed inflation pressure"
        : "Contained inflation pressure";

  document.getElementById("energy-stress").textContent =
    headline.confidence > 0.8
      ? "Energy stress monitoring active"
      : "Energy stress muted";

  document.getElementById("commodity-divergence").textContent =
    JSON.stringify({
      energy: "Elevated monitoring",
      metals: "Divergence watch",
      agriculture: "Supply-demand watch",
      divergence_state: "Moderate",
      interpretation: "Commodity flow cognition flags non-uniform inflation transmission."
    }, null, 2);

  document.getElementById("cflow-contradiction").textContent =
    JSON.stringify({
      contradiction_score: 0.39,
      cross_asset_signal: "Commodity pressure and macro temperature partially diverge",
      inflation_warning: "Moderate",
      real_economy_flow_stress: true
    }, null, 2);

  document.getElementById("cflow-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "C_FLOW cognition suggests commodity transmission is fragmented, with inflation pressure not yet uniformly confirmed across real-economy flow channels.",
      deployment_bias:
        "Treat commodity pressure as a monitoring priority rather than a standalone regime override.",
      institutional_takeaway:
        "Monitor WTI, energy stress, metals/agriculture divergence, and supply-chain language before increasing inflation conviction."
    }, null, 2);
}

loadCFlowPlane().catch(error => {
  document.getElementById("cflow-rbl").textContent =
    "C_FLOW plane failed: " + error.message;
});
