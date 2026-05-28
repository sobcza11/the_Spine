async function loadFXPlane() {

  const payload = await fetch(
    "./payloads/oc_local_site_hydration_v1.json"
  ).then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("fx-regime").textContent =
    headline.regime || "Unknown";

  const dollarPressure =
    headline.regime.includes("Fragmented")
      ? "Elevated Dollar Liquidity Stress"
      : "Contained";

  document.getElementById("dollar-pressure").textContent =
    dollarPressure;

  const carryStress =
    headline.confidence > 0.8
      ? "Carry instability elevated"
      : "Carry conditions stable";

  document.getElementById("carry-stress").textContent =
    carryStress;

  document.getElementById("cb-divergence").textContent =
    JSON.stringify({
      fed: "Restrictive",
      boj: "Ultra-accommodative",
      ecb: "Moderately restrictive",
      divergence_state: "Elevated"
    }, null, 2);

  document.getElementById("fx-contradiction").textContent =
    JSON.stringify({
      contradiction_score: 0.42,
      cross_asset_signal: "FX & rates disagreement elevated",
      liquidity_fragmentation: true,
      systemic_warning: "Moderate"
    }, null, 2);

  document.getElementById("fx-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "FX cognition suggests fragmented liquidity posture with elevated cross-border divergence and unstable carry conditions.",
      deployment_bias:
        "Prefer defensive FX positioning until contradiction pressure stabilizes.",
      institutional_takeaway:
        "Monitor USD funding stress, CB divergence, and cross-asset liquidity fractures closely."
    }, null, 2);
}

loadFXPlane().catch(error => {
  document.getElementById("fx-rbl").textContent =
    "FX plane failed: " + error.message;
});
