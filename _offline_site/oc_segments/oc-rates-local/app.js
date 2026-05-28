async function loadRatesPlane() {
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("rates-regime").textContent =
    headline.regime || "Unknown";

  document.getElementById("curve-stress").textContent =
    headline.regime && headline.regime.includes("Fragmented")
      ? "Curve stress elevated"
      : "Curve stress contained";

  document.getElementById("duration-instability").textContent =
    headline.confidence > 0.8
      ? "Duration instability active"
      : "Duration instability muted";

  document.getElementById("policy-divergence").textContent =
    JSON.stringify({
      fed_policy_pressure: "Restrictive",
      china_policy_pressure: "Liquidity-sensitive",
      ecb_policy_pressure: "Moderately restrictive",
      divergence_state: "Elevated",
      interpretation: "Rates cognition flags cross-policy divergence as a core regime input."
    }, null, 2);

  document.getElementById("rates-contradiction").textContent =
    JSON.stringify({
      contradiction_score: 0.48,
      cross_asset_signal: "Rates, FX, and equity signals partially misaligned",
      curve_warning: "Moderate",
      systemic_duration_pressure: true
    }, null, 2);

  document.getElementById("rates-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "Rates cognition indicates fragmented duration posture with policy divergence and curve stress requiring executive attention.",
      deployment_bias:
        "Avoid overconfidence in broad risk posture until duration pressure and curve stress stabilize.",
      institutional_takeaway:
        "Monitor curve stress, real yield pressure, China policy pressure, and Fed/liquidity divergence."
    }, null, 2);
}

loadRatesPlane().catch(error => {
  document.getElementById("rates-rbl").textContent =
    "Rates plane failed: " + error.message;
});
