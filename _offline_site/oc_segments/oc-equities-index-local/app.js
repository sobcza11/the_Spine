async function loadEquitiesIndexPlane() {
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("equity-regime").textContent =
    headline.regime || "Unknown";

  document.getElementById("beta-posture").textContent =
    headline.risk_posture === "Constructive"
      ? "Constructive but fragmented beta posture"
      : "Defensive beta posture";

  document.getElementById("breadth-condition").textContent =
    headline.regime && headline.regime.includes("Fragmented")
      ? "Breadth confirmation incomplete"
      : "Breadth confirmation stable";

  document.getElementById("leadership-concentration").textContent =
    JSON.stringify({
      mega_cap_leadership: "Elevated watch",
      equal_weight_confirmation: "Incomplete",
      participation_state: "Narrow-to-mixed",
      interpretation: "Equity index cognition flags possible leadership concentration beneath constructive headline posture."
    }, null, 2);

  document.getElementById("equity-contradiction").textContent =
    JSON.stringify({
      contradiction_score: 0.44,
      cross_asset_signal: "Constructive equity posture conflicts with rates and FX fragmentation",
      breadth_warning: "Moderate",
      systemic_market_tone: "Constructive but unstable"
    }, null, 2);

  document.getElementById("equity-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "EQUITIES_INDEX cognition suggests headline risk appetite remains constructive, but breadth and cross-asset confirmation are incomplete.",
      deployment_bias:
        "Treat equity strength as conditional until breadth, rates, and FX confirmation improve.",
      institutional_takeaway:
        "Monitor index breadth, leadership concentration, equal-weight confirmation, and equity/rates contradiction."
    }, null, 2);
}

loadEquitiesIndexPlane().catch(error => {
  document.getElementById("equity-rbl").textContent =
    "EQUITIES_INDEX plane failed: " + error.message;
});
