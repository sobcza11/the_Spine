async function loadEquitiesSectorPlane() {
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("sector-regime").textContent =
    headline.regime || "Unknown";

  document.getElementById("rotation-tone").textContent =
    headline.regime && headline.regime.includes("Fragmented")
      ? "Rotation unstable"
      : "Rotation orderly";

  document.getElementById("internal-breadth").textContent =
    headline.risk_posture === "Constructive"
      ? "Constructive but uneven internal breadth"
      : "Defensive internal breadth";

  document.getElementById("leadership-map").textContent =
    JSON.stringify({
      leading_groups: ["Technology", "Industrials", "Financials"],
      lagging_groups: ["Utilities", "Staples", "Real Estate"],
      leadership_concentration: "Moderate-to-elevated",
      interpretation: "Sector cognition flags uneven leadership rather than broad uniform participation."
    }, null, 2);

  document.getElementById("sector-dispersion").textContent =
    JSON.stringify({
      sector_breadth: "Mixed",
      rotation_instability: "Elevated",
      dispersion_state: "Moderate",
      defensive_cyclical_balance: "Cyclical tilt with defensive undercurrent",
      earnings_regime_tone: "Constructive but fragile"
    }, null, 2);

  document.getElementById("sector-rbl").textContent =
    JSON.stringify({
      executive_summary: narrative.executive_summary,
      interpretation:
        "EQUITIES_SECTOR cognition suggests internal market structure is constructive but uneven, with rotation instability and leadership concentration requiring monitoring.",
      deployment_bias:
        "Avoid treating index strength as broad confirmation until sector breadth and dispersion stabilize.",
      institutional_takeaway:
        "Monitor leadership concentration, cyclical/defensive balance, sector breadth, dispersion, and earnings tone."
    }, null, 2);
}

loadEquitiesSectorPlane().catch(error => {
  document.getElementById("sector-rbl").textContent =
    "EQUITIES_SECTOR plane failed: " + error.message;
});
