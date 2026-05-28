async function loadSite() {
  const manifest = await fetch("./manifest.json").then(r => r.json());
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then(r => r.json());

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};
  const historical = sitePayload.historical_memory || {};
  const routing = payload.routing || {};
  const provenance = payload.provenance || {};

  document.getElementById("status").textContent =
    manifest.offline_site_ready ? "Offline cognition site ready" : "Not ready";

  document.getElementById("regime").textContent =
    headline.regime || "Unknown regime";

  document.getElementById("gate").textContent =
    manifest.online_transition_allowed
      ? "Online transition allowed"
      : "Online transition blocked";

  let output = {};

  if (manifest.site_name === "oc-rbl-local") {
    output = {
      layer: "RBL Executive Interpretation",
      rbl_summary: narrative.rbl_summary,
      executive_summary: narrative.executive_summary,
      risk_posture: headline.risk_posture,
      decision_bias: headline.decision_bias
    };
  }

  if (manifest.site_name === "oc-contradiction-local") {
    output = {
      layer: "Contradiction Cognition",
      regime: headline.regime,
      contradiction_posture: headline.regime && headline.regime.includes("Fragmented")
        ? "Fragmented cross-asset disagreement detected"
        : "No elevated contradiction detected",
      confidence: headline.confidence,
      governance_note: "Contradiction is rendered as governed cognition, not AI-owned truth."
    };
  }

  if (manifest.site_name === "oc-historical-memory-local") {
    output = {
      layer: "Historical Memory",
      top_match: historical.top_match,
      matches: historical.matches
    };
  }

  if (manifest.site_name === "oc-final-metric-local") {
    output = {
      layer: "Final Metric",
      confidence: headline.confidence,
      conviction: headline.conviction,
      macro_temperature: headline.macro_temperature,
      risk_posture: headline.risk_posture,
      decision_bias: headline.decision_bias
    };
  }

  if (manifest.site_name === "oc-attention-routing-local") {
    output = {
      layer: "Executive Attention Routing",
      focus_panel: sitePayload.dashboard?.header?.focus_panel,
      dashboard_mode: sitePayload.dashboard?.header?.dashboard_mode,
      attention_route: headline.regime && headline.regime.includes("Fragmented")
        ? "Prioritize contradiction and historical memory review"
        : "Prioritize RBL and final metric review"
    };
  }

  if (manifest.site_name === "oc-governance-local") {
    output = {
      layer: "Governance Intelligence",
      offline_first: routing.offline_first,
      frontend_hydration_ready: routing.frontend_hydration_ready,
      executive_dashboard_ready: routing.executive_dashboard_ready,
      ai_dependency: routing.ai_dependency,
      provenance: provenance
    };
  }

  document.getElementById("output").textContent =
    JSON.stringify(output, null, 2);
}

loadSite().catch(error => {
  document.getElementById("output").textContent =
    "Site load failed: " + error.message;
});
