async function loadSegment() {
  const manifest = await fetch("./manifest.json").then((r) => r.json());
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then((r) => r.json());

  document.getElementById("segment-status").textContent =
    manifest.offline_site_ready ? "Offline segment ready" : "Offline segment not ready";

  document.getElementById("online-mirror").textContent =
    manifest.online_mirror;

  document.getElementById("deployment-gate").textContent =
    manifest.online_transition_allowed
      ? "Online transition allowed"
      : "Online transition blocked pending validation";

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("payload-summary").textContent = JSON.stringify({
    deployment_ready: payload.deployment_ready,
    site_mode: sitePayload.site_mode,
    runtime_mode: sitePayload.runtime_mode,
    regime: headline.regime,
    confidence: headline.confidence,
    conviction: headline.conviction,
    macro_temperature: headline.macro_temperature,
    executive_summary: narrative.executive_summary
  }, null, 2);
}

loadSegment().catch((error) => {
  document.getElementById("payload-summary").textContent =
    "Segment load failed: " + error.message;
});
