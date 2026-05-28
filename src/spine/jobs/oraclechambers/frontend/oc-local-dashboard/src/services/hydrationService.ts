import type { OCHydrationPayload } from "../types/ocHydrationTypes";

export async function loadHydrationPayload(): Promise<OCHydrationPayload> {

  const response = await fetch(
    "/oc_local_site_hydration_v1.json"
  );

  if (!response.ok) {
    throw new Error(
      `Hydration payload fetch failed: ${response.status}`
    );
  }

  const payload = await response.json();

  return {
    deployment_ready: Boolean(payload.deployment_ready),
    site_mode: payload.site_mode ?? "offline_first",

    runtime: {
      deployment_ready: Boolean(payload.deployment_ready),
      deployment_mode: payload.deployment_mode ?? "offline_first",
      runtime_mode: payload.runtime_mode,
      refresh_interval_seconds:
        payload.refresh_interval_seconds,
      focus_panel: payload.focus_panel,
      priority_panel: payload.priority_panel,
      display_mode: payload.display_mode,
    },

    regime: {
      headline_regime:
        payload.regime ?? payload.headline_regime,
      regime_label: payload.regime_label,
      confidence:
        payload.confidence ??
        payload.headline_confidence,
      tone_direction: payload.tone_direction,
    },

    metrics: {
      confidence:
        payload.confidence ??
        payload.headline_confidence,
      conviction: payload.conviction,
      deployability: payload.deployability,
      instability_risk: payload.instability_risk,
      macro_temperature: payload.macro_temperature,
    },

    panels: payload.panels ?? [],
    overlays: payload.overlays ?? [],
    raw: payload,
  };
}
