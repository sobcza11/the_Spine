import {
  isOCHydrationPayload,
  type OCHydrationPayload,
} from "../types/ocHydrationTypes";

const DEFAULT_HYDRATION_PATH =
  "/data/serving/oraclechambers/oc_local_site_hydration_v1.json";

export async function loadOCHydrationPayload(
  path: string = DEFAULT_HYDRATION_PATH
): Promise<OCHydrationPayload> {
  const response = await fetch(path, {
    method: "GET",
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`OC hydration payload fetch failed: ${response.status}`);
  }

  const json = await response.json();

  const normalizedPayload: OCHydrationPayload = {
    deployment_ready: Boolean(json.deployment_ready),
    site_mode: json.site_mode ?? "offline_first",
    runtime: {
      deployment_ready: Boolean(json.deployment_ready),
      deployment_mode: json.deployment_mode ?? json.site_mode ?? "offline_first",
      runtime_mode: json.runtime_mode,
      refresh_interval_seconds: json.refresh_interval_seconds,
      focus_panel: json.focus_panel,
      priority_panel: json.priority_panel,
      display_mode: json.display_mode,
    },
    regime: {
      headline_regime: json.regime ?? json.headline_regime,
      regime_label: json.regime_label,
      confidence: json.confidence ?? json.headline_confidence,
      tone_direction: json.tone_direction,
      dominance: json.dominance,
    },
    metrics: {
      confidence: json.confidence ?? json.headline_confidence,
      conviction: json.conviction,
      deployability: json.deployability,
      instability_risk: json.instability_risk,
      macro_temperature: json.macro_temperature,
    },
    panels: json.panels ?? [],
    overlays: json.overlays ?? [],
    raw: json,
  };

  if (!isOCHydrationPayload(normalizedPayload)) {
    throw new Error("Invalid OC hydration payload contract.");
  }

  return normalizedPayload;
}
