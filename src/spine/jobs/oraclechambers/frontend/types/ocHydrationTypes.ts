export type OCStatus =
  | "completed"
  | "partial"
  | "not_completed"
  | "in_progress"
  | "unknown";

export type OCDeploymentMode =
  | "offline_first"
  | "local_runtime"
  | "online_transition"
  | "live_runtime";

export type OCMacroTemperature =
  | "COLD"
  | "COOL"
  | "NEUTRAL"
  | "WARM"
  | "HOT"
  | string;

export type OCPanelId =
  | "rbl_panel"
  | "zt_panel"
  | "final_metric_panel"
  | "contradiction_overlay"
  | "historical_memory_overlay"
  | "domain_panel"
  | string;

export type OCDomain =
  | "FX"
  | "RATES"
  | "C_FLOW"
  | "EQUITIES_INDEX"
  | "EQUITIES_SECTOR"
  | "CB"
  | "CROSS_DOMAIN"
  | string;

export interface OCMetricState {
  confidence?: number;
  conviction?: number;
  deployability?: number;
  instability_risk?: number;
  macro_temperature?: OCMacroTemperature;
  [key: string]: unknown;
}

export interface OCRegimeState {
  regime_label?: string;
  headline_regime?: string;
  tone_direction?: string;
  confidence?: number;
  dominance?: number;
  [key: string]: unknown;
}

export interface OCPanelState {
  id: OCPanelId;
  title: string;
  domain?: OCDomain;
  status?: OCStatus;
  priority?: number;
  summary?: string;
  metrics?: OCMetricState;
  data?: Record<string, unknown>;
}

export interface OCOverlayState {
  id: OCPanelId;
  title: string;
  active: boolean;
  severity?: number;
  domain?: OCDomain;
  summary?: string;
  data?: Record<string, unknown>;
}

export interface OCRuntimeState {
  deployment_ready: boolean;
  deployment_mode: OCDeploymentMode | string;
  runtime_mode?: string;
  refresh_interval_seconds?: number;
  focus_panel?: OCPanelId;
  priority_panel?: OCPanelId;
  display_mode?: string;
}

export interface OCHydrationPayload {
  run_ts?: string;
  deployment_ready: boolean;
  site_mode?: string;
  runtime?: OCRuntimeState;
  regime?: OCRegimeState;
  metrics?: OCMetricState;
  panels?: OCPanelState[];
  overlays?: OCOverlayState[];
  raw?: Record<string, unknown>;
}

export function isOCHydrationPayload(value: unknown): value is OCHydrationPayload {
  if (!value || typeof value !== "object") return false;

  const payload = value as Partial<OCHydrationPayload>;

  return typeof payload.deployment_ready === "boolean";
}

