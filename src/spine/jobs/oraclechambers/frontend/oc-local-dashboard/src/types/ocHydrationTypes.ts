export interface OCRuntimeState {
  deployment_ready: boolean;
  deployment_mode: string;
  runtime_mode?: string;
  refresh_interval_seconds?: number;
  focus_panel?: string;
  priority_panel?: string;
  display_mode?: string;
}

export interface OCRegimeState {
  regime_label?: string;
  headline_regime?: string;
  confidence?: number;
  tone_direction?: string;
}

export interface OCMetricState {
  confidence?: number;
  conviction?: number;
  deployability?: number;
  instability_risk?: number;
  macro_temperature?: string;
}

export interface OCPanelState {
  id: string;
  title: string;
  domain?: string;
  summary?: string;
}

export interface OCOverlayState {
  id: string;
  title: string;
  active: boolean;
  summary?: string;
}

export interface OCHydrationPayload {
  deployment_ready: boolean;
  site_mode?: string;
  runtime?: OCRuntimeState;
  regime?: OCRegimeState;
  metrics?: OCMetricState;
  panels?: OCPanelState[];
  overlays?: OCOverlayState[];
  raw?: Record<string, unknown>;
}
