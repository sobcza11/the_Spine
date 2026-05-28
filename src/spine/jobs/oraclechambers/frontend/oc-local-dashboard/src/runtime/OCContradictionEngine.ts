import type { OCHydrationPayload } from "../types/ocHydrationTypes";

export interface OCContradictionState {
  active: boolean;
  severity: number;
  label: string;
  summary: string;
}

export function evaluateContradictionState(
  payload: OCHydrationPayload
): OCContradictionState {
  const regime =
    payload.regime?.headline_regime ??
    payload.regime?.regime_label ??
    "";

  const displayMode =
    payload.runtime?.display_mode ?? "";

  const confidence =
    payload.metrics?.confidence ?? 0;

  const fragmented =
    regime.toLowerCase().includes("fragmented") ||
    displayMode.toLowerCase().includes("fragmented");

  const highConfidenceFragmentation =
    fragmented && confidence >= 0.75;

  if (highConfidenceFragmentation) {
    return {
      active: true,
      severity: 0.85,
      label: "High-Confidence Fragmentation",
      summary:
        "System detects a confident but fragmented cross-asset regime. Executive review should prioritize disagreement, liquidity fractures & unstable confirmation signals.",
    };
  }

  if (fragmented) {
    return {
      active: true,
      severity: 0.65,
      label: "Fragmented Regime",
      summary:
        "Cross-asset cognition indicates regime fragmentation. Monitor domain disagreement before increasing conviction.",
    };
  }

  return {
    active: false,
    severity: 0.15,
    label: "No Major Contradiction",
    summary:
      "No elevated contradiction state detected from current hydration payload.",
  };
}

