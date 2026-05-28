import type { OCHydrationPayload } from "../types/ocHydrationTypes";
import { evaluateContradictionState } from "./OCContradictionEngine";
import { evaluateHistoricalRegimeState } from "./OCHistoricalRegimeEngine";

export interface OCAttentionRoute {
  focusPanel: string;
  priority: "low" | "medium" | "high";
  escalation: boolean;
  summary: string;
}

export function evaluateAttentionRoute(
  payload: OCHydrationPayload
): OCAttentionRoute {
  const contradiction = evaluateContradictionState(payload);
  const historical = evaluateHistoricalRegimeState(payload);

  const confidence = payload.metrics?.confidence ?? 0;
  const conviction = payload.metrics?.conviction ?? 0;

  if (contradiction.active && contradiction.severity >= 0.75) {
    return {
      focusPanel: "contradiction_overlay",
      priority: "high",
      escalation: true,
      summary:
        "Attention routed to contradiction overlay due to elevated cross-asset disagreement.",
    };
  }

  if (historical.similarityScore >= 0.6) {
    return {
      focusPanel: "historical_memory_panel",
      priority: "medium",
      escalation: false,
      summary:
        `Attention routed to historical memory due to strong analog match: ${historical.label}.`,
    };
  }

  if (confidence >= 0.85 && conviction >= 0.6) {
    return {
      focusPanel: "rbl_panel",
      priority: "medium",
      escalation: false,
      summary:
        "Attention routed to RBL panel due to high-confidence governed synthesis.",
    };
  }

  return {
    focusPanel: "final_metric_panel",
    priority: "low",
    escalation: false,
    summary:
      "Attention routed to final metric panel pending stronger regime confirmation.",
  };
}