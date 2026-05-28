import type { OCHydrationPayload } from "../types/ocHydrationTypes";

export interface OCHistoricalRegimeState {
  label: string;
  similarityScore: number;
  summary: string;
}

export function evaluateHistoricalRegimeState(
  payload: OCHydrationPayload
): OCHistoricalRegimeState {
  const raw = payload.raw ?? {};

  const topMatch =
    String(raw.top_match ?? raw.historical_top_match ?? "");

  const similarity =
    Number(raw.similarity_score ?? raw.historical_similarity_score ?? 0);

  if (topMatch) {
    return {
      label: topMatch,
      similarityScore: similarity,
      summary:
        `Current macro cognition most closely resembles ${topMatch}. Use as historical context, not deterministic forecast.`,
    };
  }

  const regime =
    payload.regime?.headline_regime ??
    payload.regime?.regime_label ??
    "Unknown Regime";

  return {
    label: regime,
    similarityScore: similarity,
    summary:
      "Historical regime memory is active but no explicit analog match was found in the current hydration payload.",
  };
}

