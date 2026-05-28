import { useEffect, useState } from "react";

import type {
  OCHydrationPayload,
} from "../types/ocHydrationTypes";

import {
  loadHydrationPayload,
} from "../services/hydrationService";

export interface OCRuntimeStore {
  payload: OCHydrationPayload | null;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useOCRuntimeStore(): OCRuntimeStore {
  const [payload, setPayload] =
    useState<OCHydrationPayload | null>(null);

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState<string | null>(null);

  async function refresh() {
    try {
      setLoading(true);
      setError(null);

      const nextPayload =
        await loadHydrationPayload();

      setPayload(nextPayload);
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Unknown OracleChambers runtime error"
      );
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
  }, []);

  return {
    payload,
    loading,
    error,
    refresh,
  };
}

