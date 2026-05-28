import type { OCHydrationPayload } from "../types/ocHydrationTypes";

interface OCOverlayPanelsProps {
  payload: OCHydrationPayload;
}

export default function OCOverlayPanels({ payload }: OCOverlayPanelsProps) {
  const contradictionActive =
    payload.runtime?.display_mode?.includes("fragmented") ||
    payload.regime?.headline_regime?.includes("Fragmented") ||
    false;

  return (
    <section style={{
      marginTop: "24px",
      display: "grid",
      gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
      gap: "16px"
    }}>
      <section style={{
        border: "1px solid #475569",
        borderRadius: "16px",
        padding: "18px",
        background: "#0f172a"
      }}>
        <h2>Contradiction Overlay</h2>
        <p style={{ color: "#cbd5e1" }}>
          {contradictionActive
            ? "Cross-asset regime disagreement detected."
            : "No elevated contradiction state detected."}
        </p>
      </section>

      <section style={{
        border: "1px solid #475569",
        borderRadius: "16px",
        padding: "18px",
        background: "#0f172a"
      }}>
        <h2>Historical Memory</h2>
        <p style={{ color: "#cbd5e1" }}>
          {payload.regime?.headline_regime
            ?? payload.regime?.regime_label
            ?? "Historical analog awaiting payload state."}
        </p>
      </section>

      <section style={{
        border: "1px solid #475569",
        borderRadius: "16px",
        padding: "18px",
        background: "#0f172a"
      }}>
        <h2>CB Divergence</h2>
        <p style={{ color: "#cbd5e1" }}>
          Embedded central bank cognition reserved for FX, Rates, C_FLOW & Equities.
        </p>
      </section>
    </section>
  );
}

