import type {
  OCHydrationPayload,
} from "../types/ocHydrationTypes";

interface OCExecutivePanelsProps {
  payload: OCHydrationPayload;
}

function PanelCard({
  title,
  label,
  value,
}: {
  title: string;
  label: string;
  value: string;
}) {
  return (
    <section style={{
      border: "1px solid #334155",
      borderRadius: "16px",
      padding: "18px",
      background: "#0f172a"
    }}>
      <div style={{
        color: "#94a3b8",
        fontSize: "12px",
        textTransform: "uppercase",
        letterSpacing: "0.08em"
      }}>
        {label}
      </div>

      <h2 style={{
        marginTop: "8px",
        marginBottom: "12px",
        fontSize: "20px"
      }}>
        {title}
      </h2>

      <p style={{
        color: "#cbd5e1",
        lineHeight: "1.6"
      }}>
        {value}
      </p>
    </section>
  );
}

export default function OCExecutivePanels({
  payload,
}: OCExecutivePanelsProps) {
  return (
    <section style={{
      display: "grid",
      gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
      gap: "16px",
      marginTop: "24px"
    }}>
      <PanelCard
        label="RBL"
        title="Read Between The Lines"
        value={
          payload.regime?.headline_regime
          ?? payload.regime?.regime_label
          ?? "Awaiting institutional interpretation."
        }
      />

      <PanelCard
        label="Zₜ"
        title="Zeitgeist State"
        value={
          payload.runtime?.display_mode
          ?? payload.runtime?.runtime_mode
          ?? "Awaiting market tone state."
        }
      />

      <PanelCard
        label="Final Metric"
        title="Deployment Confidence"
        value={
          payload.metrics?.confidence !== undefined
            ? `Confidence: ${payload.metrics.confidence}`
            : "Awaiting final metric state."
        }
      />
    </section>
  );
}
