import type { OCHydrationPayload } from "../types/ocHydrationTypes";

interface OCDomainPanelsProps {
  payload: OCHydrationPayload;
}

const DOMAINS = [
  "FX",
  "RATES",
  "C_FLOW",
  "EQUITIES_INDEX",
  "EQUITIES_SECTOR",
];

export default function OCDomainPanels({ payload }: OCDomainPanelsProps) {
  return (
    <section style={{
      marginTop: "24px",
      display: "grid",
      gridTemplateColumns: "repeat(5, minmax(0, 1fr))",
      gap: "14px"
    }}>
      {DOMAINS.map((domain) => (
        <section
          key={domain}
          style={{
            border: "1px solid #334155",
            borderRadius: "16px",
            padding: "16px",
            background: "#020617"
          }}
        >
          <div style={{
            color: "#94a3b8",
            fontSize: "12px",
            textTransform: "uppercase",
            letterSpacing: "0.08em"
          }}>
            Domain
          </div>

          <h3>{domain}</h3>

          <p style={{ color: "#cbd5e1", lineHeight: "1.5" }}>
            {payload.deployment_ready
              ? "Cognition route available."
              : "Awaiting deployment readiness."}
          </p>
        </section>
      ))}
    </section>
  );
}
