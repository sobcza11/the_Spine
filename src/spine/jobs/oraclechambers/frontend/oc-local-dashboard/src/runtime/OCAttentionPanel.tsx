import type { OCHydrationPayload } from "../types/ocHydrationTypes";
import { evaluateAttentionRoute } from "./OCAttentionRouter";

interface OCAttentionPanelProps {
  payload: OCHydrationPayload;
}

export default function OCAttentionPanel({
  payload,
}: OCAttentionPanelProps) {
  const route = evaluateAttentionRoute(payload);

  return (
    <section style={{
      marginTop: "24px",
      border: route.escalation
        ? "1px solid #f97316"
        : "1px solid #334155",
      borderRadius: "16px",
      padding: "18px",
      background: route.escalation
        ? "#1c1917"
        : "#0f172a"
    }}>
      <div style={{
        color: "#94a3b8",
        fontSize: "12px",
        textTransform: "uppercase",
        letterSpacing: "0.08em"
      }}>
        Executive Attention Routing
      </div>

      <h2>
        Focus: {route.focusPanel}
      </h2>

      <p>
        Priority: {route.priority}
      </p>

      <p style={{
        color: "#cbd5e1",
        lineHeight: "1.6"
      }}>
        {route.summary}
      </p>
    </section>
  );
}

