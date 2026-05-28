import React from "react";
import ReactDOM from "react-dom/client";

import {
  useOCRuntimeStore,
} from "./runtime/ocRuntimeStore";

import OCExecutivePanels from "./runtime/OCExecutivePanels";

import OCDomainPanels from "./runtime/OCDomainPanels";

import OCOverlayPanels from "./runtime/OCOverlayPanels";

import {
  evaluateContradictionState,
} from "./runtime/OCContradictionEngine";

import OCAttentionPanel from "./runtime/OCAttentionPanel";


function App() {

  const {
    payload,
    loading,
    error,
    refresh,
  } = useOCRuntimeStore();

  if (loading) {
    return (
      <main style={{
        background: "#020617",
        color: "#e5e7eb",
        minHeight: "100vh",
        padding: "24px"
      }}>
        <h1>GEOSCEN | ORACLECHAMBERS</h1>

        <p>
          Loading institutional cognition payload...
        </p>
      </main>
    );
  }

  if (error || !payload) {
    return (
      <main style={{
        background: "#020617",
        color: "#ef4444",
        minHeight: "100vh",
        padding: "24px"
      }}>
        <h1>OracleChambers Runtime Error</h1>

        <p>
          {error ?? "No hydration payload available."}
        </p>
      </main>
    );
  }

  const runtimePayload = payload;

  const contradictionState =
    evaluateContradictionState(runtimePayload);

  return (
    <main style={{
      background: "#020617",
      color: "#e5e7eb",
      minHeight: "100vh",
      padding: "24px"
    }}>

      <header>

        <div style={{
          color: "#94a3b8",
          fontSize: "12px",
          textTransform: "uppercase",
          letterSpacing: "0.08em"
        }}>
          GEOSCEN | ORACLECHAMBERS
        </div>

        <h1>
          Institutional Cognition Runtime
        </h1>

        <p>
          Deployment Status:{" "}
          {runtimePayload.deployment_ready
            ? "Ready"
            : "Not Ready"}
        </p>

        <button
          onClick={refresh}
          style={{
            marginTop: "12px",
            padding: "8px 14px",
            borderRadius: "10px",
            border: "1px solid #475569",
            background: "#0f172a",
            color: "#e5e7eb",
            cursor: "pointer"
          }}
        >
          Refresh Runtime
        </button>

      </header>

      <OCExecutivePanels payload={runtimePayload} />
      <OCAttentionPanel payload={runtimePayload} />

      <section style={{
        marginTop: "24px",
        border: "1px solid #334155",
        borderRadius: "16px",
        padding: "18px",
        background: "#111827"
      }}>

        <h2>
          Contradiction Engine
        </h2>

        <p>
          {contradictionState.label}
        </p>

        <p style={{
          color: "#cbd5e1"
        }}>
          {contradictionState.summary}
        </p>

      </section>

      <OCDomainPanels payload={runtimePayload} />

      <OCOverlayPanels payload={runtimePayload} />

    </main>
  );
}

ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
