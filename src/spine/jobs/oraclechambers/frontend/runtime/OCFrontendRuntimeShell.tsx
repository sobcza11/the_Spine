import React from "react";
import type { OCHydrationPayload, OCPanelState, OCOverlayState } from "../types/ocHydrationTypes";

interface OCFrontendRuntimeShellProps {
  payload: OCHydrationPayload;
}

function PanelCard({ panel }: { panel: OCPanelState }) {
  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900 p-4 shadow-sm">
      <div className="text-xs uppercase tracking-wide text-slate-400">
        {panel.domain ?? "ORACLECHAMBERS"}
      </div>

      <h2 className="mt-1 text-lg font-semibold text-slate-100">
        {panel.title}
      </h2>

      <p className="mt-3 text-sm leading-6 text-slate-300">
        {panel.summary ?? "Awaiting governed cognition state."}
      </p>
    </section>
  );
}

function OverlayCard({ overlay }: { overlay: OCOverlayState }) {
  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-950 p-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-slate-100">
          {overlay.title}
        </h3>

        <span className="text-xs text-slate-400">
          {overlay.active ? "Active" : "Inactive"}
        </span>
      </div>

      <p className="mt-3 text-sm leading-6 text-slate-300">
        {overlay.summary ?? "No overlay escalation currently available."}
      </p>
    </section>
  );
}

export default function OCFrontendRuntimeShell({ payload }: OCFrontendRuntimeShellProps) {
  const panels = payload.panels ?? [];
  const overlays = payload.overlays ?? [];

  return (
    <main className="min-h-screen bg-slate-950 p-6 text-slate-100">
      <header className="mb-6 rounded-2xl border border-slate-700 bg-slate-900 p-5">
        <div className="text-xs uppercase tracking-wide text-slate-400">
          GEOSCEN | ORACLECHAMBERS
        </div>

        <h1 className="mt-2 text-2xl font-bold">
          Institutional Cognition Runtime
        </h1>

        <div className="mt-4 grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
          <div>
            <div className="text-slate-500">Deployment</div>
            <div>{payload.deployment_ready ? "Ready" : "Not Ready"}</div>
          </div>

          <div>
            <div className="text-slate-500">Mode</div>
            <div>{payload.runtime?.deployment_mode ?? payload.site_mode ?? "offline_first"}</div>
          </div>

          <div>
            <div className="text-slate-500">Focus Panel</div>
            <div>{payload.runtime?.focus_panel ?? payload.runtime?.priority_panel ?? "rbl_panel"}</div>
          </div>

          <div>
            <div className="text-slate-500">Regime</div>
            <div>{payload.regime?.headline_regime ?? payload.regime?.regime_label ?? "Pending"}</div>
          </div>
        </div>
      </header>

      <section className="grid gap-4 md:grid-cols-3">
        {panels.map((panel) => (
          <PanelCard key={panel.id} panel={panel} />
        ))}
      </section>

      <section className="mt-6 grid gap-4 md:grid-cols-2">
        {overlays.map((overlay) => (
          <OverlayCard key={overlay.id} overlay={overlay} />
        ))}
      </section>
    </main>
  );
}
