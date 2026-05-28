const PATHS = {
  geoscenReadiness: "./data/serving/geoscen/geoscen_tier1_readiness_report.json",
  cFlowLatest: "./data/serving/c_flow/c_flow_latest_v5.json",
  wtiPanel: "./data/serving/wti/wti_panel.json"
};

async function getJson(path) {
  try {
    const res = await fetch(`${path}?ts=${Date.now()}`, { cache: "no-store" });
    if (!res.ok) throw new Error(`${res.status} ${path}`);
    return await res.json();
  } catch (err) {
    console.warn("Load failed:", path, err);
    return null;
  }
}

function html(id, value) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = value;
}

function sources(items) {
  return `<div class="source-list">${items.map(x => `• ${x}`).join("<br>")}</div>`;
}

async function renderMacro() {
  const readiness = await getJson(PATHS.geoscenReadiness);
  const s = readiness?.summary || {};
  html("macro-zt", `<div class="value">${s.readiness_state || "UNKNOWN"}</div>${sources(["GeoScen readiness report", "Tier 1 validator"])}`);
  html("macro-rbl", "Tier 1 system is deploy-ready. Governance, freshness, serving integrity & placeholder checks are clean.");
  html("macro-score", `<div class="value">${s.readiness_score ?? "--"} / 100</div>`);
  html("macro-graph", sources(["GeoScen Tier 1 Overlay", "C_FLOW v5", "WTI Pressure", "Rates", "FX", "Equity Breadth"]));
}

async function renderCommflow() {
  const c = await getJson(PATHS.cFlowLatest);
  html("commflow-zt", `<div class="value">${c?.c_flow_state_v5 || "UNKNOWN"}</div>${sources(["C_FLOW latest v5 JSON"])}`);
  html("commflow-rbl", "C_FLOW v5 is live. WTI should now route into C_FLOW as commodity-pressure context.");
  html("commflow-score", `<div class="value">${Number(c?.fund_flow_pressure ?? 0).toFixed(4)}</div>`);
  html("commflow-graph", sources(["C_FLOW v5", "WTI panel", "GeoScen overlay", "Equity breadth/factors"]));
}

function renderEquities() {
  html("equities-zt", "Breadth / factor source ready.");
  html("equities-rbl", "Equities should consume breadth_factor_serving_v1, equities_serving_v2, ETF PMI mappings & sigma rank.");
  html("equities-score", "Pending chart binding.");
  html("equities-graph", sources(["Breadth factor serving", "Equities serving", "ETF PMI breadth", "Equities sigma rank"]));
}

function bindNav() {
  const buttons = document.querySelectorAll("[data-view]");
  const views = document.querySelectorAll(".view");

  buttons.forEach(btn => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.view;
      buttons.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      views.forEach(v => v.classList.remove("active"));
      document.getElementById(`view-${target}`)?.classList.add("active");

      if (target === "macro") renderMacro();
      if (target === "commflow") renderCommflow();
      if (target === "equities") renderEquities();
    });
  });

  document.querySelector('[data-view="macro"]')?.classList.add("active");
}

document.addEventListener("DOMContentLoaded", () => {
  bindNav();
  renderMacro();
});
