
async function loadJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}`);
  return await res.json();
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text ?? "Unavailable";
}

async function hydrate() {
  try {
    const frontend = await loadJson("./geoscen_runtime/geoscen_frontend_intelligence_layer_v1.json");
    const contradiction = await loadJson("./geoscen_runtime/geoscen_contradiction_engine_v1.json");
    const drift = await loadJson("./geoscen_runtime/geoscen_historical_narrative_drift_engine_v1.json");
    const structural = await loadJson("./geoscen_runtime/geoscen_structural_macro_layer_v1.json");
    const cb = await loadJson("./geoscen_runtime/geoscen_cross_country_policy_cognition_v1.json");

    setText("runtime-status", "Healthy | Offline");
    setText("macro-zt", frontend.temperature || "UNKNOWN");
    setText("macro-rbl", `Temperature score: ${frontend.temperature_score ?? "--"}`);

    const directRbl = await loadJson("./geoscen_runtime/geoscen_rbl_synthesis_v1.json").catch(() => null);

    const rblText =
    frontend.rbl ||
    frontend?.evidence?.geoscen_rbl?.rbl ||
    directRbl?.rbl ||
    directRbl?.rbl_text ||
    directRbl?.summary ||
    directRbl?.report ||
    JSON.stringify(directRbl, null, 2) ||
    "RBL unavailable";

    setText("rbl-text", rblText);    

    setText("final-metric", `${Math.round((frontend.temperature_score || 0) * 100)} / 100`);

    const routes = document.getElementById("graph-routes");
    if (routes) {
      routes.innerHTML = "";
      (frontend.graph_routes || []).forEach(route => {
        const li = document.createElement("li");
        li.textContent = `${route.title} → ${route.component}`;
        routes.appendChild(li);
      });
    }

    setText(
      "contradiction-text",
      `${contradiction.contradiction_severity || "none"} | score=${contradiction.contradiction_score ?? 0}`
    );

    setText(
      "drift-text",
      `${drift.drift_regime || "Unavailable"} | score=${drift.drift_score ?? "--"}`
    );

    setText(
      "structural-text",
      `${structural.structural_regime || "Unavailable"} | score=${structural.structural_pressure_score ?? "--"}`
    );

    setText(
      "cb-text",
      `Active CBs: ${cb.active_cb_count ?? "--"} / ${cb.tracked_cb_count ?? "--"}`
    );
  } catch (err) {
    setText("runtime-status", "Runtime Error");
    console.error(err);
  }
}

hydrate();
