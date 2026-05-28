const PLANES = [
  ["fx","FX","fx_ai_components_v1.json"],
  ["rates","RATES","rates_ai_components_v1.json"],
  ["c_flow","C_FLOW","cflow_ai_components_v1.json"],
  ["equities_sector","EQUITIES SECTOR","equities_sector_ai_components_v1.json"],
  ["equities_industry","EQUITIES INDUSTRY","equities_industry_ai_components_v1.json"]
];

async function load(){

  const payloads = [];

  for(const [site,label,file] of PLANES){

    const payload =
      await fetch(`./${site}/payloads/${file}`)
      .then(r => r.json());

    payloads.push({
      site,
      label,
      payload
    });
  }

  const fx =
    payloads.find(x => x.site === "fx")?.payload;

  const rates =
    payloads.find(x => x.site === "rates")?.payload;

  const equities =
    payloads.find(x => x.site === "equities_sector")?.payload;

  const regime =
      fx?.zt?.zeitgeist?.regime || "Unknown";

  const primaryStress =
    "FX + Rates";

  const contradiction =
    "Equity strength vs Rates pressure";

  const decisionBias =
    "Defensive monitoring / no broad risk confirmation";

  const takeaway =
    "Conditions remain partially constructive but cross-asset confirmation is incomplete.";

  document.getElementById("executive-summary").innerHTML = `

    <div class="section">

      <h2>Executive Summary</h2>

      <table>

        <tr>
          <th>Current Regime</th>
          <td>${regime}</td>
        </tr>

        <tr>
          <th>Primary Stress</th>
          <td>${primaryStress}</td>
        </tr>

        <tr>
          <th>Main Contradiction</th>
          <td>${contradiction}</td>
        </tr>

        <tr>
          <th>Decision Bias</th>
          <td>${decisionBias}</td>
        </tr>

        <tr>
          <th>Client Takeaway</th>
          <td>${takeaway}</td>
        </tr>

      </table>

    </div>

  `;

  document.getElementById("cards").innerHTML =
    payloads.map(x => {

      const a =
        x.payload.analytics || {};

      const d =
        x.payload.executive_decision_layer || {};

      return `

      <div class="card">

        <h2>${x.label}</h2>

        <div class="label">Zₜ Composite</div>

        <div class="score">
          ${a.z_t_composite ?? "N/A"}
        </div>

        <p>
          <span class="badge">
            ${a.signal_strength || "N/A"}
          </span>
        </p>

        <p>${d.action_bias || ""}</p>

      </div>

      `;
    }).join("");

  document.getElementById("analytics-table").innerHTML =

    `<tr>
      <th>Plane</th>
      <th>Zₜ</th>
      <th>Stress</th>
      <th>Dispersion</th>
      <th>Contradiction</th>
      <th>Decision</th>
    </tr>`

    +

    payloads.map(x => {

      const a =
        x.payload.analytics || {};

      const d =
        x.payload.executive_decision_layer || {};

      return `

      <tr>
        <td>${x.label}</td>
        <td>${a.z_t_composite}</td>
        <td>${a.stress_score}</td>
        <td>${a.dispersion_score}</td>
        <td>${a.contradiction_score}</td>
        <td>${d.action_bias}</td>
      </tr>

      `;
    }).join("");

  document.getElementById("feature-table").innerHTML =

    `<tr>
      <th>Plane</th>
      <th>Top Feature</th>
      <th>Weight</th>
      <th>Direction</th>
    </tr>`

    +

    payloads.map(x => {

      const f =
        (x.payload.feature_vector || [])[0] || {};

      return `

      <tr>
        <td>${x.label}</td>
        <td>${f.feature || "N/A"}</td>
        <td>${f.weight ?? "N/A"}</td>
        <td>${f.direction || "N/A"}</td>
      </tr>

      `;
    }).join("");

  document.getElementById("analog-table").innerHTML =

    `<tr>
      <th>Plane</th>
      <th>Top Analog</th>
      <th>Similarity</th>
      <th>Why</th>
    </tr>`

    +

    payloads.map(x => {

      const h =
        (x.payload.historical_analogs || [])[0] || {};

      return `

      <tr>
        <td>${x.label}</td>
        <td>${h.regime || "N/A"}</td>
        <td>${h.similarity ?? "N/A"}</td>
        <td>${h.why || "N/A"}</td>
      </tr>

      `;
    }).join("");
}

load().catch(err => {

  document.body.innerHTML =
    "Dashboard failed: " + err.message;

});
