function renderMacroRegime(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.regime_probability ??
                p.structural_pressure_score ??
                p.temperature_score ??
                p.drift_score ??
                0
        });
    });

    return `

        <section class="narrative-block">

            <h2>Macro Regime Visualization</h2>

            <div class="heatmap-grid">

                ${rows.map(r => heatmapCell(
                    r.label,
                    r.value
                )).join("")}

            </div>

        </section>

    `;
}
