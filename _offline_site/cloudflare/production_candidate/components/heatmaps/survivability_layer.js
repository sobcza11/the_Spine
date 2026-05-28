function renderSurvivabilityLayer(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_survivability_persistence ??
                p.q_survivability_escalation ??
                p.q_antifragility_score ??
                p.q_recursive_pressure ??
                0
        });
    });

    return `

        <section class="narrative-block">

            <h2>Survivability Visualization</h2>

            <div class="heatmap-grid">

                ${rows.map(r => heatmapCell(
                    r.label,
                    r.value
                )).join("")}

            </div>

        </section>

    `;
}
