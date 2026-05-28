function statusBar(label, value){

    const v = Number(value || 0);

    const pct = Math.max(
        0,
        Math.min(100, v * 100)
    );

    return `

    <div class="status-bar-wrap">

        <div class="status-bar-label">
            <span>${label}</span>
            <strong>${pct.toFixed(1)}%</strong>
        </div>

        <div class="status-bar-bg">
            <div
                class="status-bar-fill"
                style="width:${pct}%"
            ></div>
        </div>

    </div>

    `;
}

function renderExecutiveStatusBars(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push(
            statusBar(
                x.component,
                p.temperature_score ??
                p.structural_pressure_score ??
                p.fusion_pressure ??
                p.contradiction_score ??
                p.drift_score ??
                0.35
            )
        );
    });

    return `

        <section class="narrative-block">

            <h2>Executive Status Bars</h2>

            ${rows.join("")}

        </section>

    `;
}
