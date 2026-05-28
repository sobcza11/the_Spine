function renderExecutiveAlerts(payloads){

    const alerts = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        const score =
            p.contradiction_score ??
            p.systemicity_score ??
            p.fusion_pressure ??
            p.q_recursive_fragility ??
            0;

        if(score >= 0.70){

            alerts.push(`
                <div class="alert-critical">
                    <strong>${x.component}</strong>
                    <p>Critical recursive escalation detected.</p>
                </div>
            `);

        } else if(score >= 0.40){

            alerts.push(`
                <div class="alert-warning">
                    <strong>${x.component}</strong>
                    <p>Elevated systemic pressure detected.</p>
                </div>
            `);

        }
    });

    if(alerts.length === 0){

        alerts.push(`
            <div class="alert-stable">
                <strong>Runtime Stable</strong>
                <p>No major recursive escalation signals detected.</p>
            </div>
        `);
    }

    return `

        <section class="narrative-block">

            <h2>Institutional Alert & Escalation System</h2>

            ${alerts.join("")}

        </section>

    `;
}
