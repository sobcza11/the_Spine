function renderCorporateSystemicity(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_quarterly_systemicity_overlay ??
                p.q_corporate_fragility_conditioning ??
                p.q_survivability_escalation_score ??
                p.systemicity_score ??
                0
        });
    });

    return renderHeatmap(
        "Corporate Systemicity Overlay",
        rows
    );
}
