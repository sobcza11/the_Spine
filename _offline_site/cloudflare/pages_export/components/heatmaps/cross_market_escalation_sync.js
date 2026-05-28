function renderCrossMarketEscalationSync(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.average_systemic_pressure ??
                p.fusion_pressure ??
                p.temperature_score ??
                p.contradiction_score ??
                p.drift_score ??
                0
        });
    });

    return renderHeatmap(
        "Cross-Market Escalation Synchronization",
        rows
    );
}
