function renderRecursiveRiskEscalation(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.risk_escalation_score ??
                p.average_systemic_pressure ??
                p.contradiction_score ??
                p.structural_pressure_score ??
                p.temperature_score ??
                0
        });
    });

    return renderHeatmap(
        "Recursive Risk Escalation Heatmap",
        rows
    );
}
