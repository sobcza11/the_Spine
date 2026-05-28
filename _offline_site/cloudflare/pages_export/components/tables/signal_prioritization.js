function renderSignalPrioritization(payloads){

    const rows = (payloads || []).map(x => {

        const p = x.payload?.payload || x.payload || {};

        const priority =
            p.average_systemic_pressure ??
            p.temperature_score ??
            p.contradiction_score ??
            p.structural_pressure_score ??
            p.drift_score ??
            0;

        return [
            x.component,
            Number(priority || 0).toFixed(4),
            priority >= 0.7 ? "critical" : priority >= 0.4 ? "watch" : "monitor"
        ];
    });

    return renderTable(
        "Institutional Signal Prioritization",
        ["Component", "Priority Score", "Priority State"],
        rows
    );
}
