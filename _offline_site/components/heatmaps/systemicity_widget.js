function renderSystemicityWidget(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        const value =
            p.q_quarterly_systemicity_overlay ??
            p.systemicity_score ??
            p.structural_pressure_score ??
            p.temperature_score ??
            p.drift_score ??
            p.contradiction_score ??
            0;

        rows.push({
            label: x.component,
            value: value
        });
    });

    return renderHeatmap(
        "Systemicity Monitor",
        rows
    );
}
