function renderRecursiveHeatmap(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.temperature_score ??
                p.contradiction_score ??
                p.drift_score ??
                p.structural_pressure_score ??
                p.fusion_pressure ??
                0
        });
    });

    return renderHeatmap(
        "Recursive Runtime Heatmap",
        rows
    );
}
