function renderRecursiveDeteriorationMonitoring(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_recursive_deterioration ??
                p.q_quarterly_systemicity_overlay ??
                p.q_corporate_fragility_conditioning ??
                p.edge_count ??
                p.component_count ??
                0
        });
    });

    return renderHeatmap(
        "Recursive Deterioration Monitoring",
        rows
    );
}
