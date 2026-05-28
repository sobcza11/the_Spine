function renderSectorDeteriorationSync(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_sector_sync ??
                p.q_cross_sector_pressure ??
                p.q_recursive_sync ??
                0
        });
    });

    return renderHeatmap(
        "Cross-Sector Deterioration Synchronization",
        rows
    );
}
