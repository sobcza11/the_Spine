function renderRecursiveFragility(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_recursive_fragility ??
                p.q_systemic_fragility ??
                p.q_recursive_pressure ??
                0
        });
    });

    return renderHeatmap(
        "Recursive Fragility Overlay",
        rows
    );
}
