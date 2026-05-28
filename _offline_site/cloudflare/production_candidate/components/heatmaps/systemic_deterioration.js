function renderSystemicDeterioration(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.systemic_deterioration ??
                p.q_systemic_fragility ??
                p.q_recursive_contagion ??
                p.contradiction_score ??
                0
        });
    });

    return renderHeatmap(
        "Systemic Deterioration Overlay",
        rows
    );
}
