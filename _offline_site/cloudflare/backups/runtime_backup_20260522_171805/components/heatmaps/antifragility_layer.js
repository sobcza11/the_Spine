function renderAntiFragilityLayer(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_antifragility_score ??
                p.antifragility_score ??
                p.q_recovery_resilience ??
                0
        });
    });

    return renderHeatmap(
        "Anti-Fragility Visualization",
        rows
    );
}
