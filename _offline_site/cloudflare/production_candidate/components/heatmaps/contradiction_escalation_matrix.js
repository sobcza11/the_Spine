function renderContradictionEscalationMatrix(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.contradiction_score ??
                p.active_contradiction_count ??
                p.recursive_contradiction_pressure ??
                0
        });
    });

    return renderHeatmap(
        "Recursive Contradiction Escalation Matrix",
        rows
    );
}
