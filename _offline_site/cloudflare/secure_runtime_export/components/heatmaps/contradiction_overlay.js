function renderContradictionOverlay(payloads){

    const contradiction = (payloads || []).find(
        x =>
        x.component === "contradiction_engine" ||
        JSON.stringify(x.payload || {}).includes("contradiction_score")
    );

    const p = contradiction?.payload?.payload || contradiction?.payload || {};

    return renderHeatmap(
        "Contradiction Overlay",
        [
            {
                label: "Contradiction Score",
                value: p.contradiction_score ?? 0
            },
            {
                label: "Active Contradictions",
                value: p.active_contradiction_count ?? 0
            }
        ]
    );
}
