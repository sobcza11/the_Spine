function renderRecursiveAttentionRouting(payloads){

    const rows = (payloads || []).map(x => {

        const p = x.payload?.payload || x.payload || {};

        const score =
            p.average_systemic_pressure ??
            p.contradiction_score ??
            p.structural_pressure_score ??
            p.temperature_score ??
            p.drift_score ??
            0;

        return [
            x.component,
            Number(score || 0).toFixed(4),
            score >= 0.7 ? "highest_attention" : score >= 0.4 ? "watch_attention" : "monitor_attention"
        ];
    });

    return renderTable(
        "Recursive Attention Routing",
        ["Component", "Attention Score", "Routing State"],
        rows
    );
}
