function renderSurvivabilityDrift(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_survivability_drift ??
                p.q_survivability_memory_4q ??
                p.drift_score ??
                0
        });
    });

    return renderHeatmap(
        "Survivability Drift Monitoring",
        rows
    );
}
