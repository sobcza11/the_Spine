function renderPersistenceEscalation(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_survivability_persistence ??
                p.q_recursive_memory ??
                p.q_drift_escalation ??
                0
        });
    });

    return renderHeatmap(
        "Persistence Escalation Overlay",
        rows
    );
}
