function renderInstitutionalDeteriorationHeatmap(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.q_systemic_deterioration ??
                p.q_quarterly_systemicity_overlay ??
                p.q_corporate_fragility_conditioning ??
                p.q_credit_cycle_survivability_pressure ??
                0
        });
    });

    return renderHeatmap(
        "Institutional Deterioration Heatmap",
        rows
    );
}
