function renderBalanceSheetTopology(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push([
            x.component,
            p.debt_to_equity ??
            p.q_debt_pressure ??
            "--",
            p.interest_coverage ??
            p.q_interest_fragility ??
            "--",
            p.fcf_yield ??
            p.q_fcf_pressure ??
            "--"
        ]);
    });

    return renderTable(
        "Balance-Sheet Topology",
        [
            "Component",
            "Debt Pressure",
            "Interest Fragility",
            "FCF Pressure"
        ],
        rows
    );
}
