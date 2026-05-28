function renderGlobalRuntimeStatus(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push([
            x.component,
            p.status ?? "--",
            p.generated_at_utc ?? "--"
        ]);
    });

    return renderTable(
        "Global Runtime Status Matrix",
        ["Component", "Status", "Generated"],
        rows
    );
}
