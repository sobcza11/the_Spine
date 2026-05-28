function renderGeoScenFinStateHooks(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push([
            x.component,
            p.executive_state ??
            p.status ??
            "--",
            p.edge_count ??
            p.component_count ??
            p.symbols ??
            "--"
        ]);
    });

    return renderTable(
        "GeoScen ? FINSTATE Propagation Hooks",
        ["Component", "State", "Count"],
        rows
    );
}
