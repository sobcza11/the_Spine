function renderCrossAssetSync(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push([
            x.component,
            p.cross_asset_sync ??
            p.cross_asset_pressure ??
            p.systemic_sync ??
            "--",
            p.regime ??
            p.primary_state ??
            "--"
        ]);
    });

    return renderTable(
        "Cross-Asset Synchronization",
        ["Component", "Sync", "State"],
        rows
    );
}
