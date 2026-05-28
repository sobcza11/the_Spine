function renderCrossAssetCognition(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.cross_asset_pressure ??
                p.q_cross_sector_pressure ??
                p.fusion_pressure ??
                p.systemicity_score ??
                0
        });
    });

    return renderHeatmap(
        "Recursive Cross-Asset Cognition",
        rows
    );
}
