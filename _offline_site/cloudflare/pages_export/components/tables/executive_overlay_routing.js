function renderExecutiveOverlayRouting(payloads){

    const rows = (payloads || []).map(x => {

        const p = x.payload?.payload || x.payload || {};

        return [
            x.component,
            p.status ?? "--",
            p.executive_state ?? p.runtime_objective ?? "--"
        ];
    });

    return renderTable(
        "Executive Overlay Routing Layer",
        ["Component", "Status", "Executive Route"],
        rows
    );
}
