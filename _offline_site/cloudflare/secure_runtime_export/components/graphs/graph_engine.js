function renderGraph(title, graphObject){

    const rows = Object.entries(graphObject || {}).map(
        ([path, count]) => [path, count]
    );

    return renderTable(
        title,
        ["Path", "Count"],
        rows
    );
}

function renderEdgeList(title, edges){

    const rows = (edges || []).map(e => [
        e.source_node || e.source || "--",
        e.target_node || e.target || "--",
        e.edge_type || e.type || "--",
        e.edge_pressure ?? e.weight ?? "--"
    ]);

    return renderTable(
        title,
        ["Source", "Target", "Type", "Pressure"],
        rows
    );
}
