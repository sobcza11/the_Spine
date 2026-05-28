function renderExecutiveFragilityTopology(payloads){

    const graphPayload = (payloads || []).find(
        x => JSON.stringify(x.payload || {}).includes("sample_edges")
    );

    const p = graphPayload?.payload?.payload || graphPayload?.payload || {};

    if(Array.isArray(p.sample_edges)){
        return renderEdgeList(
            "Executive Fragility Topology Graph",
            p.sample_edges.slice(0, 50)
        );
    }

    return `
        <section class="narrative-block">
            <h2>Executive Fragility Topology Graph</h2>
            <p>No fragility edge list available yet.</p>
        </section>
    `;
}
