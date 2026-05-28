function renderPropagationOverlay(payloads){

    const langroid = (payloads || []).find(
        x =>
        x.component === "escalation_graph" ||
        x.component === "executive_synthesis" ||
        JSON.stringify(x.payload || {}).includes("escalation_graph")
    );

    const payload = langroid?.payload?.payload || langroid?.payload || {};

    if(payload.escalation_graph){
        return renderGraph(
            "Propagation Overlay",
            payload.escalation_graph
        );
    }

    return `
        <section class="narrative-block">
            <h2>Propagation Overlay</h2>
            <p>No escalation graph attached to this route yet.</p>
        </section>
    `;
}
