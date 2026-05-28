function renderEscalationGraph(payloads){

    const langroid = (payloads || []).find(
        x => JSON.stringify(x.payload || {}).includes("escalation_graph")
    );

    const p = langroid?.payload?.payload || langroid?.payload || {};

    if(!p.escalation_graph){
        return `
            <section class="narrative-block">
                <h2>Escalation Graph</h2>
                <p>No escalation graph available for this route yet.</p>
            </section>
        `;
    }

    return renderGraph(
        "Escalation Graph",
        p.escalation_graph
    );
}
