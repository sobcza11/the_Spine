function renderExecutiveCards(payloads){

    const flat = {};

    (payloads || []).forEach(x => {
        flat[x.component] = x.payload || {};
    });

    const metrics = [
        {
            label: "Runtime",
            value: "OFFLINE",
            state: "Cloudflare-ready static runtime"
        },
        {
            label: "Components",
            value: (payloads || []).length,
            state: "Route payload count"
        },
        {
            label: "Governance",
            value: "AI-LAST",
            state: "Registry-first / explainable"
        },
        {
            label: "Status",
            value: "ACTIVE",
            state: "Institutional route initialized"
        }
    ];

    return renderMetricCards(metrics);
}
