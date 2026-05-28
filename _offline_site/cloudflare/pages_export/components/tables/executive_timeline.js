function renderExecutiveTimeline(payloads){

    const rows = (payloads || []).map(x => {

        const p = x.payload?.payload || x.payload || {};

        return [
            x.component,
            p.generated_at_utc ?? p.built_at_utc ?? "--",
            p.status ?? "--"
        ];
    });

    return renderTable(
        "Executive Cognitive Timeline",
        ["Component", "Timestamp", "Status"],
        rows
    );
}
