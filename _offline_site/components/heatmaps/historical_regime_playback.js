function renderHistoricalRegimePlayback(payloads){

    const rows = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        rows.push({
            label: x.component,
            value:
                p.drift_score ??
                p.temperature_score ??
                p.average_systemic_pressure ??
                0
        });
    });

    return renderHeatmap(
        "Historical Regime Playback",
        rows
    );
}
