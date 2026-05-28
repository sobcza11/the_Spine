function renderRuntimeNarratives(payloads){

    const blocks = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        const narrative = extractNarrative(p);

        blocks.push(
            renderNarrative(
                x.component,
                narrative,
                p.component || "Runtime Narrative"
            )
        );
    });

    return blocks.join("");
}
