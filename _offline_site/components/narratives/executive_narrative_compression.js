function renderExecutiveNarrativeCompression(payloads){

    const lines = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        if(p.interpretation){
            lines.push(`${x.component}: ${p.interpretation}`);
        }else if(p.rbl_text){
            lines.push(`${x.component}: ${p.rbl_text}`);
        }else if(p.status){
            lines.push(`${x.component}: ${p.status}`);
        }
    });

    const compressed = lines.length
        ? lines.slice(0, 5).join(" ")
        : "Executive narrative compression initialized. Runtime payloads available for synthesis.";

    return renderNarrative(
        "Executive Narrative Compression",
        compressed,
        "Executive Runtime"
    );
}
