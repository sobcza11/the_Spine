function renderNarrative(title, text, source="OC Runtime"){

    return `

    <article class="narrative-block">

        <div class="eyebrow">${source}</div>

        <h2>${title}</h2>

        <p>${safeText(text)}</p>

    </article>

    `;
}

function extractNarrative(payload){

    return (
        payload?.rbl_text ||
        payload?.rbl ||
        payload?.summary ||
        payload?.interpretation_summary ||
        payload?.narrative ||
        payload?.rbl_contradiction_paragraph ||
        "Narrative unavailable."
    );
}
