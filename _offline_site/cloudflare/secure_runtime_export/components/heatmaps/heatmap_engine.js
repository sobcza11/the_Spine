function heatmapCell(label, value){

    const v = Number(value || 0);

    let state = "low";

    if(v >= 0.70){
        state = "high";
    }else if(v >= 0.40){
        state = "medium";
    }

    return `

    <div class="heatmap-cell ${state}">

        <span>${label}</span>

        <strong>${v.toFixed(3)}</strong>

    </div>

    `;
}

function renderHeatmap(title, rows){

    return `

    <article class="narrative-block">

        <h2>${title}</h2>

        <div class="heatmap-grid">

            ${rows.map(r => heatmapCell(r.label, r.value)).join("")}

        </div>

    </article>

    `;
}
