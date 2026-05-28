async function loadRouteDefinition(routePath){

    const map = {
        "/equities-industry": "./routes/equities_industry/route.json",
        "/equities-sector": "./routes/equities_sector/route.json",
        "/cflow": "./routes/cflow/route.json",
        "/finstate": "./routes/finstate/route.json",
        "/fx": "./routes/fx/route.json",
        "/rates": "./routes/rates/route.json",
        "/executive": "./routes/executive/route.json"
    };

    const path = map[routePath];

    if(!path){
        return null;
    }

    const res = await fetch(path);

    if(!res.ok){
        return null;
    }

    return await res.json();
}

function renderRoutePanels(routeDef){

    if(!routeDef){
        return `
            <section class="narrative-block">
                <h2>Route Definition Missing</h2>
                <p>No route definition is available for this route yet.</p>
            </section>
        `;
    }

    return `
        <section class="narrative-block">
            <h2>${routeDef.label}</h2>
            <p>Status: ${routeDef.status}</p>
        </section>

        <section class="grid">
            ${routeDef.panels.map(panel => `
                <article class="metric-card">
                    <div class="metric-title">${panel.title}</div>
                    <div class="metric-value">${panel.type}</div>
                    <div class="metric-state">${panel.source}</div>
                </article>
            `).join("")}
        </section>
    `;
}
