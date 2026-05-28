async function hydrateRouteRuntime(route){

    const routeBody = document.getElementById("active-route-body");

    if(!routeBody){
        return;
    }

    routeBody.innerHTML = `
        <section class="narrative-block">
            <h2>${route.label}</h2>
            <p>Loading institutional runtime...</p>
        </section>
    `;

    const routeDef = await loadRouteDefinition(route.route);

    let payloads = await loadRouteRuntime(route);

    if(route.route === "/executive"){
        payloads = await loadExecutiveRuntime();
    }

    if(route.route === "/finstate"){
        payloads = await loadFinStateRuntime();
    }

    if(route.route === "/cflow"){
        payloads = await loadCFlowRuntime();
    }

    routeBody.innerHTML = `

        ${executiveHeader(
            route.label,
            "OFFLINE ACTIVE",
            new Date().toISOString()
        )}

        ${renderRoutePanels(routeDef)}

        ${
            route.route === "/executive"
            ? renderExecutiveRuntimeDashboard(payloads)
            : route.route === "/finstate"
            ? renderFinStateDashboard(payloads)
            : renderInstitutionalDashboard(payloads)
        }

    `;
}

window.addEventListener(
    "route-change",
    e => hydrateRouteRuntime(e.detail)
);
