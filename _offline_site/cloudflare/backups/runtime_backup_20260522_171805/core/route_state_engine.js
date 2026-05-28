let ACTIVE_ROUTE = null;

async function getRoutes(){

    const response = await fetch("./config/geoscen_routes.json");
    return await response.json();
}

async function resolveRoute(){

    const routes = await getRoutes();

    const hash =
        window.location.hash.replace("#", "") || "/executive";

    const route =
        routes.routes.find(r => r.route === hash)
        || routes.routes.find(r => r.route === "/executive");

    ACTIVE_ROUTE = route;

    return route;
}

async function renderActiveRoute(){

    const route = await resolveRoute();

    const title = document.getElementById("active-route-title");
    const body = document.getElementById("active-route-body");

    if(title){
        title.textContent = route.label;
    }

    if(body){
        body.innerHTML = `
            <div class="narrative-block">
                <h2>${route.label}</h2>
                <p>Route initialized. Runtime components: ${route.components.join(", ")}</p>
            </div>
        `;
    }

    return route;
}

window.addEventListener("hashchange", renderActiveRoute);
window.addEventListener("route-change", renderActiveRoute);

renderActiveRoute();
