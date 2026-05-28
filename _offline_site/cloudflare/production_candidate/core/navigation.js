async function buildNavigation(){

    const response = await fetch("./config/geoscen_routes.json");
    const data = await response.json();

    const nav = document.getElementById("route-nav");

    if(!nav){
        return;
    }

    nav.innerHTML = "";

    data.routes.forEach(route => {

        const button = document.createElement("button");

        button.className = "route-button";
        button.textContent = route.label;
        button.dataset.route = route.route;

        button.onclick = () => {
            window.location.hash = route.route;
            window.dispatchEvent(new CustomEvent("route-change", {
                detail: route
            }));
        };

        nav.appendChild(button);
    });
}

buildNavigation();
