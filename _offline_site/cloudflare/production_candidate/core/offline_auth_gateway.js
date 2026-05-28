function initOfflineAuthGateway(){

    const AUTH_KEY = "isovector_offline_auth";

    const existing = localStorage.getItem(AUTH_KEY);

    if(existing === "accepted"){
        return true;
    }

    const accepted = confirm(
        "IsoVector Offline Runtime\n\nAuthorized access only. Continue?"
    );

    if(accepted){
        localStorage.setItem(AUTH_KEY, "accepted");
        return true;
    }

    document.body.innerHTML = `
        <section class="narrative-block">
            <h2>Access Not Authorized</h2>
            <p>Offline runtime access was not accepted.</p>
        </section>
    `;

    return false;
}

initOfflineAuthGateway();
