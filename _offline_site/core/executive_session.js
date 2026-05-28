function initExecutiveSession(){

    const session = {
        session_id: crypto.randomUUID
            ? crypto.randomUUID()
            : `session_${Date.now()}`,
        started_at: new Date().toISOString(),
        runtime: "GeoScen / IsoVector Offline Runtime",
        mode: "static_cloudflare_pages"
    };

    sessionStorage.setItem(
        "isovector_session",
        JSON.stringify(session)
    );

    window.ISOVECTOR_SESSION = session;

    return session;
}

initExecutiveSession();
