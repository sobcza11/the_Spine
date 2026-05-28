function initExecutiveTelemetry(){

    const key = "isovector_exec_telemetry";

    const telemetry = JSON.parse(
        sessionStorage.getItem(key) || "[]"
    );

    function record(event_type, detail={}){

        telemetry.push({
            event_type,
            detail,
            timestamp: new Date().toISOString()
        });

        sessionStorage.setItem(
            key,
            JSON.stringify(telemetry.slice(-250))
        );
    }

    window.ISOVECTOR_TELEMETRY = {
        record,
        get: () => telemetry
    };

    record("session_initialized", {
        route: window.location.hash || "/executive"
    });
}

initExecutiveTelemetry();
