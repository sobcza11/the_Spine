function initRecursiveRuntimeLogging(){

    const key = "isovector_runtime_log";

    const logs = JSON.parse(
        sessionStorage.getItem(key) || "[]"
    );

    function log(level, message, detail={}){

        logs.push({
            level,
            message,
            detail,
            timestamp: new Date().toISOString()
        });

        sessionStorage.setItem(
            key,
            JSON.stringify(logs.slice(-500))
        );

        if(level === "error"){
            console.error(message, detail);
        }else if(level === "warn"){
            console.warn(message, detail);
        }else{
            console.log(message, detail);
        }
    }

    window.ISOVECTOR_LOG = {
        log,
        get: () => logs
    };

    log("info", "recursive_runtime_logging_initialized", {
        runtime: "GeoScen / IsoVector"
    });
}

initRecursiveRuntimeLogging();
