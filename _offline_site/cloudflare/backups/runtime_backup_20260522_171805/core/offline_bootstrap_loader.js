async function bootstrapOfflineRuntime(){

    const status = document.getElementById("runtime-status");

    if(status){
        status.textContent = "BOOTSTRAP";
    }

    const checks = [
        "./config/geoscen_routes.json",
        "./config/runtime_manifest_v1.json",
        "./deploy_manifest/executive_operational_readiness_v1.json",
        "./deploy_manifest/executive_cognitive_runtime_completion_v1.json"
    ];

    const results = [];

    for(const path of checks){

        try{
            const res = await fetch(path);
            results.push({
                path,
                ok: res.ok
            });
        }catch(err){
            results.push({
                path,
                ok: false
            });
        }
    }

    const ready = results.every(x => x.ok);

    if(status){
        status.textContent = ready ? "READY" : "CHECK";
    }

    window.OFFLINE_BOOTSTRAP = {
        ready,
        checks: results,
        booted_at: new Date().toISOString()
    };

    return window.OFFLINE_BOOTSTRAP;
}

bootstrapOfflineRuntime();
