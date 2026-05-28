async function loadRuntimePayload(path){

    try{

        const response = await fetch(path);

        if(!response.ok){

            throw new Error(
                `Runtime load failure: ${path}`
            );
        }

        const payload = await response.json();

        return payload;

    }catch(err){

        console.error(
            "Runtime loader failure:",
            err
        );

        return {
            status: "runtime_load_failed",
            path: path
        };
    }
}

async function loadRouteRuntime(routeConfig){

    const runtimePayloads = [];

    for(const component of routeConfig.components){

        const path =
            `/runtime/${component}.json`;

        const payload =
            await loadRuntimePayload(path);

        runtimePayloads.push({
            component,
            payload
        });
    }

    return runtimePayloads;
}
