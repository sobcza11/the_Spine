async function safeRuntimeFetch(path, fallback={}){

    try{

        const res = await fetch(path);

        if(!res.ok){
            throw new Error(`Fetch failed: ${path}`);
        }

        return await res.json();

    }catch(err){

        console.warn("Runtime fallback activated:", path, err);

        return {
            component: "runtime_fallback",
            path,
            status: "fallback_runtime_payload",
            payload: fallback
        };
    }
}

window.safeRuntimeFetch = safeRuntimeFetch;
