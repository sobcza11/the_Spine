function initRuntimePerformanceMonitoring(){

    const perf = {
        started_at: new Date().toISOString(),
        navigation_type: performance?.navigation?.type ?? null,
        load_event_ms: null,
        resource_count: 0
    };

    window.addEventListener("load", () => {

        const nav = performance.getEntriesByType("navigation")[0];

        perf.load_event_ms = nav ? nav.loadEventEnd : null;
        perf.resource_count = performance.getEntriesByType("resource").length;

        window.ISOVECTOR_PERFORMANCE = perf;

        console.log("IsoVector runtime performance:", perf);
    });

    return perf;
}

initRuntimePerformanceMonitoring();
