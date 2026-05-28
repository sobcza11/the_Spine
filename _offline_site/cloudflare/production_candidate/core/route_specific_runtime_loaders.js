async function loadExecutiveRuntime(){

    return await Promise.all([
        loadRuntimePayload(
            "./geoscen_runtime/geoscen_rbl_synthesis_v1_normalized.json"
        ),
        loadRuntimePayload(
            "./geoscen_runtime/geoscen_contradiction_engine_v1_normalized.json"
        ),
        loadRuntimePayload(
            "./geoscen_runtime/geoscen_historical_narrative_drift_engine_v1_normalized.json"
        ),
        loadRuntimePayload(
            "./langroid_runtime/langroid_runtime_export_normalized.json"
        )
    ]);
}

async function loadFinStateRuntime(){

    return await Promise.all([
        loadRuntimePayload(
            "./runtime/zt_finstate_zeitgeist.json"
        ),
        loadRuntimePayload(
            "./runtime/rbl_finstate_oc.json"
        ),
        loadRuntimePayload(
            "./runtime/rbl_finstate_systemic_oc.json"
        )
    ]);
}

async function loadCFlowRuntime(){

    return await Promise.all([
        loadRuntimePayload(
            "./runtime/zt_commodity_flow.json"
        ),
        loadRuntimePayload(
            "./runtime/rbl_commodities_oc.json"
        ),
        loadRuntimePayload(
            "./runtime/rbl_commodities_systemic_oc.json"
        )
    ]);
}
