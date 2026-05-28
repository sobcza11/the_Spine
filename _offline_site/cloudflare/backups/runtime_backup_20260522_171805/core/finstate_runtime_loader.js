async function loadFinStateRuntime(){

    return await Promise.all([
        loadRuntimePayload(
            "./finstate_runtime/finstate_runtime_registry_v1.json"
        ),
        loadRuntimePayload(
            "./finstate_payloads/i2_survivability_loader_v1.json"
        ),
        loadRuntimePayload(
            "./finstate_payloads/quarterly_deterioration_propagation_v1.json"
        ),
        loadRuntimePayload(
            "./finstate_payloads/corporate_contagion_engine_v1.json"
        ),
        loadRuntimePayload(
            "./finstate_payloads/survivability_memory_system_v1.json"
        ),
        loadRuntimePayload(
            "./finstate_payloads/recursive_pressure_conditioning_v1.json"
        ),
        loadRuntimePayload(
            "./finstate_payloads/sector_survivability_ranking_v1.json"
        ),
        loadRuntimePayload(
            "./finstate_payloads/recursive_survivability_graph_v1.json"
        ),
        loadRuntimePayload(
            "./finstate_payloads/finstate_executive_synthesis_v1.json"
        )
    ]);
}
