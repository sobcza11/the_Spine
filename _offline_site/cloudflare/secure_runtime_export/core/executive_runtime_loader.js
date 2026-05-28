async function loadExecutiveRuntime(){

    return await Promise.all([
        loadRuntimePayload("./executive_runtime/executive_runtime_control_center_v1.json"),
        loadRuntimePayload("./executive_runtime/executive_regime_transition_engine_v1.json"),
        loadRuntimePayload("./executive_runtime/multi_route_synchronization_engine_v1.json"),
        loadRuntimePayload("./executive_runtime/global_systemic_pressure_engine_v1.json"),
        loadRuntimePayload("./executive_runtime/macro_corporate_commodity_fusion_v1.json"),
        loadRuntimePayload("./executive_runtime/dynamic_regime_probability_engine_v1.json"),
        loadRuntimePayload("./geoscen_runtime/geoscen_rbl_synthesis_v1_normalized.json"),
        loadRuntimePayload("./finstate_payloads/finstate_executive_synthesis_v1.json"),
        loadRuntimePayload("./finstate_payloads/recursive_survivability_graph_v1.json"),
        loadRuntimePayload("./langroid_runtime/langroid_runtime_export_normalized.json")
    ]);
}
