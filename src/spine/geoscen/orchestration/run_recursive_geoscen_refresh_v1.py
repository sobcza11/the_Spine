from pathlib import Path
from datetime import datetime, UTC
import importlib
import json
import traceback
import pandas as pd


RECURSIVE_PIPELINE = [

    # =====================================================
    # CORE RECURSIVE LAYERS
    # =====================================================

    {
        "step": 1,
        "layer": "systemic_escalation_registry",
        "module": "spine.geoscen.recursive.build_geoscen_systemic_escalation_registry_v1",
        "function": "build_geoscen_systemic_escalation_registry_v1",
    },

    {
        "step": 2,
        "layer": "recursive_escalation_engine",
        "module": "spine.geoscen.recursive.build_recursive_escalation_engine_v1",
        "function": "build_recursive_escalation_engine_v1",
    },

    {
        "step": 3,
        "layer": "systemic_fragility_state_machine",
        "module": "spine.geoscen.recursive.build_systemic_fragility_state_machine_v1",
        "function": "build_systemic_fragility_state_machine_v1",
    },

    {
        "step": 4,
        "layer": "recursive_topology_memory",
        "module": "spine.geoscen.recursive.build_recursive_topology_memory_engine_v1",
        "function": "build_recursive_topology_memory_engine_v1",
    },

    {
        "step": 5,
        "layer": "recursive_contagion_propagation",
        "module": "spine.geoscen.recursive.build_recursive_contagion_propagation_engine_v1",
        "function": "build_recursive_contagion_propagation_engine_v1",
    },

    {
        "step": 6,
        "layer": "recursive_governance",
        "module": "spine.geoscen.recursive.build_geoscen_recursive_governance_v1",
        "function": "build_geoscen_recursive_governance_v1",
    },

    {
        "step": 7,
        "layer": "recursive_topology",
        "module": "spine.geoscen.recursive.build_recursive_geoscen_topology_v1",
        "function": "build_recursive_geoscen_topology_v1",
    },

    # =====================================================
    # DOMAIN RECURSION
    # =====================================================

    {
        "step": 8,
        "layer": "finstate_recursive",
        "module": "spine.finstate.recursive.build_finstate_recursive_integration_v1",
        "function": "build_finstate_recursive_integration_v1",
    },

    {
        "step": 9,
        "layer": "rates_recursive",
        "module": "spine.rates.recursive.build_rates_recursive_pressure_engine_v1",
        "function": "build_rates_recursive_pressure_engine_v1",
    },

    {
        "step": 10,
        "layer": "fx_recursive",
        "module": "spine.fx.recursive.build_fx_recursive_stress_engine_v1",
        "function": "build_fx_recursive_stress_engine_v1",
    },

    # =====================================================
    # CROSS-DOMAIN RECURSION
    # =====================================================

    {
        "step": 11,
        "layer": "cross_domain_recursive_fusion",
        "module": "spine.geoscen.fusion.build_cross_domain_recursive_fusion_v1",
        "function": "build_cross_domain_recursive_fusion_v1",
    },

    {
        "step": 12,
        "layer": "recursive_regime_memory",
        "module": "spine.geoscen.fusion.build_recursive_regime_memory_v1",
        "function": "build_recursive_regime_memory_v1",
    },

    # =====================================================
    # PROJECTION
    # =====================================================

    {
        "step": 13,
        "layer": "recursive_scenario_projection",
        "module": "spine.geoscen.projection.build_recursive_scenario_projection_v1",
        "function": "build_recursive_scenario_projection_v1",
    },

    # =====================================================
    # NARRATIVE
    # =====================================================

    {
        "step": 14,
        "layer": "recursive_narrative_cognition",
        "module": "spine.geoscen.narrative.build_recursive_narrative_cognition_scaffold_v1",
        "function": "build_recursive_narrative_cognition_scaffold_v1",
    },

    # =====================================================
    # EXECUTIVE
    # =====================================================

    {
        "step": 15,
        "layer": "recursive_executive_synthesis",
        "module": "spine.geoscen.executive.build_recursive_executive_synthesis_pack_v1",
        "function": "build_recursive_executive_synthesis_pack_v1",
    },

    # =====================================================
    # POLICY
    # =====================================================

    {
        "step": 16,
        "layer": "recursive_policy_response",
        "module": "spine.geoscen.policy.build_recursive_policy_response_layer_v1",
        "function": "build_recursive_policy_response_layer_v1",
    },

    # =====================================================
    # GLOBAL
    # =====================================================

    {
        "step": 17,
        "layer": "global_recursive_ae_em_expansion",
        "module": "spine.geoscen.global_expansion.build_global_recursive_ae_em_expansion_v1",
        "function": "build_global_recursive_ae_em_expansion_v1",
    },

    # =====================================================
    # SCENARIO TREE
    # =====================================================

    {
        "step": 18,
        "layer": "recursive_scenario_tree_generation",
        "module": "spine.geoscen.scenario_tree.build_recursive_scenario_tree_generation_v1",
        "function": "build_recursive_scenario_tree_generation_v1",
    },
]


def run_recursive_geoscen_refresh_v1():

    repo_root = Path.cwd()

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "orchestration"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    orchestration_rows = []

    success_count = 0
    failed_count = 0

    for item in RECURSIVE_PIPELINE:

        step = item["step"]
        layer = item["layer"]
        module_name = item["module"]
        function_name = item["function"]

        start_ts = datetime.now(UTC)

        try:

            module = importlib.import_module(module_name)

            fn = getattr(module, function_name)

            fn()

            status = "success"

            success_count += 1

            error_message = None

        except Exception as e:

            status = "failed"

            failed_count += 1

            error_message = str(e)

            traceback.print_exc()

        end_ts = datetime.now(UTC)

        orchestration_rows.append(
            {
                "step": step,
                "layer": layer,
                "module": module_name,
                "function": function_name,
                "status": status,
                "started_at_utc": start_ts.isoformat(),
                "completed_at_utc": end_ts.isoformat(),
                "runtime_seconds": round(
                    (end_ts - start_ts).total_seconds(),
                    4,
                ),
                "error_message": error_message,
            }
        )

    orchestration_df = pd.DataFrame(orchestration_rows)

    overall_status = (
        "recursive_runtime_operational"
        if failed_count == 0
        else "recursive_runtime_partial_failure"
    )

    summary = {
        "component": "recursive_geoscen_orchestration_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "pipeline_steps": int(len(orchestration_df)),
        "successful_steps": int(success_count),
        "failed_steps": int(failed_count),
        "overall_status": overall_status,
        "runtime_seconds_total": round(
            float(orchestration_df["runtime_seconds"].sum()),
            4,
        ),
        "successful_layers": orchestration_df.loc[
            orchestration_df["status"] == "success",
            "layer",
        ].tolist(),
        "failed_layers": orchestration_df.loc[
            orchestration_df["status"] == "failed",
            "layer",
        ].tolist(),
        "status": "recursive_geoscen_orchestration_complete",
    }

    parquet_path = (
        out_dir
        / "recursive_geoscen_orchestration_v1.parquet"
    )

    json_path = (
        out_dir
        / "recursive_geoscen_orchestration_v1.json"
    )

    summary_path = (
        out_dir
        / "recursive_geoscen_orchestration_summary_v1.json"
    )

    orchestration_df.to_parquet(
        parquet_path,
        index=False,
    )

    orchestration_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive GeoScen orchestration complete")
    print("Pipeline Steps:", len(orchestration_df))
    print("Successful Steps:", success_count)
    print("Failed Steps:", failed_count)
    print("Overall Status:", overall_status)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return orchestration_df


if __name__ == "__main__":
    run_recursive_geoscen_refresh_v1()
