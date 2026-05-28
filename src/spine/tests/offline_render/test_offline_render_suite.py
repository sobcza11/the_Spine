from pathlib import Path


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\offline_render"
)

FILES = [
    "contradiction_matrix_dashboard.html",
    "geoscen_sovereign_dashboard.html",
    "runtime_health_dashboard.html",
    "historical_analog_dashboard.html",
    "live_refresh_simulation.json",
    "local_websocket_runtime.json",
    "real_rag_integration.json",
    "real_langroid_execution.json",
]


def test_offline_render_suite():

    missing = []

    for name in FILES:

        path = ROOT / name

        if not path.exists():
            missing.append(name)

    assert not missing, f"Missing render artifacts: {missing}"
