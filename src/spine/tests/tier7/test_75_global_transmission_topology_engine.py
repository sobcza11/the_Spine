from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\global_transmission_topology_engine.json"
)

def test_global_transmission_topology_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "global-transmission-topology-engine"
    assert d["transmission_topology_enabled"] is True
    assert d["transmission_domain_count"] > 0

    assert d["transmission_contract"]["cross_market_linkages_visible"] is True
    assert d["transmission_contract"]["regional_spillover_supported"] is True
    assert d["transmission_contract"]["contagion_paths_supported"] is True

    assert d["governance"]["transmission_topology_governed"] is True
    assert d["governance"]["deterministic_inputs_authoritative"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
