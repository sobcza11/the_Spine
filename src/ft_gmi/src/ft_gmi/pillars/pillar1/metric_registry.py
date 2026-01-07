"""
FT-GMI Pillar 1 Metric Registry

Explicit registry of diagnostic metrics.
"""

METRIC_REGISTRY = {
    "financial_stress": {
        "description": "Composite financial stress indicator",
        "direction": "higher = more stress",
    },
    "policy_pressure": {
        "description": "Monetary / fiscal policy strain indicator",
        "direction": "higher = more pressure",
    },
}
