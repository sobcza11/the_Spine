"""
FT-GMI Pillar 1 Output Schema

Diagnostic contract only.
No forecasting, no scoring logic.
"""

PILLAR1_OUTPUT_SCHEMA = {
    "date": "datetime64[ns]",
    "region": "string",
    "pillar": "string",
    "metric": "string",
    "value": "float64",
    "units": "string",
    "source": "string",
}
