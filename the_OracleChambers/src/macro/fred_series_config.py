# src/macro/fred_series_config.py

"""
Configuration of FRED series to ingest for the_Spine Macro Engine.

NOTE:
- IDs here are for core, stable FRED series.
- You can extend this list as needed.
"""

FRED_SERIES = [
    # === Rates & Curve ===
    {
        "series_id": "DGS10",
        "name": "us_10y_yield",
        "group": "rates",
        "freq": "D",  # original frequency
    },
    {
        "series_id": "DGS2",
        "name": "us_2y_yield",
        "group": "rates",
        "freq": "D",
    },
    {
        "series_id": "DGS3MO",
        "name": "us_3m_yield",
        "group": "rates",
        "freq": "D",
    },
    {
        "series_id": "T10Y2Y",
        "name": "us_10y_2y_spread",
        "group": "curve_spread",
        "freq": "D",
    },
    {
        "series_id": "T10Y3M",
        "name": "us_10y_3m_spread",
        "group": "curve_spread",
        "freq": "D",
    },

    # === Policy & Funding ===
    {
        "series_id": "FEDFUNDS",
        "name": "fed_funds_rate",
        "group": "policy",
        "freq": "M",
    },

    # === Labor ===
    {
        "series_id": "UNRATE",
        "name": "unemployment_rate",
        "group": "labor",
        "freq": "M",
    },

    # === Inflation & Prices ===
    {
        "series_id": "CPIAUCSL",
        "name": "cpi_all_items",
        "group": "inflation",
        "freq": "M",
    },
    {
        "series_id": "PCEPI",
        "name": "pce_deflator",
        "group": "inflation",
        "freq": "M",
    },

    # === Energy ===
    {
        "series_id": "DCOILWTICO",
        "name": "wti_spot_price",
        "group": "energy",
        "freq": "D",
    },
]
