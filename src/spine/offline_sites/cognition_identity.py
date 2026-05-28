COUNTRY_IDENTITY = {
    "US": {
        "identity": "global_discount_rate_anchor",
        "transmission": [
            "rates",
            "usd_liquidity",
            "equities",
            "commodities",
        ],
    },

    "JP": {
        "identity": "carry_regime",
        "transmission": [
            "usdjpy",
            "funding_stress",
            "global_liquidity",
        ],
    },

    "DE": {
        "identity": "euro_core_anchor",
        "transmission": [
            "ecb",
            "eur",
            "peripheral_spreads",
        ],
    },

    "CN": {
        "identity": "liquidity_growth_hybrid",
        "transmission": [
            "commodities",
            "fx",
            "global_growth",
        ],
    },

    "CA": {
        "identity": "oil_housing_credit",
        "transmission": [
            "crude",
            "cad",
            "housing",
        ],
    },
}


FX_IDENTITY = {
    "AUDUSD": {
        "identity": "commodity_risk_pair",
        "transmission": [
            "china",
            "commodities",
            "risk_appetite",
        ],
    },

    "USDJPY": {
        "identity": "carry_transmission_pair",
        "transmission": [
            "boj",
            "us_rates",
            "funding_stress",
        ],
    },

    "EURUSD": {
        "identity": "fed_ecb_divergence_pair",
        "transmission": [
            "ecb",
            "eur",
            "rates_divergence",
        ],
    },
}


INDEX_IDENTITY = {
    "SPY": {
        "identity": "broad_market_reference",
    },

    "QQQ": {
        "identity": "growth_duration_reference",
    },

    "IWM": {
        "identity": "domestic_fragility_reference",
    },
}


SECTOR_IDENTITY = {
    "XLF": {
        "identity": "credit_liquidity_transmission",
    },

    "XLE": {
        "identity": "inflation_energy_pressure",
    },

    "XLK": {
        "identity": "duration_sensitive_growth",
    },

    "XLRE": {
        "identity": "financing_fragility",
    },
}
