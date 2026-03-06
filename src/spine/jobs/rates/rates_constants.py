"""
Deterministic constants for Spine rates jobs.
CPMAI-compliant: explicit source identifiers, canonical symbols, R2 keys.
"""
"""
Deterministic constants for Spine rates jobs.
CPMAI-compliant: explicit source identifiers, canonical symbols, R2 keys.
"""

# -----------------------------
# Environment Variables
# -----------------------------
FRED_API_KEY_ENV = "FRED_API_KEY"

R2_ACCOUNT_ID_ENV = "R2_ACCOUNT_ID"
R2_ACCESS_KEY_ID_ENV = "R2_ACCESS_KEY_ID"
R2_SECRET_ACCESS_KEY_ENV = "R2_SECRET_ACCESS_KEY"
R2_BUCKET_ENV = "R2_BUCKET"
R2_BUCKET_NAME_ENV = "R2_BUCKET_NAME"
R2_ENDPOINT_ENV = "R2_ENDPOINT"
R2_REGION_ENV = "R2_REGION"

# -----------------------------
# R2 Canonical Keys
# -----------------------------
R2_RATES_YIELDS_DAILY_T1_KEY = "spine_us/leaves/rates/rates_yields_daily_t1.parquet"
R2_RATES_YIELDS_MONTHLY_T1_KEY = "spine_us/leaves/rates/rates_yields_monthly_t1.parquet"
R2_RATES_SPREADS_T2_KEY = "spine_us/leaves/rates/rates_spreads_t2.parquet"

# -----------------------------
# Freshness Expectations
# -----------------------------
DAILY_RATES_MAX_LAG_DAYS = 7
MONTHLY_RATES_MAX_LAG_DAYS = 45
RATES_SPREADS_MAX_LAG_DAYS = 45

# -----------------------------
# Daily T1 Series
# -----------------------------
# True daily U.S. Treasury benchmark series from FRED.
DAILY_RATES_SERIES = {
    "US02Y": "DGS2",
    "US10Y": "DGS10",
}

# -----------------------------
# Monthly T1 Series
# -----------------------------
# IMPORTANT:
# - US02Y / US10Y are true U.S. Treasury yields resampled by FRED to monthly.
# - Non-U.S. 10Y series below are true long-term government bond yields.
# - Non-U.S. short-end series below are short-rate proxies (call/interbank),
#   intentionally labeled XXST instead of XX02Y.

MONTHLY_RATES_SERIES = {
    # United States (true monthly resample of Treasury yields)
    "US02Y": "DGS2",
    "US10Y": "DGS10",

    # Germany
    "DE10Y": "IRLTLT01DEM156N",
    "DEST": "IRSTCI01DEM156N",

    # Italy
    "IT10Y": "IRLTLT01ITM156N",
    "ITST": "IRSTCI01ITM156N",

    # Japan
    "JP10Y": "IRLTLT01JPM156N",
    "JPST": "IRSTCI01JPM156N",

    # United Kingdom
    "UK10Y": "IRLTLT01GBM156N",
    "UKST": "IRSTCI01GBM156N",

    # Switzerland
    "CH10Y": "IRLTLT01CHM156N",
    "CHST": "IRSTCI01CHM156N",

    # Canada
    "CA10Y": "IRLTLT01CAM156N",
    "CAST": "IRSTCI01CAM156N",

    # Australia
    "AU10Y": "IRLTLT01AUM156N",
    "AUST": "IRSTCI01AUM156N",

    # New Zealand
    "NZ10Y": "IRLTLT01NZM156N",
    "NZST": "IRSTCI01NZM156N",

    # Sweden
    "SE10Y": "IRLTLT01SEM156N",
    "SEST": "IRSTCI01SEM156N",

    # Norway
    "NO10Y": "IRLTLT01NOM156N",
    "NOST": "IRSTCI01NOM156N",

}

# -----------------------------
# Derived T2 Spread Definitions
# -----------------------------
# Spread symbol -> (left_symbol, right_symbol)
RATES_SPREADS_DAILY = {
    "US10Y_US02Y": ("US10Y", "US02Y"),
}

RATES_SPREADS_MONTHLY = {
    "US10Y_US02Y_M": ("US10Y", "US02Y"),
    "DE10Y_DEST": ("DE10Y", "DEST"),
    "IT10Y_ITST": ("IT10Y", "ITST"),
    "JP10Y_JPST": ("JP10Y", "JPST"),
    "UK10Y_UKST": ("UK10Y", "UKST"),
    "CH10Y_CHST": ("CH10Y", "CHST"),
    "CA10Y_CAST": ("CA10Y", "CAST"),
    "AU10Y_AUST": ("AU10Y", "AUST"),
    "NZ10Y_NZST": ("NZ10Y", "NZST"),
    "SE10Y_SEST": ("SE10Y", "SEST"),
    "NO10Y_NOST": ("NO10Y", "NOST"),

    # Cross-country macro spreads
    "IT10Y_DE10Y": ("IT10Y", "DE10Y"),
    "US10Y_DE10Y": ("US10Y", "DE10Y"),
    "US10Y_JP10Y": ("US10Y", "JP10Y"),
    "US10Y_UK10Y": ("US10Y", "UK10Y"),
}

