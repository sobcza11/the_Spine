import os

# ============================================================
# ENERGY DOMAIN CONSTANTS (CPMAI EXPLICIT PROVENANCE)
# ============================================================

# -----------------------------
# EIA Series Identifiers
# -----------------------------
# Daily Cushing WTI Spot Price (USD per Barrel)
# Official EIA Series ID
EIA_WTI_SERIES_ID = "PET.RWTC.D"

# -----------------------------
# Canonical Symbols
# -----------------------------
WTI_SYMBOL = "WTI"

# -----------------------------
# R2 Canonical Keys
# -----------------------------
R2_WTI_PRICE_T1_KEY = "spine_us/leaves/energy/wti_price_t1.parquet"

# -----------------------------
# Freshness Expectations
# -----------------------------
WTI_MAX_LAG_DAYS = 7  # allow weekends + EIA publication lag

# -----------------------------
# Environment Variables (Explicit)
# -----------------------------
EIA_API_KEY_ENV = "EIA_API_KEY"

R2_ACCOUNT_ID_ENV = "R2_ACCOUNT_ID"
R2_ACCESS_KEY_ID_ENV = "R2_ACCESS_KEY_ID"
R2_SECRET_ACCESS_KEY_ENV = "R2_SECRET_ACCESS_KEY"
R2_BUCKET_ENV = "R2_BUCKET"
R2_BUCKET_NAME_ENV = "R2_BUCKET_NAME"
R2_ENDPOINT_ENV = "R2_ENDPOINT"
R2_REGION_ENV = "R2_REGION"

