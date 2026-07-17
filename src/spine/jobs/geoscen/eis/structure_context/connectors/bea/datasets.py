from __future__ import annotations

APPROVED_DATASETS = {"NIPA", "Regional"}
NIPA_ALLOWED_PARAMS = {"TableName", "Frequency", "Year"}
REGIONAL_ALLOWED_PARAMS = {"TableName", "LineCode", "GeoFips", "Year"}
NIPA_FREQUENCIES = {"A", "Q", "M"}
MAX_LIST_VALUES = 50
MAX_IDENTIFIER_LEN = 80
MAX_STRING_LEN = 240
