"""Shared, network-free canonicalization for governed Tiingo index observations."""

from __future__ import annotations

import pandas as pd


CANONICAL_COLUMNS = (
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_volume",
    "div_cash",
    "split_factor",
    "source",
)
TIINGO_TO_CANONICAL = {
    "adjOpen": "adj_open",
    "adjHigh": "adj_high",
    "adjLow": "adj_low",
    "adjClose": "adj_close",
    "adjVolume": "adj_volume",
    "divCash": "div_cash",
    "splitFactor": "split_factor",
}
NUMERIC_COLUMNS = CANONICAL_COLUMNS[2:-1]
PRICE_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
)
VOLUME_COLUMNS = ("volume", "adj_volume")


def canonicalize_tiingo_daily(frame: pd.DataFrame, instrument_id: str) -> pd.DataFrame:
    """Preserve separate raw, adjusted, dividend, and split observations."""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)
    if "date" not in frame.columns:
        raise ValueError("INDEX_CANONICALIZATION_DATE_MISSING")

    result = frame.rename(columns=TIINGO_TO_CANONICAL).copy()
    result["symbol"] = instrument_id.upper()
    result["date"] = pd.to_datetime(
        result["date"], errors="raise", utc=True, format="mixed"
    ).dt.tz_convert(None).dt.floor("D")
    result["source"] = "tiingo"
    for column in NUMERIC_COLUMNS:
        if column not in result:
            raise ValueError(f"INDEX_CANONICALIZATION_FIELD_MISSING:{column}")
        result[column] = pd.to_numeric(result[column], errors="raise")
    if result[list(NUMERIC_COLUMNS)].isna().any().any():
        raise ValueError("INDEX_CANONICALIZATION_NUMERIC_NULL")
    if (result[list(PRICE_COLUMNS)] <= 0).any().any():
        raise ValueError("INDEX_CANONICALIZATION_PRICE_INVALID")
    if (result[list(VOLUME_COLUMNS)] < 0).any().any():
        raise ValueError("INDEX_CANONICALIZATION_VOLUME_INVALID")
    if (result["split_factor"] <= 0).any():
        raise ValueError("INDEX_CORPORATE_ACTION_SPLIT_FACTOR_INVALID")

    result = result.loc[:, list(CANONICAL_COLUMNS)]
    result = result.sort_values(["symbol", "date"], kind="mergesort")
    if result.duplicated(["symbol", "date"]).any():
        raise ValueError("INDEX_CANONICALIZATION_OBSERVATION_KEY_DUPLICATE")
    return result.reset_index(drop=True)
