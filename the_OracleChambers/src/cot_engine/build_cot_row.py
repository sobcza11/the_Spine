"""Build a single tidy COT row for one (report_date, code)."""

from __future__ import annotations

import numpy as np
import pandas as pd

# Map parsed Category labels to prefixes (adjust to match your real categories)
TRADER_MAP: dict[str, str] = {
    "Managed Money": "mm",
    "Producer/Merchant/Processor/User": "pm",
    "Swap Dealers": "sd",
    "Other Reportables": "or",
    "Nonreportable Positions": "nr",
}


def safe_get_row(df: pd.DataFrame | None, category: str) -> pd.Series | None:
    """
    Return the first row matching 'Category' == category, or None if not found.
    """
    if df is None or df.empty:
        return None
    rows = df[df.get("Category") == category]
    if rows.empty:
        return None
    return rows.iloc[0]


def build_cot_row(
    meta: dict,
    positions: pd.DataFrame | None,
    changes: pd.DataFrame | None,
    percents: pd.DataFrame | None,
    num_traders: pd.DataFrame | None,
    largest: pd.DataFrame | None,
) -> pd.Series:
    """
    Build a tidy COT row using metadata & parsed tables.

    Returns:
        pandas.Series with:
        - identifiers (report_date, commodity, exchange, code, ...)
        - open interest & change
        - per-category long/short/net & % of OI
        - concentration metrics (top4/8 long/short)
        - aggregate speculative net measures
    """
    row: dict[str, float | str | None] = {}

    # --- identifiers ---
    row["report_date"] = meta.get("date")
    row["commodity"] = meta.get("commodity")
    row["exchange"] = meta.get("exchange")
    row["code"] = meta.get("code")
    row["cot_report_type"] = meta.get("report_type", "Other (Combined)")
    row["contract_type"] = meta.get("contract_type", "")
    row["source"] = "CFTC_OTHER_LONG"

    # --- open interest (All category) ---
    all_pos = safe_get_row(positions, "All")
    if all_pos is not None:
        try:
            row["oi_total"] = float(all_pos.get("Open Interest", np.nan))
        except Exception:
            row["oi_total"] = np.nan
    else:
        row["oi_total"] = np.nan

    if changes is not None and not changes.empty:
        try:
            row["oi_change"] = float(changes.get("Change in Open Interest").iloc[0])
        except Exception:
            row["oi_change"] = np.nan
    else:
        row["oi_change"] = np.nan

    # --- trader categories (net & % of OI) ---
    for cat_label, prefix in TRADER_MAP.items():
        pos_row = safe_get_row(positions, cat_label)
        pct_row = safe_get_row(percents, cat_label)

        long_col = f"{prefix}_long"
        short_col = f"{prefix}_short"
        net_col = f"{prefix}_net"
        long_pct_col = f"{prefix}_long_pct_oi"
        short_pct_col = f"{prefix}_short_pct_oi"

        if pos_row is not None:
            try:
                row[long_col] = float(pos_row.get("Long", np.nan))
                row[short_col] = float(pos_row.get("Short", np.nan))
                row[net_col] = row[long_col] - row[short_col]
            except Exception:
                row[long_col] = row[short_col] = row[net_col] = np.nan
        else:
            row[long_col] = row[short_col] = row[net_col] = np.nan

        if pct_row is not None:
            try:
                row[long_pct_col] = float(pct_row.get("Long %", np.nan))
                row[short_pct_col] = float(pct_row.get("Short %", np.nan))
            except Exception:
                row[long_pct_col] = row[short_pct_col] = np.nan
        else:
            row[long_pct_col] = row[short_pct_col] = np.nan

    # --- concentration metrics from largest (All category) ---
    largest_all = safe_get_row(largest, "All")
    if largest_all is not None:
        for col_name, out_name in [
            ("4 or Less Long %", "top4_long_pct"),
            ("4 or Less Short %", "top4_short_pct"),
            ("8 or Less Long %", "top8_long_pct"),
            ("8 or Less Short %", "top8_short_pct"),
        ]:
            try:
                row[out_name] = float(largest_all.get(col_name, np.nan))
            except Exception:
                row[out_name] = np.nan
    else:
        row["top4_long_pct"] = row["top4_short_pct"] = np.nan
        row["top8_long_pct"] = row["top8_short_pct"] = np.nan

    # --- aggregate speculative net ---
    mm_net = row.get("mm_net", np.nan)
    or_net = row.get("or_net", np.nan)
    oi_total = row.get("oi_total", np.nan)

    if not np.isnan(mm_net) or not np.isnan(or_net):
        row["spec_net_all"] = (
            (0 if np.isnan(mm_net) else mm_net)
            + (0 if np.isnan(or_net) else or_net)
        )
    else:
        row["spec_net_all"] = np.nan

    if (
        not np.isnan(row["spec_net_all"])
        and not np.isnan(oi_total)
        and oi_total != 0
    ):
        row["spec_net_pct_oi"] = row["spec_net_all"] / oi_total
    else:
        row["spec_net_pct_oi"] = np.nan

    if not np.isnan(mm_net) and not np.isnan(oi_total) and oi_total != 0:
        row["mm_net_pct_oi"] = mm_net / oi_total
    else:
        row["mm_net_pct_oi"] = np.nan

    return pd.Series(row)
