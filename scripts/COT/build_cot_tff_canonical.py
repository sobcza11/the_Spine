
#!/usr/bin/env python
"""
Build canonical CFTC COT leaf (TFF Futures Only) for the_Spine.

Input:
    data/spine_us/raw_cftc_tff/*.csv   (CFTC TFF - Futures Only file)

Output:
    data/spine_us/us_cftc_cot_tff_canonical.parquet
    R2: spine_us/us_cftc_cot_tff_canonical.parquet

Grain:
    one row per [as_of_date, spine_symbol, trader_group]
"""

import os
import sys
import logging
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd

from utils.storage_r2 import upload_file_to_r2  # requires PYTHONPATH=src

log = logging.getLogger("SCRIPT-COT")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------


@dataclass
class CftcConfig:
    universe_csv: str = "metadata/cftc_cot_universe.csv"
    data_dir: str = "data/spine_us/raw_cftc_tff"
    output_local_path: str = "data/spine_us/us_cftc_cot_tff_canonical.parquet"
    r2_key: str = "spine_us/us_cftc_cot_tff_canonical.parquet"
    start_year: int = 2006
    end_year: int = 2025
    raw_base_url: str = ""  # not used yet; manual download via CFTC UI


# ---------------------------------------------------------------------
# Trader group mapping (raw → canonical)
# ---------------------------------------------------------------------

TRADER_GROUP_MAP = {
    # These must match the *prefixes* in the TFF column names
    # e.g. Dealer_Intermediary_Long_All, Asset_Mgr_Inst_Short_All, etc.
    "Dealer_Intermediary": "DEALER",
    "Asset_Mgr_Inst": "ASSET_MANAGER",
    "Lev_Money": "LEVERAGED_FUNDS",
    "Other_Reportables": "OTHER_REPORTABLES",
    "Nonreportable_Positions": "NON_REPORTABLES",
}


# ---------------------------------------------------------------------
# 1. Universe mapping
# ---------------------------------------------------------------------


def load_universe_mapping(cfg: CftcConfig) -> pd.DataFrame:
    """
    Load mapping between CFTC market codes/names and Spine symbols.

    metadata/cftc_cot_universe.csv expected columns:
        cftc_market_code
        cftc_market_name
        exchange
        spine_symbol
        asset_class
        active (0/1)

    We coerce cftc_market_code to string to match the normalized TFF data.
    """
    log.info("[COT] Loading universe mapping from %s", cfg.universe_csv)
    if not os.path.exists(cfg.universe_csv):
        raise FileNotFoundError(
            f"Universe CSV not found at {cfg.universe_csv}. "
            "Create metadata/cftc_cot_universe.csv with your mapping."
        )

    df_uni = pd.read_csv(cfg.universe_csv)

    # Keep only active = 1 if present
    if "active" in df_uni.columns:
        df_uni = df_uni[df_uni["active"] == 1].copy()

    # Critical: ensure type matches normalized TFF (string)
    df_uni["cftc_market_code"] = (
        df_uni["cftc_market_code"].astype(str).str.strip()
    )

    # Optional: clean name/other fields
    if "cftc_market_name" in df_uni.columns:
        df_uni["cftc_market_name"] = df_uni["cftc_market_name"].astype(str).str.strip()
    if "exchange" in df_uni.columns:
        df_uni["exchange"] = df_uni["exchange"].astype(str).str.strip()
    if "spine_symbol" in df_uni.columns:
        df_uni["spine_symbol"] = df_uni["spine_symbol"].astype(str).str.strip()
    if "asset_class" in df_uni.columns:
        df_uni["asset_class"] = df_uni["asset_class"].astype(str).str.strip()

    return df_uni


# ---------------------------------------------------------------------
# 2. Raw file handling
# ---------------------------------------------------------------------


def ensure_raw_data(cfg: CftcConfig) -> None:
    """
    Ensure the raw data directory exists.

    You manually place the CFTC TFF Futures-Only CSV (e.g. TFF_-_Futures_Only.csv)
    into data/spine_us/raw_cftc_tff.
    """
    os.makedirs(cfg.data_dir, exist_ok=True)
    log.info("[COT] Using raw TFF directory: %s", cfg.data_dir)


def load_raw_tff_files(cfg: CftcConfig) -> pd.DataFrame:
    """
    Load all TFF Futures-only CSV/TXT files from cfg.data_dir into a single DataFrame.
    """
    log.info("[COT] Loading raw TFF files from %s", cfg.data_dir)
    files = [
        os.path.join(cfg.data_dir, f)
        for f in os.listdir(cfg.data_dir)
        if f.lower().endswith((".csv", ".txt"))
    ]

    if not files:
        raise FileNotFoundError(
            f"No raw CFTC TFF CSV/TXT files found in {cfg.data_dir}. "
            "Download 'TFF - Futures Only' from CFTC and drop here."
        )

    dfs = []
    for fp in sorted(files):
        log.info("[COT] Reading raw TFF file: %s", fp)
        df = pd.read_csv(fp)
        dfs.append(df)

    raw = pd.concat(dfs, ignore_index=True)
    log.info("[COT] Loaded raw TFF rows: %d", len(raw))
    return raw


# ---------------------------------------------------------------------
# 3. Normalization of raw TFF columns
# ---------------------------------------------------------------------


def normalize_tff_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names & key fields for CFTC TFF data.

    Handles several possible variants:
        - Report_Date_as_YYYYMMDD
        - Report_Date_as_YYYY_MM_DD
        - Report_Date_as_YYYY-MM-DD
        - any col containing "Report_Date"

        - CFTC_Market_Code or CFTC_Contract_Market_Code
        - Market_and_Exchange_Name or CONTRACT_MARKET_NAME or Contract_Market_Name
        - Open_Interest_All / Open_Interest_All_All / missing

    Also makes final column selection robust: only selects columns that exist.
    """

    # -------- 1) DATE COLUMN DETECTION --------
    date_col = None
    for cand in [
        "Report_Date_as_YYYYMMDD",
        "Report_Date_as_YYYY_MM_DD",
        "Report_Date_as_YYYY-MM-DD",
    ]:
        if cand in df.columns:
            date_col = cand
            break

    if date_col is None:
        date_candidates = [c for c in df.columns if "Report_Date" in c]
        if date_candidates:
            date_col = date_candidates[0]

    if date_col is None:
        raise ValueError(
            "No recognizable date column in raw TFF data. "
            f"Columns: {list(df.columns)}"
        )

    # -------- 2) MARKET CODE / NAME DETECTION --------
    if "CFTC_Market_Code" in df.columns:
        mc_col = "CFTC_Market_Code"
    elif "CFTC_Contract_Market_Code" in df.columns:
        mc_col = "CFTC_Contract_Market_Code"
    else:
        raise ValueError(
            "No recognizable CFTC market code column. "
            "Expected 'CFTC_Market_Code' or 'CFTC_Contract_Market_Code'. "
            f"Columns: {list(df.columns)}"
        )

    if "Market_and_Exchange_Name" in df.columns:
        mn_col = "Market_and_Exchange_Name"
    elif "CONTRACT_MARKET_NAME" in df.columns:
        mn_col = "CONTRACT_MARKET_NAME"
    elif "Contract_Market_Name" in df.columns:
        mn_col = "Contract_Market_Name"
    else:
        mn_col = None  # not fatal, we can still run with blank names

    # -------- 3) TOTAL OI DETECTION --------
    oi_col = None
    if "Open_Interest_All" in df.columns:
        oi_col = "Open_Interest_All"
    elif "Open_Interest_All_All" in df.columns:
        oi_col = "Open_Interest_All_All"

    # -------- 4) RENAME CORE COLUMNS TO CANONICAL NAMES --------
    col_map = {
        date_col: "as_of_date_raw",
        mc_col: "cftc_market_code",
    }
    if mn_col is not None:
        col_map[mn_col] = "cftc_market_name"
    if oi_col is not None:
        col_map[oi_col] = "total_oi_contracts"

    df = df.rename(columns=col_map)

    # -------- 5) PARSE DATE --------
    df["as_of_date"] = pd.to_datetime(df["as_of_date_raw"].astype(str))

    # Standardize code & name
    df["cftc_market_code"] = df["cftc_market_code"].astype(str).str.strip()
    if "cftc_market_name" in df.columns:
        df["cftc_market_name"] = df["cftc_market_name"].astype(str).str.strip()
    else:
        df["cftc_market_name"] = ""

    # If total_oi_contracts missing, create placeholder
    if "total_oi_contracts" not in df.columns:
        df["total_oi_contracts"] = np.nan

    # -------- 6) BUILD LIST OF TRADER GROUP COLUMNS --------
    base_cols = [
        "as_of_date",
        "cftc_market_code",
        "cftc_market_name",
        "total_oi_contracts",
    ]

    trader_cols = [
        c for c in df.columns if any(base in c for base in TRADER_GROUP_MAP.keys())
    ]

    if not trader_cols:
        raise ValueError(
            "No trader group columns found in TFF data. "
            f"Columns: {list(df.columns)}"
        )

    existing_base_cols = [c for c in base_cols if c in df.columns]
    selected_cols = existing_base_cols + trader_cols

    missing_for_selection = [c for c in base_cols if c not in df.columns]
    if missing_for_selection:
        import logging as _logging
        _logging.warning(
            "[COT] Some expected base columns are missing and will be skipped: %s",
            missing_for_selection,
        )

    df = df[selected_cols].copy()
    log.info(
        "[COT] Normalized TFF structure. Shape=%s. Trader cols=%d",
        df.shape,
        len(trader_cols),
    )
    return df


# ---------------------------------------------------------------------
# 4. Map CFTC markets → Spine symbols
# ---------------------------------------------------------------------


def map_to_spine_symbols(df: pd.DataFrame, df_uni: pd.DataFrame) -> pd.DataFrame:
    """
    Map raw CFTC market codes to Spine symbols & asset class.

    Primary key: cftc_market_code (string).
    If no rows match the universe, we fall back to using the CFTC market name
    as a provisional spine_symbol so the pipeline can still run.
    """
    join_cols = [
        "cftc_market_code",
        "cftc_market_name",
        "exchange",
        "spine_symbol",
        "asset_class",
    ]
    join_cols = [c for c in join_cols if c in df_uni.columns]

    df_map = df.merge(
        df_uni[join_cols],
        on="cftc_market_code",
        how="left",
        suffixes=("", "_uni"),
    )

    matched = df_map["spine_symbol"].notna().sum() if "spine_symbol" in df_map.columns else 0
    n_sym = (
        df_map["spine_symbol"].nunique(dropna=True)
        if "spine_symbol" in df_map.columns
        else 0
    )

    log.info(
        "[COT] After mapping to Spine universe: rows=%s, matched_rows=%s, symbols=%s",
        df_map.shape[0],
        matched,
        n_sym,
    )

    # Fallback if universe mapping didn't match anything
    if matched == 0:
        log.warning(
            "[COT] Universe mapping did not match any rows. "
            "Falling back to using cftc_market_name as spine_symbol."
        )
        df_map["spine_symbol"] = (
            df_map["cftc_market_name"]
            .astype(str)
            .str.upper()
            .str.replace(r"[^A-Z0-9]+", "_", regex=True)
            .str.strip("_")
        )
        if "asset_class" not in df_map.columns:
            df_map["asset_class"] = "UNKNOWN"
        if "exchange" not in df_map.columns:
            df_map["exchange"] = ""

    # Clean nulls
    if "asset_class" in df_map.columns:
        df_map["asset_class"] = df_map["asset_class"].fillna("UNKNOWN")
    if "exchange" in df_map.columns:
        df_map["exchange"] = df_map["exchange"].fillna("")

    return df_map



# ---------------------------------------------------------------------
# 5. Reshape wide trader columns → long trader_group rows
# ---------------------------------------------------------------------


def reshape_long_trader_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape wide TFF trader columns into long format by trader_group.

    We fuzzily detect long/short columns for each base group by searching
    for the base substring and 'long'/'short' (case-insensitive), instead of
    assuming exact suffixes like '_Long_All' or '_Short_All'.

    Output uses canonical column names expected by the rest of the Spine:
        long_contracts, short_contracts, net_contracts, spreading_contracts,
        group_long_pct_oi, group_short_pct_oi, group_net_pct_oi.
    """

    id_cols = [
        "as_of_date",
        "cftc_market_code",
        "cftc_market_name",
        "spine_symbol",
        "asset_class",
        "exchange",
        "total_oi_contracts",
    ]
    id_cols = [c for c in id_cols if c in df.columns]

    frames: list[pd.DataFrame] = []

    for base, category in TRADER_GROUP_MAP.items():
        # All columns that belong to this base group
        group_cols = [c for c in df.columns if base in c]
        if not group_cols:
            log.warning("[COT] No columns found for base %s", base)
            continue

        long_candidates = [c for c in group_cols if "long" in c.lower()]
        short_candidates = [c for c in group_cols if "short" in c.lower()]

        if not long_candidates or not short_candidates:
            log.warning(
                "[COT] No usable long/short columns for base %s. group_cols=%s",
                base,
                group_cols,
            )
            continue

        long_col = long_candidates[0]
        short_col = short_candidates[0]

        use_cols = id_cols + [long_col, short_col]
        grp = df[use_cols].copy()

        grp["trader_group"] = category

        # handle numeric strings like '111,379'
        long_vals = pd.to_numeric(
            grp[long_col].astype(str).str.replace(",", ""),
            errors="coerce",
        )
        short_vals = pd.to_numeric(
            grp[short_col].astype(str).str.replace(",", ""),
            errors="coerce",
        )

        grp["long_contracts"] = long_vals
        grp["short_contracts"] = short_vals
        grp["net_contracts"] = grp["long_contracts"] - grp["short_contracts"]

        # we don't have spreading or pct-of-OI columns in this CSV, so fill with NaN/0
        grp["spreading_contracts"] = 0.0
        grp["group_long_pct_oi"] = np.nan
        grp["group_short_pct_oi"] = np.nan
        grp["group_net_pct_oi"] = np.nan

        cols_canon = (
            id_cols
            + [
                "trader_group",
                "long_contracts",
                "short_contracts",
                "spreading_contracts",
                "net_contracts",
                "group_long_pct_oi",
                "group_short_pct_oi",
                "group_net_pct_oi",
            ]
        )
        frames.append(grp[cols_canon])

    if not frames:
        raise ValueError(
            "No trader groups could be constructed. "
            "Check TFF column naming and TRADER_GROUP_MAP."
        )

    df_long = pd.concat(frames, ignore_index=True)
    log.info(
        "[COT] Reshaped to long trader groups. Shape=%s, groups=%s",
        df_long.shape,
        df_long["trader_group"].unique(),
    )
    return df_long


# ---------------------------------------------------------------------
# 6. Add 1-week deltas and 52-week z-scores
# ---------------------------------------------------------------------


def add_deltas_and_rolling(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each [spine_symbol, trader_group], compute:
        - 1w deltas in net, long, short contracts
        - 52-week rolling mean/std/z-score of net_contracts
    """
    df = df.sort_values(["spine_symbol", "trader_group", "as_of_date"]).copy()

    def _by_symbol_group(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("as_of_date").copy()
        g["delta_long_contracts_1w"] = g["long_contracts"].diff()
        g["delta_short_contracts_1w"] = g["short_contracts"].diff()
        g["delta_net_contracts_1w"] = g["net_contracts"].diff()

        roll = g["net_contracts"].rolling(window=52, min_periods=26)
        g["net_contracts_52w_mean"] = roll.mean()
        g["net_contracts_52w_std"] = roll.std(ddof=0)
        g["net_contracts_zscore_52w"] = (
            (g["net_contracts"] - g["net_contracts_52w_mean"])
            / g["net_contracts_52w_std"].replace(0, np.nan)
        )
        return g

    df = (
        df.groupby(["spine_symbol", "trader_group"], group_keys=False)
        .apply(_by_symbol_group)
        .reset_index(drop=True)
    )

    return df


# ---------------------------------------------------------------------
# 7. Full pipeline assembly
# ---------------------------------------------------------------------


def build_canonical_tff(cfg: CftcConfig) -> pd.DataFrame:
    """
    Full pipeline: raw TFF → normalized → mapped → long → enriched canonical leaf.
    """
    ensure_raw_data(cfg)
    df_uni = load_universe_mapping(cfg)
    df_raw = load_raw_tff_files(cfg)
    df_norm = normalize_tff_columns(df_raw)
    df_map = map_to_spine_symbols(df_norm, df_uni)
    df_long = reshape_long_trader_groups(df_map)
    df_feat = add_deltas_and_rolling(df_long)

    df_feat["source_report_type"] = "TFF_FUT_ONLY"
    ts_now = pd.Timestamp.utcnow()
    df_feat["created_at_utc"] = ts_now
    df_feat["updated_at_utc"] = ts_now

    col_order = [
        "as_of_date",
        "spine_symbol",
        "asset_class",
        "trader_group",
        "cftc_market_code",
        "cftc_market_name",
        "exchange",
        "total_oi_contracts",
        "long_contracts",
        "short_contracts",
        "spreading_contracts",
        "net_contracts",
        "group_long_pct_oi",
        "group_short_pct_oi",
        "group_net_pct_oi",
        "delta_long_contracts_1w",
        "delta_short_contracts_1w",
        "delta_net_contracts_1w",
        "net_contracts_52w_mean",
        "net_contracts_52w_std",
        "net_contracts_zscore_52w",
        "source_report_type",
        "created_at_utc",
        "updated_at_utc",
    ]

    df_feat = df_feat[col_order].sort_values(
        ["as_of_date", "spine_symbol", "trader_group"]
    )

    log.info(
        "[COT] Canonical TFF leaf built. rows=%d, symbols=%d",
        len(df_feat),
        df_feat["spine_symbol"].nunique(),
    )
    return df_feat


# ---------------------------------------------------------------------
# 8. Persistence
# ---------------------------------------------------------------------


def write_canonical_to_disk_and_r2(df: pd.DataFrame, cfg: CftcConfig) -> None:
    os.makedirs(os.path.dirname(cfg.output_local_path), exist_ok=True)
    print("DEBUG: writing canonical COT to", cfg.output_local_path)
    df.to_parquet(cfg.output_local_path, index=False)
    log.info("[COT] Wrote canonical COT leaf to %s", cfg.output_local_path)

    upload_file_to_r2(cfg.output_local_path, cfg.r2_key)
    log.info("[COT] Uploaded canonical COT leaf to R2 key=%s", cfg.r2_key)


# ---------------------------------------------------------------------
# 9. CLI
# ---------------------------------------------------------------------


def main(argv: List[str] | None = None) -> int:
    log.info("[SCRIPT-COT] Building canonical CFTC TFF COT leaf …")
    cfg = CftcConfig()
    df_canon = build_canonical_tff(cfg)
    write_canonical_to_disk_and_r2(df_canon, cfg)
    log.info("[SCRIPT-COT] Done – canonical COT TFF leaf built successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))



