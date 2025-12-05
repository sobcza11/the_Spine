"""
Block 4 (1986–1989) — Controlled Legacy COT Extension

Design intent:
- Standardize legacy COT files into a consistent weekly schema
- Produce monthly aggregates consistent with the Spine monthly cadence
- Enforce structural gating:
    support_only = True
    eligible_for_state = False
- Allow lag support (YoY scaffolding) without letting Block 4 participate in regime inference
"""

from __future__ import annotations

import argparse
import glob
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

import zipfile
import io

# -----------------------------
# Config / Contracts
# -----------------------------

@dataclass(frozen=True)
class Block4Policy:
    block_id: int = 4
    era: str = "1986_1989"
    support_only: bool = True
    eligible_for_state: bool = False
    structural_regime: str = "pre_modern_cot"


# Column aliases seen in many COT exports (legacy & modern)
COL_ALIASES: Dict[str, str] = {
    # ---- Block4 legacy (deacot1986+ "Historical Compressed") headers ----
    "Market and Exchange Names": "market_and_exchange_names",
    "CFTC Contract Market Code": "cftc_contract_market_code",
    "As of Date in Form YYYY-MM-DD": "report_date",

    "Open Interest (All)": "open_interest_all",

    "Noncommercial Positions-Long (All)": "noncomm_long",
    "Noncommercial Positions-Short (All)": "noncomm_short",

    "Commercial Positions-Long (All)": "comm_long",
    "Commercial Positions-Short (All)": "comm_short",

    # traders (optional but useful)
    "Traders-Total (All)": "traders_total",

    # concentration (optional; keep only if you want top4_long_pct as pct)
    "Concentration-Gross LT = 4 TDR-Long (All)": "top4_long_pct",

    # dates
    "report_date_as_yyyy_mm_dd": "report_date",
    "Report_Date_as_YYYY-MM-DD": "report_date",
    "report_date": "report_date",
    "as_of_date_in_form_yyyy-mm-dd": "report_date",
    "As_of_Date_In_Form_YYYY-MM-DD": "report_date",

    # identifiers
    "market_and_exchange_names": "market_and_exchange_names",
    "Market_and_Exchange_Names": "market_and_exchange_names",
    "cftc_contract_market_code": "cftc_contract_market_code",
    "CFTC_Contract_Market_Code": "cftc_contract_market_code",
    "contract_market_code": "cftc_contract_market_code",
    "Contract_Market_Code": "cftc_contract_market_code",

    # open interest
    "open_interest_all": "open_interest_all",
    "Open_Interest_All": "open_interest_all",
    "open_interest": "open_interest_all",
    "Open_Interest": "open_interest_all",

    # legacy category variants (non-commercial / commercial)
    "noncomm_positions_long_all": "noncomm_long",
    "Noncommercial_Positions_Long_All": "noncomm_long",
    "noncomm_positions_short_all": "noncomm_short",
    "Noncommercial_Positions_Short_All": "noncomm_short",

    "comm_positions_long_all": "comm_long",
    "Commercial_Positions_Long_All": "comm_long",
    "comm_positions_short_all": "comm_short",
    "Commercial_Positions_Short_All": "comm_short",

    # sometimes labeled "spec" instead of noncomm
    "spec_positions_long_all": "noncomm_long",
    "spec_positions_short_all": "noncomm_short",

    # optional concentration fields (may not exist pre-1990)
    "traders_tot_all": "traders_total",
    "Traders_Tot_All": "traders_total",
    "concentration_gross_long_4_all": "top4_long_pct",
    "Concentration_Gross_Long_4_All": "top4_long_pct",
}

COL_ALIASES_NORM = {k.lower().strip(): v for k, v in COL_ALIASES.items()}

STD_COLS = [
    "report_date",
    "cftc_contract_market_code",
    "market_and_exchange_names",
    "open_interest_all",
    "noncomm_long",
    "noncomm_short",
    "comm_long",
    "comm_short",
    "traders_total",
    "top4_long_pct",
]


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normalize column names to match alias keys
    rename_map = {}
    for c in df.columns:
        c_stripped = c.strip()
        if c_stripped in COL_ALIASES:
            rename_map[c] = COL_ALIASES[c_stripped]
            rename_map[c] = COL_ALIASES[c_stripped]
    df = df.rename(columns=rename_map)

    # If some aliases didn't match due to casing/spacing, do a second pass
    lower_map = {c.lower().strip(): c for c in df.columns}
    for key_norm, alias_out in COL_ALIASES_NORM.items():
        if key_norm in lower_map and alias_out not in df.columns:
            df = df.rename(columns={lower_map[key_norm]: alias_out})
        return df


def _coerce_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _ensure_required(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    missing = []
    for c in ["report_date", "open_interest_all"]:
        if c not in df.columns:
            missing.append(c)

    # identifiers: at least one should exist; code preferred, name acceptable
    if "cftc_contract_market_code" not in df.columns and "market_and_exchange_names" not in df.columns:
        missing.append("cftc_contract_market_code|market_and_exchange_names")

    return df, missing


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "report_date" in df.columns:
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    return df


def _derive_positions(df: pd.DataFrame) -> pd.DataFrame:
    # Derive net fields if possible
    if "noncomm_long" in df.columns and "noncomm_short" in df.columns:
        df["noncomm_net"] = df["noncomm_long"] - df["noncomm_short"]
    else:
        df["noncomm_net"] = pd.NA

    if "comm_long" in df.columns and "comm_short" in df.columns:
        df["comm_net"] = df["comm_long"] - df["comm_short"]
    else:
        df["comm_net"] = pd.NA

    # Percent of open interest (safe, interpretable, no cross-era z-scoring)
    if "open_interest_all" in df.columns:
        oi = df["open_interest_all"].replace({0: pd.NA})
        df["noncomm_net_pct_oi"] = (pd.to_numeric(df["noncomm_net"], errors="coerce") / oi) * 100
        df["comm_net_pct_oi"] = (pd.to_numeric(df["comm_net"], errors="coerce") / oi) * 100
    else:
        df["noncomm_net_pct_oi"] = pd.NA
        df["comm_net_pct_oi"] = pd.NA

    return df


def _apply_block4_policy(df: pd.DataFrame, policy: Block4Policy) -> pd.DataFrame:
    df["block_id"] = policy.block_id
    df["era"] = policy.era
    df["support_only"] = policy.support_only
    df["eligible_for_state"] = policy.eligible_for_state
    df["structural_regime"] = policy.structural_regime
    return df


def _filter_era(df: pd.DataFrame, start: str = "1986-01-01", end: str = "1990-01-01") -> pd.DataFrame:
    d0 = pd.to_datetime(start)
    d1 = pd.to_datetime(end)
    return df[(df["report_date"] >= d0) & (df["report_date"] < d1)].copy()


def _monthly_aggregate(df_weekly: pd.DataFrame) -> pd.DataFrame:
    """
    Monthly panel:
    - month_end timestamp as the time key
    - mean aggregation for numeric features
    """
    df = df_weekly.copy()
    df["month"] = df["report_date"].dt.to_period("M").dt.to_timestamp("M")  # month-end

    id_cols = ["month", "cftc_contract_market_code", "market_and_exchange_names", "block_id", "era", "support_only",
               "eligible_for_state", "structural_regime"]

    # --- Parquet type stability (IDs must be strings) ---
    df["cftc_contract_market_code"] = (
        df["cftc_contract_market_code"]
          .astype("string")
          .str.strip()
    )
    df["market_and_exchange_names"] = (
        df["market_and_exchange_names"]
          .astype("string")
          .str.strip()
    )

    # ensure identifier columns exist
    if "cftc_contract_market_code" not in df.columns:
        df["cftc_contract_market_code"] = pd.NA
    if "market_and_exchange_names" not in df.columns:
        df["market_and_exchange_names"] = pd.NA

    num_cols = [
        "open_interest_all",
        "noncomm_long",
        "noncomm_short",
        "comm_long",
        "comm_short",
        "noncomm_net",
        "comm_net",
        "noncomm_net_pct_oi",
        "comm_net_pct_oi",
        "traders_total",
        "top4_long_pct",
    ]
    present_num_cols = [c for c in num_cols if c in df.columns]

    # group & aggregate
    out = (
        df.groupby(id_cols, dropna=False)[present_num_cols]
          .mean(numeric_only=True)
          .reset_index()
          .rename(columns={"month": "report_month"})
    )

    # Guardrails: block4 must never be eligible_for_state
    if out["eligible_for_state"].any():
        raise ValueError("Policy violation: Block 4 monthly output contains eligible_for_state=True.")

    return out


def _read_any(filepath: str) -> pd.DataFrame:
    """
    Read csv/txt directly, or read first parseable .txt/.csv inside a zip.
    """
    def _try_read_bytes(b: bytes) -> Optional[pd.DataFrame]:
        for kwargs in (
            {"sep": ","},
            {"sep": "|", "engine": "python"},
            {"sep": "\t"},
        ):
            try:
                return pd.read_csv(io.BytesIO(b), **kwargs)
            except Exception:
                pass
        try:
            return pd.read_fwf(io.BytesIO(b))
        except Exception:
            return None

    if filepath.lower().endswith(".zip"):
        with zipfile.ZipFile(filepath, "r") as z:
            members = [m for m in z.namelist() if m.lower().endswith((".txt", ".csv"))]
            if not members:
                return pd.DataFrame()

            # Prefer .txt over .csv
            members_txt = [m for m in members if m.lower().endswith(".txt")]
            member = members_txt[0] if members_txt else members[0]

            b = z.read(member)
            df = _try_read_bytes(b)
            if df is None:
                raise ValueError(f"Could not parse zip member {member} in {filepath}")
            return df

    # non-zip
    try:
        return pd.read_csv(filepath)
    except Exception:
        try:
            return pd.read_csv(filepath, sep="|", engine="python")
        except Exception:
            try:
                return pd.read_csv(filepath, sep="\t")
            except Exception:
                return pd.read_fwf(filepath)



def build_block4(raw_dir: str, out_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    policy = Block4Policy()

    files = sorted(
        glob.glob(os.path.join(raw_dir, "*.csv"))
        + glob.glob(os.path.join(raw_dir, "*.txt"))
        + glob.glob(os.path.join(raw_dir, "*.zip"))
    )

    # Skip excel zips (contain .xls) and multi-year bundles
    files = [fp for fp in files if "_xls_" not in os.path.basename(fp).lower()]
    files = [fp for fp in files if "1986_2016" not in os.path.basename(fp).lower()]

    if not files:
        raise FileNotFoundError(f"No usable files found in raw_dir: {raw_dir} (expected .csv/.txt/.zip)")

    frames: List[pd.DataFrame] = []
    skipped: List[Tuple[str, List[str]]] = []

    for fp in files:
        df = _read_any(fp)
        if "deacot1986.zip" in os.path.basename(fp).lower():
            print("\n[Block4][debug] file:", os.path.basename(fp))
            print("[Block4][debug] columns:", list(df.columns))
            try:
                print("[Block4][debug] head:\n", df.head(3).to_string(index=False))
            except Exception:
                print("[Block4][debug] head: <could not render>")
        df = _standardize_columns(df)
        df = _parse_dates(df)

        df, missing = _ensure_required(df)
        if missing:
            skipped.append((os.path.basename(fp), missing))
            continue

        # standard numeric coercion
        numeric_candidates = [
            "open_interest_all", "noncomm_long", "noncomm_short", "comm_long", "comm_short", "traders_total", "top4_long_pct"
        ]
        df = _coerce_numeric(df, numeric_candidates)

        # only keep the decade
        df = _filter_era(df, "1986-01-01", "1990-01-01")
        if df.empty:
            continue

        # derive safe fields
        df = _derive_positions(df)

        # apply policy gates
        df = _apply_block4_policy(df, policy)

        # keep a clean subset plus deriveds
        keep = list(dict.fromkeys(STD_COLS + [
            "noncomm_net", "comm_net", "noncomm_net_pct_oi", "comm_net_pct_oi",
            "block_id", "era", "support_only", "eligible_for_state", "structural_regime"
        ]))
        for c in keep:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[keep]

        frames.append(df)

    if skipped:
        print("\n[Block4] Skipped files due to missing required columns:")
        for name, miss in skipped:
            print(f"  - {name}: missing {miss}")

    if not frames:
        raise ValueError("No usable Block 4 rows produced. Check raw files & mappings.")

    weekly = pd.concat(frames, ignore_index=True)
    weekly = weekly.dropna(subset=["report_date"]).sort_values("report_date")

    # enforce decade bounds strictly
    weekly = _filter_era(weekly, "1986-01-01", "1990-01-01")

    # monthly panel
    monthly = _monthly_aggregate(weekly)

    os.makedirs(out_dir, exist_ok=True)

    weekly_out = os.path.join(out_dir, "cot_block4_1986_1989_weekly_std.parquet")
    monthly_out = os.path.join(out_dir, "cot_block4_1986_1989_monthly.parquet")

    # --- Parquet type stability (IDs must be strings) ---
    weekly["cftc_contract_market_code"] = (
    weekly["cftc_contract_market_code"]
      .astype("string")
      .str.strip()
        )
    weekly["market_and_exchange_names"] = (
    weekly["market_and_exchange_names"]
      .astype("string")
      .str.strip()
        )

    weekly.to_parquet(weekly_out, index=False)
    monthly.to_parquet(monthly_out, index=False)

    print(f"\n[Block4] Weekly standardized saved:  {weekly_out}")
    print(f"[Block4] Monthly panel saved:        {monthly_out}")
    print(f"[Block4] Weekly rows: {len(weekly):,} | Monthly rows: {len(monthly):,}")

    return weekly, monthly


def merge_into_cot_store(spine_cot_store_path: str, block4_weekly: pd.DataFrame) -> None:
    """
    Append Block 4 weekly rows into the unified store, while enforcing:
      - support_only True
      - eligible_for_state False
      - era 1980_1989
      - block_id 4
    """
    # Policy enforcement before merge
    bad = block4_weekly[
        (block4_weekly["block_id"] != 4) |
        (block4_weekly["era"] != "1986_1989") |
        (block4_weekly["support_only"] != True) |
        (block4_weekly["eligible_for_state"] != False)
    ]
    if len(bad) > 0:
        raise ValueError("Policy violation in Block 4 weekly frame; refusing to merge.")

    if os.path.exists(spine_cot_store_path):
        store = pd.read_parquet(spine_cot_store_path)

        for df_ in (store, block4_weekly):
            if "cftc_contract_market_code" in df_.columns:
                df_["cftc_contract_market_code"] = df_["cftc_contract_market_code"].astype("string").str.strip()
            if "market_and_exchange_names" in df_.columns:
                df_["market_and_exchange_names"] = df_["market_and_exchange_names"].astype("string").str.strip()

        # De-dup by report_date + contract identifiers if present
        key_cols = ["report_date", "cftc_contract_market_code", "market_and_exchange_names", "block_id"]
        for c in key_cols:
            if c not in store.columns:
                store[c] = pd.NA
        merged = pd.concat([store, block4_weekly], ignore_index=True)

        # stable de-dup
        merged = merged.drop_duplicates(subset=key_cols, keep="last")
        merged = merged.sort_values(["report_date", "market_and_exchange_names"])
    else:
        merged = block4_weekly.copy()

    # Safety: Block4 must not be eligible_for_state anywhere after merge
    if (merged.query("block_id == 4")["eligible_for_state"].astype(bool)).any():
        raise ValueError("Post-merge violation: block_id=4 contains eligible_for_state=True.")

    merged.to_parquet(spine_cot_store_path, index=False)
    print(f"\n[Block4] Merged into cot store: {spine_cot_store_path}")
    print(f"[Block4] Store rows now: {len(merged):,}")


def _clean_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    # Contract codes: preserve leading zeros, strip whitespace, force string
    if "cftc_contract_market_code" in df.columns:
        df["cftc_contract_market_code"] = (
            df["cftc_contract_market_code"]
            .astype("string")
            .str.strip()
        )
        # normalize empty -> NA
        df.loc[df["cftc_contract_market_code"].isin(["", "nan", "<NA>"]), "cftc_contract_market_code"] = pd.NA

    if "market_and_exchange_names" in df.columns:
        df["market_and_exchange_names"] = (
            df["market_and_exchange_names"]
            .astype("string")
            .str.strip()
        )

    return df

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw_dir", type=str, required=True, help="Directory with legacy COT files for 1986–1989.")
    ap.add_argument("--out_dir", type=str, default="data/cot/block4", help="Output directory for Block 4 artifacts.")
    ap.add_argument("--cot_store", type=str, default="data/cot/cot_store.parquet", help="Unified store path.")
    ap.add_argument("--merge_into_store", action="store_true", help="Append Block 4 weekly rows into cot_store.parquet.")
    args = ap.parse_args()

    weekly, _monthly = build_block4(args.raw_dir, args.out_dir)

    if args.merge_into_store:
        merge_into_cot_store(args.cot_store, weekly)


if __name__ == "__main__":
    main()
