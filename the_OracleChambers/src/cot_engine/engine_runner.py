"""
COT Engine Lite runner.

Fetches CFTC data, parses reports, builds tidy rows, and updates cot_store.parquet.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from cot_engine.data_fetcher import fetch_cftc_data
from cot_engine.data_parser import (
    extract_commodity_reports,
    parse_positions_simple,
    parse_changes_simple,
    parse_percents_simple,
    parse_traders_simple,
    parse_largest_traders_simple,
)
from cot_engine.build_cot_row import build_cot_row

COT_STORE_PATH = Path("data/cot/cot_store.parquet")


def load_cot_store(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def upsert_rows(existing: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Upsert new_rows into existing using ['report_date', 'code'] as key.
    New rows override existing on collision.
    """
    if new_rows.empty:
        return existing

    if existing.empty:
        return new_rows

    keys = ["report_date", "code"]
    existing_idx = existing.set_index(keys)
    new_idx = new_rows.set_index(keys)

    combined = existing_idx.combine_first(new_idx)
    combined.update(new_idx)

    return combined.reset_index()


def run_cot_engine_once(debug: bool = False) -> bool:
    """
    Run a single engine pass:
        - fetch latest CFTC 'Other (Combined)' report
        - parse per-commodity blocks (currently placeholder)
        - build tidy rows for each
        - upsert into cot_store.parquet
    """
    raw_text = fetch_cftc_data(debug_mode=debug)
    if not raw_text:
        if debug:
            print("[engine_runner] No raw_text from CFTC.")
        return False

    reports = extract_commodity_reports(raw_text, debug)
    if not reports:
        if debug:
            print("[engine_runner] No commodity reports parsed.")
        return False

    rows = []
    for r in reports:
        if debug:
            print(
                f"[engine_runner] Processing {r.get('commodity')} ({r.get('code')})"
            )

        positions = parse_positions_simple(r["raw"], debug)
        changes = parse_changes_simple(r["raw"], debug)
        percents = parse_percents_simple(r["raw"], debug)
        num_traders = parse_traders_simple(r["raw"], debug)
        largest = parse_largest_traders_simple(r["raw"], debug)

        try:
            s = build_cot_row(r, positions, changes, percents, num_traders, largest)
            rows.append(s)
        except Exception as e:
            if debug:
                print(
                    f"[engine_runner] Failed to build row for {r.get('code')}: {e}"
                )

    if not rows:
        if debug:
            print("[engine_runner] No rows built; aborting.")
        return False

    new_df = pd.DataFrame(rows)
    existing = load_cot_store(COT_STORE_PATH)
    combined = upsert_rows(existing, new_df)

    COT_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(COT_STORE_PATH, index=False)

    if debug:
        print(
            f"[engine_runner] Wrote {len(combined)} rows to {COT_STORE_PATH.as_posix()}"
        )

    return True


if __name__ == "__main__":
    success = run_cot_engine_once(debug=True)
    print("COT engine run:", "success" if success else "failed")

