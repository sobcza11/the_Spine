"""
Build a unified COT store from yearly XLS files in data/cot/xls.

Usage (from the_OracleChambers root):

    $env:PYTHONPATH="$PWD\\src"
    python -m cot_engine.build_cot_store_from_xls

What it does:
- Discovers available years from data/cot/xls/*.xls
- Reads each year into a DataFrame
- Concatenates into one store
- (Optionally) computes derived metrics + z-scores if your utils module exposes helpers
- Writes parquet to data/cot/cot_store.parquet

Key compatibility fix:
- Forces CFTC_Contract_Market_Code to string to preserve leading zeros and avoid
  mixed-type pyarrow failures when legacy years introduce string codes like "001601".
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd


def _discover_years(xls_dir: Path) -> List[int]:
    years: List[int] = []
    for p in sorted(xls_dir.glob("*.xls")):
        m = re.match(r"^(\d{4})\.xls$", p.name)
        if m:
            years.append(int(m.group(1)))
    return years


def _pick_date_col(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "Report_Date_as_YYYY-MM-DD",
        "Report_Date_as_YYYY_MM_DD",
        "Report_Date_as_MM_DD_YYYY",
        "As_of_Date_In_Form_YYMMDD",
        "As_of_Date_In_Form_YYYYMMDD",
        "Report_Date",
        "As_of_Date",
        "Date",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _coerce_report_date(df: pd.DataFrame) -> pd.DataFrame:
    date_col = _pick_date_col(df)
    if not date_col:
        return df

    # Standardize to a single column name used downstream
    if "report_date" not in df.columns:
        df = df.rename(columns={date_col: "report_date"})
    else:
        # If report_date already exists, still attempt to parse it
        pass

    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    return df


def _year_from_path(p: Path) -> int:
    return int(p.stem)


def _read_xls(path: Path) -> pd.DataFrame:
    # Let pandas pick an engine; in your runs this is working for .xls.
    # (OLE2 warnings are normal for some legacy files.)
    df = pd.read_excel(path)
    df["year"] = _year_from_path(path)
    df = _coerce_report_date(df)
    return df


def _maybe_apply_utils(combined: pd.DataFrame) -> pd.DataFrame:
    """
    If your cot_engine.utils has helper functions for derived metrics/zscores,
    call them (best-effort) without hard-coding one exact name.
    """
    try:
        from cot_engine import utils as u  # type: ignore
    except Exception:
        return combined

    # Try common function names in order (no-op if not present)
    fn_names = [
        # derived metrics
        "add_position_metrics",
        "add_derived_metrics",
        "build_position_metrics",
        "compute_position_metrics",
        "add_cot_features",
        "build_cot_features",
        # z-scores
        "add_zscores",
        "compute_zscores",
        "zscore_features",
    ]

    for name in fn_names:
        fn = getattr(u, name, None)
        if callable(fn):
            try:
                out = fn(combined)
                if isinstance(out, pd.DataFrame):
                    combined = out
            except Exception:
                # Keep going; utils can evolve without breaking the build step.
                continue

    return combined


def _normalize_parquet_breakers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix known pyarrow/parquet failures from mixed dtypes across legacy years.
    """
    # Critical: preserve leading zeros (e.g., "001601") and avoid mixed int/str
    if "CFTC_Contract_Market_Code" in df.columns:
        df["CFTC_Contract_Market_Code"] = (
            df["CFTC_Contract_Market_Code"].astype("string").str.strip()
        )

    # Optional: these sometimes drift to object with mixed types across years
    for col in ["CFTC_Market_Code", "Market_and_Exchange_Names"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a unified COT parquet store from yearly XLS files."
    )
    parser.add_argument(
        "--xls-dir",
        default="data/cot/xls",
        help="Directory containing yearly .xls files (default: data/cot/xls)",
    )
    parser.add_argument(
        "--out",
        default="data/cot/cot_store.parquet",
        help="Output parquet path (default: data/cot/cot_store.parquet)",
    )
    parser.add_argument(
        "--years",
        default=None,
        help="Optional years to build, e.g. '1990-2025' or '2004,2005,2006'",
    )
    return parser.parse_args()


def _parse_years_arg(arg: Optional[str], discovered: List[int]) -> List[int]:
    if not arg:
        return discovered

    s = arg.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        start = int(a.strip())
        end = int(b.strip())
        return [y for y in range(start, end + 1)]
    if "," in s:
        vals = [int(x.strip()) for x in s.split(",") if x.strip()]
        return vals
    return [int(s)]


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [build_cot_store_from_xls] %(message)s",
    )

    xls_dir = Path(args.xls_dir)
    out_path = Path(args.out)

    if not xls_dir.exists():
        raise FileNotFoundError(f"XLS directory not found: {xls_dir}")

    discovered = _discover_years(xls_dir)
    if not discovered:
        raise FileNotFoundError(f"No yearly .xls files found in: {xls_dir}")

    years = _parse_years_arg(args.years, discovered)

    # Only keep years that actually exist on disk
    available = set(discovered)
    years = [y for y in years if y in available]

    logging.info("Discovered years: %s", years)

    frames: List[pd.DataFrame] = []
    for y in years:
        f = xls_dir / f"{y}.xls"
        if not f.exists():
            logging.warning("Missing %s — skipping year %s.", f.as_posix(), y)
            continue

        logging.info("Reading %s", f.as_posix())
        df = _read_xls(f)

        # Log shape + date range if we have report_date
        if "report_date" in df.columns:
            dmin = df["report_date"].min()
            dmax = df["report_date"].max()
            logging.info(
                "Year %s: %s rows, %s cols, from %s to %s",
                y,
                len(df),
                df.shape[1],
                dmin,
                dmax,
            )
        else:
            logging.info("Year %s: %s rows, %s cols", y, len(df), df.shape[1])

        frames.append(df)

    if not frames:
        raise RuntimeError("No data loaded — nothing to write.")

    combined = pd.concat(frames, ignore_index=True, sort=False)

    # Optional: derived metrics + zscores if your utils exposes them
    combined = _maybe_apply_utils(combined)

    combined = _normalize_parquet_breakers(combined)

    # Final logging
    if "report_date" in combined.columns:
        logging.info(
            "Combined: %s rows, %s cols, from %s to %s",
            len(combined),
            combined.shape[1],
            combined["report_date"].min(),
            combined["report_date"].max(),
        )
    else:
        logging.info("Combined: %s rows, %s cols", len(combined), combined.shape[1])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out_path, index=False)

    logging.info("Wrote unified COT store to %s", out_path.as_posix())


if __name__ == "__main__":
    main()

