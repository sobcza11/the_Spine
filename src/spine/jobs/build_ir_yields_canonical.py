import sys

import pandas as pd

from common.r2_client import read_parquet_from_r2, write_parquet_to_r2

# Downstream consumer reads this key (IR diff bridge)
R2_IR_YIELDS_KEY = "spine_us/us_ir_yields_canonical.parquet"

# Current yields implementation writes this (per CI logs)
R2_FX_10Y_YIELDS_KEY = "spine_us/us_fx_10y_yields.parquet"


def _import_impl():
    candidates = [
        # Preferred: leaf-style CLIs (if present)
        ("US_TeaPlant.leaves.build_ir_yields_canonical_cli", "main"),
        ("US_TeaPlant.leaves.ir_yields_canonical_leaf_cli", "main"),
        ("US_TeaPlant.leaves.ir_rates_canonical_leaf_cli", "main"),
        # Bridge-style fallbacks (if those are what exist)
        ("US_TeaPlant.bridges.ir_rates_bridge", "main"),
        ("US_TeaPlant.bridges.fx_ir_yield_bridge", "main"),
    ]
    last_err = None
    for mod_name, fn_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            fn = getattr(mod, fn_name)
            return fn, mod_name, fn_name
        except Exception as e:
            last_err = e
    raise ImportError(f"No valid implementation found. Last error: {last_err}")


def _coerce_fx10y_to_long_ir_yields(df_fx10y: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the FX 10Y yields leaf into the long-form IR yields schema expected by IR diff:
      as_of_date, ccy, tenor, rate_value

    Tolerant to column naming differences.
    """
    cols = set(df_fx10y.columns)

    # date column
    if "as_of_date" in cols:
        date_col = "as_of_date"
    elif "date" in cols:
        date_col = "date"
    else:
        raise KeyError(
            f"[IR-YIELDS] FX 10Y leaf missing date column. Found={sorted(cols)}"
        )

    # ccy column
    if "ccy" in cols:
        ccy_col = "ccy"
    elif "currency" in cols:
        ccy_col = "currency"
    else:
        raise KeyError(
            f"[IR-YIELDS] FX 10Y leaf missing ccy column. Found={sorted(cols)}"
        )

    # value column (tolerant) — UPDATED: include yld_10y
    value_candidates = ["rate_value", "rate", "yield", "yld10", "yld_10y", "value"]
    val_col = next((c for c in value_candidates if c in cols), None)
    if val_col is None:
        raise KeyError(
            "[IR-YIELDS] FX 10Y leaf missing value column; expected one of "
            f"{value_candidates}. Found={sorted(cols)}"
        )

    out = df_fx10y[[date_col, ccy_col, val_col]].copy()
    out = out.rename(
        columns={date_col: "as_of_date", ccy_col: "ccy", val_col: "rate_value"}
    )
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce")
    out["tenor"] = "10Y"

    # Keep only parseable dates
    out = out[out["as_of_date"].notna()].copy()

    # Ensure numeric (allow nulls)
    out["rate_value"] = pd.to_numeric(out["rate_value"], errors="coerce")

    # Canonical ordering
    out = out.sort_values(["as_of_date", "ccy", "tenor"]).reset_index(drop=True)
    return out


def _refresh_ir_yields_canonical_from_fx10y() -> None:
    """
    Priority 1 contract enforcer:
    If the upstream implementation only writes the FX 10Y leaf, we still ensure the
    canonical IR yields key exists & advances by merging new 10Y rows into it.

    This preserves existing POLICY rows (if present) & only extends 10Y coverage.
    """
    df_fx10y = read_parquet_from_r2(R2_FX_10Y_YIELDS_KEY)
    if df_fx10y is None or len(df_fx10y) == 0:
        print(
            f"[IR-YIELDS][WARN] FX 10Y yields leaf empty/missing: {R2_FX_10Y_YIELDS_KEY}"
        )
        return

    df_new_10y = _coerce_fx10y_to_long_ir_yields(df_fx10y)
    if df_new_10y.empty:
        print("[IR-YIELDS][WARN] FX 10Y yields normalized to empty set; nothing to merge.")
        return

    try:
        df_old = read_parquet_from_r2(R2_IR_YIELDS_KEY)
    except Exception:
        df_old = None

    if df_old is None or len(df_old) == 0:
        write_parquet_to_r2(df_new_10y, R2_IR_YIELDS_KEY, index=False)
        mx = pd.to_datetime(df_new_10y["as_of_date"]).max()
        print(
            f"[IR-YIELDS] Wrote NEW canonical IR yields leaf (10Y-only) to R2 at {R2_IR_YIELDS_KEY} "
            f"(rows={len(df_new_10y):,}, max_date={mx.date() if pd.notna(mx) else mx})"
        )
        return

    if "as_of_date" not in df_old.columns and "date" in df_old.columns:
        df_old = df_old.rename(columns={"date": "as_of_date"})

    for c in ("as_of_date", "ccy", "tenor"):
        if c not in df_old.columns:
            raise RuntimeError(
                f"[IR-YIELDS] Existing canonical leaf missing '{c}'. "
                f"Cannot safely merge. Found={sorted(df_old.columns.tolist())}"
            )

    df_old["as_of_date"] = pd.to_datetime(df_old["as_of_date"], errors="coerce")
    df_old = df_old[df_old["as_of_date"].notna()].copy()

    df_combined = pd.concat([df_old, df_new_10y], ignore_index=True)

    df_combined = df_combined.drop_duplicates(
        subset=["as_of_date", "ccy", "tenor"],
        keep="last",
    )

    df_combined = (
        df_combined.sort_values(["as_of_date", "ccy", "tenor"]).reset_index(drop=True)
    )

    write_parquet_to_r2(df_combined, R2_IR_YIELDS_KEY, index=False)

    old_max = pd.to_datetime(df_old["as_of_date"]).max()
    new_max = pd.to_datetime(df_combined["as_of_date"]).max()

    print(
        f"[IR-YIELDS] Refreshed canonical IR yields leaf at {R2_IR_YIELDS_KEY} "
        f"(rows={len(df_combined):,}, old_max={old_max.date() if pd.notna(old_max) else old_max}, "
        f"new_max={new_max.date() if pd.notna(new_max) else new_max})"
    )


def main() -> int:
    try:
        impl_main, mod_name, fn_name = _import_impl()
    except Exception as e:
        print(
            "[spine.jobs.build_ir_yields_canonical] Implementation import failed.\n"
            f"Error: {e}",
            file=sys.stderr,
        )
        return 2

    try:
        rc = int(impl_main() or 0)
    except Exception as e:
        print(
            "[spine.jobs.build_ir_yields_canonical] Implementation raised an exception.\n"
            f"Entrypoint: {mod_name}:{fn_name}\n"
            f"Error: {e}",
            file=sys.stderr,
        )
        return 1

    # Priority 1: enforce that the canonical key consumed by IR diff is actually written/advanced
    try:
        _refresh_ir_yields_canonical_from_fx10y()
    except Exception as e:
        # Do not fail the build job for the refresher step yet; we only need contract repair.
        print(f"[IR-YIELDS][WARN] Canonical refresh step failed: {e}", file=sys.stderr)

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
