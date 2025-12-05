# 02_vinv2_benchmark_overlay.py
#
# Purpose:
#   Build a simple overlay table aligning VinV tranche-level scores with
#   benchmark returns (IVV / IWD / DVY) on a monthly basis and write it
#   into the OracleChambers QA area.
#
#   This is intentionally defensive & path-agnostic:
#   - Detects SPINE_ROOT and ORACLECHAMBERS_ROOT from __file__
#   - Does not hard-code any absolute paths to your machine
#   - Tries to infer the return column name in the returns parquet

from pathlib import Path
import sys
import pandas as pd


def detect_roots() -> tuple[Path, Path]:
    """
    Detect the_Spine root and the_OracleChambers root based on this file's location.

    Layout assumed:
        .../the_Spine/
            data/...
            the_OracleChambers/
                src/oracles/02_vinv2_benchmark_overlay.py
    """
    here = Path(__file__).resolve()
    # .../the_Spine/the_OracleChambers/src/oracles/02_vinv2_benchmark_overlay.py
    oracle_root = here.parents[2]      # .../the_Spine/the_OracleChambers
    spine_root = here.parents[3]       # .../the_Spine
    print(f"[Overlay] Detected SPINE_ROOT:          {spine_root}")
    print(f"[Overlay] Detected ORACLECHAMBERS_ROOT: {oracle_root}")
    return spine_root, oracle_root


def infer_return_column(df: pd.DataFrame) -> str:
    """
    Try to find a reasonable return column in the returns DataFrame.

    Common possibilities:
      - 'ret'
      - 'return'
      - 'monthly_ret'
      - 'excess_ret'
    """
    candidates = ["ret", "return", "monthly_ret", "excess_ret"]
    lower_map = {c.lower(): c for c in df.columns}

    for cand in candidates:
        if cand in lower_map:
            col = lower_map[cand]
            print(f"[Overlay] Using return column: '{col}'")
            return col

    # If we get here, we don't know which column to use
    raise ValueError(
        "[Overlay] Could not infer return column name. "
        f"Columns present: {list(df.columns)}. "
        "Expected something like 'ret', 'return', 'monthly_ret', or 'excess_ret'."
    )


def build_overlay() -> None:
    spine_root, oracle_root = detect_roots()

    # ---------------------------------------------------------------------
    # 1. Locate core inputs
    # ---------------------------------------------------------------------
    vinv_panel_path = spine_root / "data" / "vinv" / "vinv_2_0" / "phase2_outputs" / "vinv_monthly_panel_phase2.parquet"
    returns_path = spine_root / "data" / "vinv" / "vinv_returns_monthly.parquet"
    qa_dir = oracle_root / "data" / "vinv" / "vinv_2_0" / "qa"

    print(f"[Overlay] VinV Phase2 panel path: {vinv_panel_path}")
    print(f"[Overlay] Returns panel path:     {returns_path}")
    print(f"[Overlay] QA output directory:    {qa_dir}")

    if not vinv_panel_path.exists():
        print(f"[Overlay][ERROR] VinV Phase2 panel missing at {vinv_panel_path}")
        sys.exit(1)

    if not returns_path.exists():
        print(f"[Overlay][ERROR] Returns parquet missing at {returns_path}")
        sys.exit(1)

    qa_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------------
    # 2. Load VinV Phase 2 panel
    # ---------------------------------------------------------------------
    df_vinv = pd.read_parquet(vinv_panel_path)
    print(f"[Overlay] Loaded VinV Phase2 panel: {df_vinv.shape}")
    # Expect at least: symbol, date, tranche, VinV_0_10
    needed_cols = {"symbol", "date", "tranche", "VinV_0_10"}
    missing = needed_cols - set(df_vinv.columns)
    if missing:
        print(f"[Overlay][ERROR] VinV panel missing expected columns: {missing}")
        sys.exit(1)

    # Ensure date is datetime
    df_vinv["date"] = pd.to_datetime(df_vinv["date"])

    # ---------------------------------------------------------------------
    # 3. Load returns panel & infer return column
    # ---------------------------------------------------------------------
    df_rets = pd.read_parquet(returns_path)
    print(f"[Overlay] Loaded returns panel: {df_rets.shape}")

    # Expect at least date + symbol + some return column
    base_needed = {"symbol", "date"}
    missing_rets = base_needed - set(df_rets.columns)
    if missing_rets:
        print(f"[Overlay][ERROR] Returns panel missing expected columns: {missing_rets}")
        sys.exit(1)

    df_rets["date"] = pd.to_datetime(df_rets["date"])

    ret_col = infer_return_column(df_rets)

    # ---------------------------------------------------------------------
    # 4. Identify benchmark tickers & pivot them
    # ---------------------------------------------------------------------
    benchmark_universe = ["IVV", "IWD", "DVY"]
    present_benchmarks = [b for b in benchmark_universe if b in df_rets["symbol"].unique().tolist()]

    if not present_benchmarks:
        print("[Overlay][WARN] No benchmark tickers (IVV/IWD/DVY) found in returns panel.")
        print("                Proceeding without benchmark columns.")
        df_bench = None
    else:
        print(f"[Overlay] Found benchmark tickers in returns: {present_benchmarks}")
        df_bench = (
            df_rets[df_rets["symbol"].isin(present_benchmarks)]
            .pivot(index="date", columns="symbol", values=ret_col)
            .sort_index()
        )
        # Rename columns to e.g. IVV_ret, IWD_ret, DVY_ret
        df_bench = df_bench.rename(columns={sym: f"{sym}_ret" for sym in df_bench.columns})
        df_bench.reset_index(inplace=True)
        print(f"[Overlay] Benchmark frame shape: {df_bench.shape}")

    # ---------------------------------------------------------------------
    # 5. Tranche-level VinV score summary by date
    # ---------------------------------------------------------------------
    df_tranche = (
        df_vinv
        .groupby(["date", "tranche"], as_index=False)["VinV_0_10"]
        .agg(vinv_score_mean="mean", vinv_score_median="median")
    )
    print(f"[Overlay] Tranche-level VinV summary shape: {df_tranche.shape}")
    print(df_tranche.head())

    # ---------------------------------------------------------------------
    # 6. Join with benchmark returns (if available)
    # ---------------------------------------------------------------------
    if df_bench is not None:
        df_overlay = df_tranche.merge(df_bench, on="date", how="left")
    else:
        df_overlay = df_tranche.copy()

    print(f"[Overlay] Final overlay shape: {df_overlay.shape}")
    print(df_overlay.head())

    # ---------------------------------------------------------------------
    # 7. Persist outputs in QA area
    # ---------------------------------------------------------------------
    out_parquet = qa_dir / "vinv_benchmark_overlay.parquet"
    out_csv = qa_dir / "vinv_benchmark_overlay.csv"

    df_overlay.to_parquet(out_parquet, index=False)
    df_overlay.to_csv(out_csv, index=False)

    print(f"[Overlay] Wrote overlay parquet → {out_parquet}")
    print(f"[Overlay] Wrote overlay csv     → {out_csv}")
    print("[Overlay] Done.")


if __name__ == "__main__":
    build_overlay()

