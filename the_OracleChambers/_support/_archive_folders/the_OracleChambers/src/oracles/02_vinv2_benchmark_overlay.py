from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def detect_roots():
    """
    Infer SPINE_ROOT (…/the_Spine) and ORACLECHAMBERS_ROOT (…/the_Spine/the_OracleChambers)
    based on this file's location.
    """
    this_file = Path(__file__).resolve()
    # this_file = .../the_Spine/the_OracleChambers/src/oracles/02_vinv2_benchmark_overlay.py
    oracle_root = this_file.parents[2]
    spine_root = oracle_root.parent
    print(f"[Overlay] Detected SPINE_ROOT:          {spine_root}")
    print(f"[Overlay] Detected ORACLECHAMBERS_ROOT: {oracle_root}")
    return spine_root, oracle_root


def safe_read_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"[Overlay] {label} not found at {path}")
    print(f"[Overlay] Loading {label} from {path}")
    return pd.read_parquet(path)


def build_vinv_eq_series(df_vinv: pd.DataFrame) -> pd.DataFrame:
    """
    Build an equal-weight VinV_0_10 series across core stock tickers.
    """
    core_names = ["AAPL", "MSFT", "JPM", "XOM"]
    df_core = df_vinv[df_vinv["symbol"].isin(core_names)].copy()

    if df_core.empty:
        raise ValueError(
            "[Overlay] No rows found for core tickers "
            f"{core_names}. Check universe in VinV panel."
        )

    # Make sure date is datetime
    df_core["date"] = pd.to_datetime(df_core["date"])

    # Equal-weight across names per month
    df_eq = (
        df_core.groupby("date", as_index=False)["VinV_0_10"]
        .mean()
        .rename(columns={"VinV_0_10": "vinv_eq_0_10"})
        .sort_values("date")
    )

    print("[Overlay] Built equal-weight VinV_0_10 series:")
    print(df_eq.head())
    return df_eq


def build_ivv_cumret(df_returns: pd.DataFrame) -> pd.DataFrame:
    """
    Build IVV cumulative return index from monthly returns.
    Expects columns: ['date', 'ticker', 'ret'] or ['monthly_ret'].
    """
    df_ivv = df_returns[df_returns["ticker"] == "IVV"].copy()
    if df_ivv.empty:
        raise ValueError("[Overlay] No IVV rows found in returns panel.")

    # Normalize column names a bit
    df_ivv["date"] = pd.to_datetime(df_ivv["date"])

    if "ret" in df_ivv.columns:
        ret_col = "ret"
    elif "monthly_ret" in df_ivv.columns:
        ret_col = "monthly_ret"
    else:
        raise ValueError(
            "[Overlay] Could not find a return column in returns panel "
            "(expected 'ret' or 'monthly_ret')."
        )

    df_ivv = df_ivv.sort_values("date")
    df_ivv["ivv_cumret"] = (1.0 + df_ivv[ret_col].fillna(0.0)).cumprod()

    print("[Overlay] Built IVV cumulative return index:")
    print(df_ivv[["date", ret_col, "ivv_cumret"]].head())

    return df_ivv[["date", "ivv_cumret"]]


def merge_and_export(
    df_vinv_eq: pd.DataFrame,
    df_ivv: pd.DataFrame,
    qa_dir: Path,
) -> Path:
    qa_dir.mkdir(parents=True, exist_ok=True)

    df_merge = pd.merge(df_vinv_eq, df_ivv, on="date", how="inner").sort_values("date")

    sample_path = qa_dir / "vinv_ivv_overlay_sample.csv"
    df_merge.to_csv(sample_path, index=False)
    print(f"[Overlay] Wrote merged sample CSV → {sample_path}")

    # Quick sanity plot
    fig, ax1 = plt.subplots(figsize=(10, 5))

    ax1.plot(df_merge["date"], df_merge["vinv_eq_0_10"], label="VinV eq-weight (0–10)")
    ax1.set_ylabel("VinV eq-weight (0–10)")
    ax1.set_xlabel("Date")

    ax2 = ax1.twinx()
    ax2.plot(
        df_merge["date"],
        df_merge["ivv_cumret"],
        linestyle="--",
        label="IVV cumulative return",
    )
    ax2.set_ylabel("IVV cumulative return index")

    ax1.set_title("VinV_0_10 (eq-weight) vs IVV Cumulative Returns")

    # Handle legend nicely across both axes
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left")

    fig.tight_layout()
    plot_path = qa_dir / "vinv_vs_ivv_overlay.png"
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)

    print(f"[Overlay] Wrote overlay plot → {plot_path}")
    return plot_path


def main():
    spine_root, oracle_root = detect_roots()

    # Paths (all relative to detected roots)
    vinv_panel_path = (
        spine_root
        / "data"
        / "vinv"
        / "vinv_2_0"
        / "phase2_outputs"
        / "vinv_monthly_panel_phase2.parquet"
    )
    returns_path = spine_root / "data" / "vinv" / "vinv_returns_monthly.parquet"
    qa_dir = oracle_root / "data" / "vinv" / "vinv_2_0" / "qa"

    # Load data
    df_vinv = safe_read_parquet(vinv_panel_path, "VinV Phase2 panel")
    df_returns = safe_read_parquet(returns_path, "VinV monthly returns")

    # Build series
    df_vinv_eq = build_vinv_eq_series(df_vinv)
    df_ivv = build_ivv_cumret(df_returns)

    # Merge & plot
    merge_and_export(df_vinv_eq, df_ivv, qa_dir)

    print("[Overlay] Done.")


if __name__ == "__main__":
    main()
