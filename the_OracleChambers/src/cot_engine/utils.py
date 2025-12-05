from pathlib import Path
import numpy as np
import pandas as pd


def load_cot_features_for_overlay() -> pd.DataFrame:
    """
    Load COT history from data/cot/cot_store.parquet and
    build a compact monthly feature table for overlay into VinV.

    Outputs columns:
        year_month              (month-start timestamp)
        mm_net_z_xsec_mean      (avg cross-sec z of commercials' net % OI)
        spec_net_z_xsec_mean    (avg cross-sec z of non-commercials' net % OI)
        top4_long_pct_mean      (avg % of OI held by top-4 long traders)
    """
    path = Path("data") / "cot" / "cot_store.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"COT store not found at {path}. Run build_cot_store_from_xls first."
        )

    df = pd.read_parquet(path)

    # Explicit date handling
    df = df.copy()
    df["report_date"] = pd.to_datetime(df["report_date"])

    # Guard against zero OI
    df = df[df["Open_Interest_All"] != 0].copy()

    # === Net positions (legacy compatible) ===
    df["spec_net_all"] = (
        df["NonComm_Positions_Long_All"]
        - df["NonComm_Positions_Short_All"]
    )
    df["comm_net_all"] = (
        df["Comm_Positions_Long_All"]
        - df["Comm_Positions_Short_All"]
    )

    # Net as % of open interest
    oi = df["Open_Interest_All"].astype(float).replace(0, np.nan)
    df["spec_net_pct_oi"] = df["spec_net_all"] / oi
    df["mm_net_pct_oi"] = df["comm_net_all"] / oi

    # === Top-4 concentration (robust to schema changes) ===
    top4_col_candidates = [
        "Conc_Gross_LE_4_TDR_Long_All",
        "Conc_Net_LE_4_TDR_Long_All",
        "Conc_Gross_LE_4_TDR_Long_Other",
        "Conc_Net_LE_4_TDR_Long_Other",
    ]

    top4_col = next((c for c in top4_col_candidates if c in df.columns), None)
    if top4_col is None:
        raise KeyError(
            "No top-4 concentration column found. Tried: "
            f"{top4_col_candidates}"
        )

    df["top4_long_pct"] = df[top4_col].astype(float)

    # === Cross-sectional z-score helper (SAFE & future-proof) ===
    def zscore_series(x: pd.Series) -> pd.Series:
        mu = x.mean()
        sigma = x.std(ddof=0)
        if sigma == 0 or np.isnan(sigma):
            return pd.Series(0.0, index=x.index)
        return (x - mu) / sigma

    # Compute z-scores WITHOUT deprecated groupby.apply behavior
    df["mm_net_z_xsec"] = (
        df.groupby("report_date", group_keys=False)["mm_net_pct_oi"]
          .transform(zscore_series)
    )

    df["spec_net_z_xsec"] = (
        df.groupby("report_date", group_keys=False)["spec_net_pct_oi"]
          .transform(zscore_series)
    )

    # === Collapse to monthly features ===
    df["year_month"] = (
        df["report_date"]
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    cot_monthly = (
        df.groupby("year_month", as_index=False)
          .agg(
              mm_net_z_xsec_mean=("mm_net_z_xsec", "mean"),
              spec_net_z_xsec_mean=("spec_net_z_xsec", "mean"),
              top4_long_pct_mean=("top4_long_pct", "mean"),
          )
          .sort_values("year_month")
          .reset_index(drop=True)
    )

    return cot_monthly


def build_cot_monthly_features(store_path: str | Path | None = None) -> pd.DataFrame:
    """
    Backwards-compatible wrapper.

    join_vinv_with_cot.py still calls:
        build_cot_monthly_features("data/cot/cot_store.parquet")

    The argument is ignored — the canonical path is handled internally.
    """
    return load_cot_features_for_overlay()


def attach_cot_to_vinv(
    vinv: pd.DataFrame,
    cot_store_path: str = "data/cot/cot_store.parquet",
) -> pd.DataFrame:
    """
    Attach monthly COT features to a VinV monthly panel.

    Expects:
        - vinv has a 'date' or 'year_month' column
    """
    vinv = vinv.copy()

    if "date" in vinv.columns:
        vinv["year_month"] = (
            pd.to_datetime(vinv["date"])
              .dt.to_period("M")
              .dt.to_timestamp()
        )
    elif "year_month" in vinv.columns:
        vinv["year_month"] = pd.to_datetime(vinv["year_month"])
    else:
        raise ValueError("VinV must contain 'date' or 'year_month' column.")

    cot_monthly = build_cot_monthly_features(cot_store_path)

    return vinv.merge(
        cot_monthly,
        on="year_month",
        how="left",
        validate="m:1",
    )


