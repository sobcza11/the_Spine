import pandas as pd


CORE_RATES = [
    "y2", "y5", "y10", "y30",
    "spread_10_2", "spread_30_5",
    "eu_y10", "uk_y10",
    "us_eu_10y_spread", "us_uk_10y_spread",
]


def main():
    df = pd.read_parquet("../../data/rates/tier1/rates_tier1_panel.parquet").copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    for col in CORE_RATES:
        mean = df[col].expanding().mean()
        std = df[col].expanding().std()
        df[f"{col}_z"] = (df[col] - mean) / std

    z_cols = [f"{col}_z" for col in CORE_RATES]
    df["rates_core_zt"] = df[z_cols].mean(axis=1)

    # --- ADD THIS BLOCK HERE ---
    mean = df["rates_core_zt"].expanding().mean()
    std  = df["rates_core_zt"].expanding().std()
    df["rates_core_zt"] = (df["rates_core_zt"] - mean) / std

    df["rates_core_zt"] = df["rates_core_zt"].fillna(0)
    # --- END BLOCK ---

    out = df[["date", "rates_core_zt"]].copy()
    out.to_parquet("../../data/rates/zt/rates_core_zt.parquet", index=False)

    print("OK | rates_core_zt built")
    print(out.tail())


if __name__ == "__main__":
    main()
    