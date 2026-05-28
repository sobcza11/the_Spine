from pathlib import Path
import pandas as pd

REPO_ROOT = Path.cwd()

EQUITY_FILE = REPO_ROOT / "data/spine_us/derived/etf_pmi_composite_v2.parquet"

OUTFILE = REPO_ROOT / "data/serving/equities/equities_zt.parquet"
FACTOR_OUT = REPO_ROOT / "data/serving/equities/equities_factor_panel.parquet"
DETAIL_OUT = REPO_ROOT / "data/serving/equities/equities_risk_expression_panel.parquet"
RULE_OUT = REPO_ROOT / "data/serving/equities/equities_sector_group_rules.parquet"

ETF_GROUPS = {
    "CYCLICAL": ["XLB", "XLE", "XLF", "XLI", "XLK", "XLY"],
    "DEFENSIVE": ["XLC", "XLP", "XLRE", "XLU", "XLV"],
}


def main():
    df = pd.read_parquet(EQUITY_FILE).copy()

    df = df.rename(columns={
        "as_of_date": "date",
        "etf_symbol": "etf",
    })

    df["date"] = pd.to_datetime(df["date"])

    required = {"date", "etf", "contribution"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing EQUITIES columns: {missing}. Found: {list(df.columns)}")

    all_etfs = sorted(set(sum(ETF_GROUPS.values(), [])))

    df = df[df["etf"].isin(all_etfs)].copy()
    df["equity_signal"] = pd.to_numeric(df["contribution"], errors="coerce")

    etf_panel = (
        df.groupby(["date", "etf"], as_index=False)
        .agg(
            etf_signal=("equity_signal", "sum"),
            industry_count=("industry", "nunique"),
        )
    )

    rules = []
    factor_frames = []

    for group, etfs in ETF_GROUPS.items():
        sub = etf_panel[etf_panel["etf"].isin(etfs)].copy()

        for etf in etfs:
            rules.append({"etf": etf, "factor_group": group})

        pivot = sub.pivot_table(
            index="date",
            columns="etf",
            values="etf_signal",
            aggfunc="mean",
        )

        tmp = pd.DataFrame({
            "date": pivot.index,
            "factor_group": group,
            "factor_breadth": (pivot > 0).mean(axis=1),
            "factor_dispersion": pivot.std(axis=1),
            "factor_risk_mean": pivot.mean(axis=1),
            "signal_count": pivot.notna().sum(axis=1),
        }).reset_index(drop=True)

        tmp["factor_zt"] = tmp["factor_risk_mean"].fillna(0)
        factor_frames.append(tmp)

    factor_panel = pd.concat(factor_frames, ignore_index=True)

    wide = (
        factor_panel
        .pivot(index="date", columns="factor_group", values="factor_zt")
        .reset_index()
    )

    factor_cols = [c for c in wide.columns if c != "date"]
    wide["equities_risk_expression_zt"] = wide[factor_cols].mean(axis=1).fillna(0)

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)

    wide[["date", "equities_risk_expression_zt"]].to_parquet(OUTFILE, index=False)
    factor_panel.to_parquet(FACTOR_OUT, index=False)
    wide.to_parquet(DETAIL_OUT, index=False)
    pd.DataFrame(rules).to_parquet(RULE_OUT, index=False)

    print("OK | EQUITIES rebuilt from ETF PMI composite v2")
    print(wide.tail())
    print("\nFactor coverage:")
    print(factor_panel.groupby("factor_group")["signal_count"].max())


if __name__ == "__main__":
    main()

