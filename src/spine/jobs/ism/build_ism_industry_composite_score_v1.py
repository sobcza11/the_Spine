from pathlib import Path

import numpy as np
import pandas as pd


COMPONENT_WEIGHTS = {
    "_no": 1.25,
    "_pro": 1.10,
    "_emp": 0.90,
    "_sd": 0.75,
    "_inv": 0.60,
    "_cin": 1.15,
    "_prc": 0.80,
    "_bkl": 1.00,
    "_exp": 0.85,
    "_imp": 0.50,
}


def zscore(x):
    std = x.std()

    if std == 0 or np.isnan(std):
        return pd.Series([0.0] * len(x), index=x.index)

    return (x - x.mean()) / std


def main():
    repo_root = Path.cwd()

    in_path = (
        repo_root
        / "data"
        / "ism"
        / "ism_pmi_transp.xlsx"
    )

    out_dir = (
        repo_root
        / "data"
        / "processed"
        / "ism"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    xls = pd.ExcelFile(in_path)

    manufacturing_tabs = [
        "m_no",
        "m_pro",
        "m_emp",
        "m_sd",
        "m_inv",
        "m_cin",
        "m_prc",
        "m_bkl",
        "m_exp",
        "m_imp",
    ]

    dfs = {}

    for tab in manufacturing_tabs:
        df = pd.read_excel(in_path, sheet_name=tab)

        if "date" not in df.columns:
            df = df.rename(columns={df.columns[0]: "date"})

        dfs[tab] = df.copy()

    industries = []

    for c in dfs["m_no"].columns:
        if c != "date":
            industry = c.replace("_no", "")
            industries.append(industry)

    final_rows = []

    for idx in range(len(dfs["m_no"])):
        row_date = dfs["m_no"].iloc[idx]["date"]

        out_row = {
            "date": row_date
        }

        for industry in industries:
            weighted_sum = 0.0
            total_weight = 0.0

            component_values = {}

            for suffix, weight in COMPONENT_WEIGHTS.items():
                tab = "m" + suffix

                col = f"{industry}{suffix}"

                if (
                    tab not in dfs
                    or col not in dfs[tab].columns
                ):
                    continue

                val = dfs[tab].iloc[idx][col]

                if pd.isna(val):
                    continue

                component_values[suffix] = val

                weighted_sum += val * weight
                total_weight += weight

            if total_weight == 0:
                composite = 0.0
            else:
                composite = weighted_sum / total_weight

            out_row[f"{industry}_ism_raw"] = round(composite, 4)

        final_rows.append(out_row)

    out_df = pd.DataFrame(final_rows)

    score_cols = [
        c for c in out_df.columns
        if c.endswith("_ism_raw")
    ]

    for col in score_cols:
        out_df[col.replace("_raw", "_zt")] = zscore(out_df[col])

    regime_rows = []

    for idx in range(len(out_df)):
        row = out_df.iloc[idx]

        regime_row = {
            "date": row["date"]
        }

        zt_cols = [
            c for c in out_df.columns
            if c.endswith("_ism_zt")
        ]

        vals = row[zt_cols]

        strongest = vals.sort_values(ascending=False).head(5)
        weakest = vals.sort_values().head(5)

        regime_row["strongest_industries"] = (
            ", ".join([
                x.replace("_ism_zt", "")
                for x in strongest.index
            ])
        )

        regime_row["weakest_industries"] = (
            ", ".join([
                x.replace("_ism_zt", "")
                for x in weakest.index
            ])
        )

        regime_row["ism_regime_score"] = round(vals.mean(), 4)

        regime_rows.append(regime_row)

    regime_df = pd.DataFrame(regime_rows)

    composite_out = (
        out_dir
        / "ism_industry_composite_scores_v1.parquet"
    )

    regime_out = (
        out_dir
        / "ism_industry_regime_summary_v1.parquet"
    )

    out_df.to_parquet(composite_out, index=False)
    regime_df.to_parquet(regime_out, index=False)

    print("OK | ISM industry composite score v1")
    print(f"input: {in_path}")
    print(f"composite output: {composite_out}")
    print(f"regime output: {regime_out}")

    print("")
    print(regime_df.tail().to_string(index=False))


if __name__ == "__main__":
    main()

    