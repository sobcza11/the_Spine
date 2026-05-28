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


TAB_MAP = {
    "_no": "m_no",
    "_pro": "m_pro",
    "_emp": "m_emp",
    "_sd": "m_sd",
    "_inv": "m_inv",
    "_cin": "m_cin",
    "_prc": "m_prc",
    "_bkl": "m_bkl",
    "_exp": "m_exp",
    "_imp": "m_imp",
}


def zscore(s):
    s = pd.to_numeric(s, errors="coerce")
    std = s.std()
    if std == 0 or np.isnan(std):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - s.mean()) / std


def clean_sheet(df):
    if "date" not in df.columns:
        df = df.rename(columns={df.columns[0]: "date"})

    df["date"] = df["date"].astype(str).str.strip()
    df = df[df["date"].notna()]
    df = df[df["date"] != ""]
    df = df.dropna(how="all")

    return df.reset_index(drop=True)


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "ism" / "ism_pmi_transp.xlsx"
    out_dir = repo_root / "data" / "processed" / "ism"
    out_dir.mkdir(parents=True, exist_ok=True)

    dfs = {}

    for suffix, tab in TAB_MAP.items():
        df = pd.read_excel(in_path, sheet_name=tab)
        df = clean_sheet(df)
        dfs[suffix] = df

        print(f"{tab}: rows={len(df)}, cols={len(df.columns)}")

    base = dfs["_no"].copy()
    industries = [c.replace("_no", "") for c in base.columns if c != "date"]

    all_dates = sorted(set().union(*[set(df["date"]) for df in dfs.values()]))

    final_rows = []

    for date in all_dates:
        out_row = {"date": date}

        for industry in industries:
            weighted_sum = 0.0
            total_weight = 0.0

            for suffix, weight in COMPONENT_WEIGHTS.items():
                df = dfs[suffix]
                col = f"{industry}{suffix}"

                if col not in df.columns:
                    continue

                match = df[df["date"] == date]

                if match.empty:
                    continue

                val = match.iloc[0][col]

                if pd.isna(val):
                    continue

                val = float(val)
                weighted_sum += val * weight
                total_weight += weight

            composite = weighted_sum / total_weight if total_weight else 0.0
            out_row[f"{industry}_ism_raw"] = round(composite, 4)

        final_rows.append(out_row)

    out_df = pd.DataFrame(final_rows)

    score_cols = [c for c in out_df.columns if c.endswith("_ism_raw")]

    for col in score_cols:
        out_df[col.replace("_raw", "_zt")] = zscore(out_df[col])

    regime_rows = []

    zt_cols = [c for c in out_df.columns if c.endswith("_ism_zt")]

    for _, row in out_df.iterrows():
        vals = row[zt_cols].astype(float)

        strongest = vals.sort_values(ascending=False).head(5)
        weakest = vals.sort_values().head(5)

        regime_rows.append({
            "date": row["date"],
            "strongest_industries": ", ".join([x.replace("_ism_zt", "") for x in strongest.index]),
            "weakest_industries": ", ".join([x.replace("_ism_zt", "") for x in weakest.index]),
            "ism_regime_score": round(vals.mean(), 4),
        })

    regime_df = pd.DataFrame(regime_rows)

    composite_out = out_dir / "ism_industry_composite_scores_v1.parquet"
    regime_out = out_dir / "ism_industry_regime_summary_v1.parquet"

    out_df.to_parquet(composite_out, index=False)
    regime_df.to_parquet(regime_out, index=False)

    print("OK | ISM industry composite score v2")
    print(f"input: {in_path}")
    print(f"composite output: {composite_out}")
    print(f"regime output: {regime_out}")
    print("")
    print(regime_df.to_string(index=False))


if __name__ == "__main__":
    main()

