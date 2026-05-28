from pathlib import Path

import pandas as pd


def zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    std = s.std()
    if std == 0 or pd.isna(std):
        return s * 0
    return (s - s.mean()) / std


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "rates" / "tier1" / "rates_tier1_panel.parquet"
    out_path = repo_root / "data" / "processed" / "rates" / "credit_proxy_inputs.parquet"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Missing source file: {in_path}")

    df = pd.read_parquet(in_path).copy()
    df["date"] = pd.to_datetime(df["date"])

    print("available columns:")
    print(df.columns.tolist())

    # Temporary deterministic proxy until direct credit data is added.
    # Uses rate stress as proxy input if true credit spread columns do not exist.
    if {"baa_10y_spread", "hy_oas"}.issubset(df.columns):
        out = df[["date", "baa_10y_spread", "hy_oas"]].copy()

    else:
        candidates = [
            c for c in df.columns
            if any(x in c.lower() for x in ["spread", "curve", "policy", "term", "yield"])
        ]

        if not candidates:
            raise KeyError(
                "No usable credit or rate-spread proxy columns found. "
                "Need baa_10y_spread / hy_oas or available spread-like columns."
            )

        proxy_a = candidates[0]
        proxy_b = candidates[1] if len(candidates) > 1 else candidates[0]

        out = df[["date", proxy_a, proxy_b]].copy()
        out = out.rename(columns={
            proxy_a: "baa_10y_spread",
            proxy_b: "hy_oas",
        })

        print(f"Using proxy_a as baa_10y_spread: {proxy_a}")
        print(f"Using proxy_b as hy_oas: {proxy_b}")

    out = out.sort_values("date").reset_index(drop=True)

    out["baa_10y_spread_z"] = zscore(out["baa_10y_spread"])
    out["hy_oas_z"] = zscore(out["hy_oas"])

    out = out[
        [
            "date",
            "baa_10y_spread",
            "baa_10y_spread_z",
            "hy_oas",
            "hy_oas_z",
        ]
    ].dropna(subset=["date"])

    out.to_parquet(out_path, index=False)

    print("OK | credit proxy inputs v1")
    print(f"output: {out_path}")
    print(out.tail().to_string(index=False))


if __name__ == "__main__":
    main()

    