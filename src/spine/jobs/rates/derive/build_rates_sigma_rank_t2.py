import json
from pathlib import Path

import pandas as pd


ROOT = Path.cwd()

PANEL_PATH = ROOT / "data/serving/rates/rates_selected_global_panel.json"
OUT_PATH = ROOT / "data/serving/rates/rates_sigma_rank.json"


CURVE_COUNTRIES = ["AU", "CA", "CH", "DE", "IT", "JP", "UK", "US"]
HYBRID_COUNTRIES = ["CN"]


def read_json_rows(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)

    if isinstance(data, list):
        return pd.DataFrame(data)

    if isinstance(data, dict):
        for key in ("rows", "data", "series"):
            if isinstance(data.get(key), list):
                return pd.DataFrame(data[key])

    raise ValueError(f"Unsupported JSON shape: {path}")


def build_curve_sigma(panel: pd.DataFrame) -> pd.DataFrame:
    df = panel.copy()

    df["country"] = df["country"].astype(str).str.upper().str.strip()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["spread"] = pd.to_numeric(df["spread"], errors="coerce")

    df = df[
        df["country"].isin(CURVE_COUNTRIES)
        & df["spread"].notna()
    ].copy()

    grouped = []

    for date, g in df.groupby("date", sort=True):
        g = g.copy()

        if len(g) < 3:
            continue

        mean = g["spread"].mean()
        std = g["spread"].std(ddof=0)

        if not pd.notna(std) or std == 0:
            g["sigma_z"] = 0.0
        else:
            g["sigma_z"] = (g["spread"] - mean) / std

        g["sigma_rank"] = g["sigma_z"].rank(
            method="dense",
            ascending=False
        ).astype(int)

        g["sigma_universe"] = len(g)
        g["sigma_mean"] = mean
        g["sigma_std"] = std
        g["series_type"] = "curve_sigma"
        g["is_hybrid"] = False
        g["display_group"] = "Curve Countries"

        grouped.append(g)

    if not grouped:
        return pd.DataFrame(
            columns=[
                "country",
                "date",
                "series_type",
                "display_group",
                "is_hybrid",
                "spread",
                "sigma_z",
                "sigma_rank",
                "sigma_universe",
                "sigma_mean",
                "sigma_std",
            ]
        )

    out = pd.concat(grouped, ignore_index=True)

    return out[
        [
            "country",
            "date",
            "series_type",
            "display_group",
            "is_hybrid",
            "spread",
            "sigma_z",
            "sigma_rank",
            "sigma_universe",
            "sigma_mean",
            "sigma_std",
        ]
    ]


def build_china_hybrid(panel: pd.DataFrame) -> pd.DataFrame:
    df = panel.copy()

    df["country"] = df["country"].astype(str).str.upper().str.strip()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    df = df[df["country"].isin(HYBRID_COUNTRIES)].copy()

    if df.empty:
        return pd.DataFrame(
            columns=[
                "country",
                "date",
                "series_type",
                "display_group",
                "is_hybrid",
                "policy_pressure_t1",
                "policy_proxy",
                "y10_proxy",
                "sigma_z",
                "sigma_rank",
                "note",
            ]
        )

    df["policy_pressure_t1"] = pd.to_numeric(
        df.get("policy_pressure_t1"),
        errors="coerce"
    )
    df["policy_proxy"] = pd.to_numeric(
        df.get("policy_proxy"),
        errors="coerce"
    )
    df["y10_proxy"] = pd.to_numeric(
        df.get("y10_proxy"),
        errors="coerce"
    )

    df["series_type"] = "hybrid_policy_proxy"
    df["display_group"] = "Hybrid / Policy Proxy"
    df["is_hybrid"] = True

    df["sigma_z"] = pd.NA
    df["sigma_rank"] = pd.NA
    df["note"] = "China is excluded from curve-country Sigma Rank v1; shown separately as hybrid policy/proxy context."

    return df[
        [
            "country",
            "date",
            "series_type",
            "display_group",
            "is_hybrid",
            "policy_pressure_t1",
            "policy_proxy",
            "y10_proxy",
            "sigma_z",
            "sigma_rank",
            "note",
        ]
    ]


def main():
    panel = read_json_rows(PANEL_PATH)

    curve_sigma = build_curve_sigma(panel)
    china_hybrid = build_china_hybrid(panel)

    out = pd.concat([curve_sigma, china_hybrid], ignore_index=True)
    out = out.sort_values(["date", "is_hybrid", "sigma_rank", "country"]).reset_index(drop=True)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_json(OUT_PATH, orient="records", date_format="iso", indent=2)

    print(f"Saved to {OUT_PATH}")
    print(f"Rows: {len(out)}")
    print(out.groupby(["display_group", "series_type"]).size())


if __name__ == "__main__":
    main()
    