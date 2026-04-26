import json
from pathlib import Path

import pandas as pd


ROOT = Path.cwd()

CURVE_PATH = ROOT / "data/serving/rates/rates_curve_global.json"
SPREAD_PATH = ROOT / "data/serving/rates/rates_spread_global.json"
US_POLICY_PATH = ROOT / "data/serving/rates/rates_policy_pressure_data.json"
CN_POLICY_PATH = ROOT / "data/serving/rates/china/china_policy.json"
CN_Y10_PATH = ROOT / "data/serving/rates/china/china_y10_proxy.json"

OUT_PATH = ROOT / "data/serving/rates/rates_selected_global_panel.json"


def read_json_rows(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return pd.DataFrame(data)

    if isinstance(data, dict):
        for key in ("rows", "data", "series"):
            if isinstance(data.get(key), list):
                return pd.DataFrame(data[key])

    raise ValueError(f"Unsupported JSON shape: {path}")


def build_curve_countries() -> pd.DataFrame:
    curve = read_json_rows(CURVE_PATH)
    spreads = read_json_rows(SPREAD_PATH)
    us_policy = read_json_rows(US_POLICY_PATH)

    curve["date"] = pd.to_datetime(curve["date"]).dt.strftime("%Y-%m-%d")
    spreads["date"] = pd.to_datetime(spreads["date"]).dt.strftime("%Y-%m-%d")
    us_policy["date"] = pd.to_datetime(us_policy["date"]).dt.strftime("%Y-%m-%d")

    # 🔥 FIX: normalize spread column
    spreads = spreads.rename(columns={"curve_spread": "spread"})

    if "spread" not in spreads.columns:
        spreads["spread"] = pd.NA

    # 🔥 FIX: safe merge (do NOT drop curve countries)
    df = curve.merge(
        spreads[["country", "date", "spread"]],
        on=["country", "date"],
        how="left",
    )

    # 🔥 FIX: fallback to curve_spread if spread missing
    if "curve_spread" in curve.columns:
        df["spread"] = df["spread"].fillna(curve["curve_spread"])

    # 🔥 FIX: ensure symbol always exists
    if "symbol" not in df.columns:
        df["symbol"] = df["country"].astype(str) + "_CURVE"

    us_policy = us_policy[
        [
            "date",
            "EFFR",
            "policy_pressure_t1",
            "state",
        ]
    ].rename(columns={"EFFR": "policy_proxy"})

    df = df.merge(
        us_policy,
        on="date",
        how="left",
    )

    df.loc[df["country"] != "US", ["policy_proxy", "policy_pressure_t1", "state"]] = None

    df["series_type"] = "curve"
    df["curve_eligible"] = True
    df["spread_eligible"] = True

    df["y10_proxy"] = None
    df["y10_proxy_source"] = None
    df["policy_proxy_source"] = None
    df.loc[df["country"] == "US", "policy_proxy_source"] = "EFFR"

    return df[
        [
            "country",
            "date",
            "series_type",
            "curve_eligible",
            "spread_eligible",
            "y2",
            "y10",
            "spread",
            "symbol",
            "y10_proxy",
            "y10_proxy_source",
            "policy_proxy",
            "policy_proxy_source",
            "policy_pressure_t1",
            "state",
        ]
    ]


def build_china_hybrid() -> pd.DataFrame:
    policy = read_json_rows(CN_POLICY_PATH)
    y10 = read_json_rows(CN_Y10_PATH)

    policy["date"] = pd.to_datetime(policy["date"]).dt.strftime("%Y-%m-%d")
    y10["date"] = pd.to_datetime(y10["date"]).dt.strftime("%Y-%m-%d")

    df = policy.merge(y10, on="date", how="outer").sort_values("date")

    df[["value", "policy_z", "CN_policy_pressure_t1", "state"]] = (
        df[["value", "policy_z", "CN_policy_pressure_t1", "state"]].ffill()
    )

    df["country"] = "CN"
    df["series_type"] = "hybrid"
    df["curve_eligible"] = False
    df["spread_eligible"] = False

    df["y2"] = None
    df["y10"] = None
    df["spread"] = None
    df["symbol"] = "CN_HYBRID"

    df["y10_proxy"] = df["y10_proxy"] if "y10_proxy" in df.columns else None
    df["y10_proxy_source"] = "RBCNBIS"
    df["policy_proxy"] = df["value"]
    df["policy_proxy_source"] = "INTDSRCNM193N"

    return df[
        [
            "country",
            "date",
            "series_type",
            "curve_eligible",
            "spread_eligible",
            "y2",
            "y10",
            "spread",
            "symbol",
            "y10_proxy",
            "y10_proxy_source",
            "policy_proxy",
            "policy_proxy_source",
            "CN_policy_pressure_t1",
            "state",
        ]
    ].rename(columns={"CN_policy_pressure_t1": "policy_pressure_t1"})


def main():
    curve_countries = build_curve_countries()
    china = build_china_hybrid()

    out = pd.concat([curve_countries, china], ignore_index=True)
    out = out.sort_values(["country", "date"]).reset_index(drop=True)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_json(OUT_PATH, orient="records", date_format="iso", indent=2)

    print(f"Saved to {OUT_PATH}")
    print(f"Rows: {len(out)}")
    print(out.groupby(["country", "series_type"]).size())


if __name__ == "__main__":
    main()
    