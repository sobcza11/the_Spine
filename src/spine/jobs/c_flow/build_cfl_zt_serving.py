from pathlib import Path
import json
import pandas as pd

REPO_ROOT = Path.cwd()

INFILE = REPO_ROOT / "data/processed/cflow/cfl_cot_panel_clean.parquet"

ZT_OUT = REPO_ROOT / "data/processed/cflow/cfl_zt_panel.parquet"
PANEL_JSON = REPO_ROOT / "data/serving/cflow/commflow_panel.json"
LATEST_JSON = REPO_ROOT / "data/serving/cflow/commflow_latest.json"

FACTOR_MAP = {
    "WTI": "ENERGY",
    "GOLD": "MONETARY_HEDGE",
    "SILVER": "MONETARY_HEDGE",
    "COPPER": "GROWTH_METALS",
    "CORN": "GRAINS",
    "SOYBEANS": "GRAINS",
}

def expanding_z(s: pd.Series) -> pd.Series:
    return (s - s.expanding().mean()) / s.expanding().std()


def main():
    df = pd.read_parquet(INFILE).copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["asset", "date"]).reset_index(drop=True)

    required = ["asset", "date", "managed_money_z"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required C-FL columns: {missing}")

    if "delta" not in df.columns:
        if "managed_money_pct_oi" in df.columns:
            df["delta"] = df.groupby("asset")["managed_money_pct_oi"].diff()
        else:
            df["delta"] = df.groupby("asset")["managed_money_z"].diff()

    df["delta"] = df["delta"].fillna(0)

    df["crowding_index_v2"] = (
        df["managed_money_z"].abs() + df["delta"].abs()
    ) / 2

    asset_state = (
        df.groupby(["date", "asset"], as_index=False)
        .agg(
            managed_money_z=("managed_money_z", "mean"),
            delta=("delta", "mean"),
            crowding_index=("crowding_index_v2", "mean"),
        )
    )

    asset_state["factor"] = asset_state["asset"].map(FACTOR_MAP)

    factor_state = (
        asset_state.groupby(["date", "factor"], as_index=False)
        .agg(
            factor_positioning_zt=("managed_money_z", "mean"),
            factor_flow_zt=("delta", "mean"),
            factor_crowding_zt=("crowding_index", "mean"),
        )
    )

    factor_state["factor_zt_raw"] = factor_state[
        ["factor_positioning_zt", "factor_flow_zt", "factor_crowding_zt"]
    ].mean(axis=1)

    FACTOR_OUT = REPO_ROOT / "data/processed/cflow/cfl_factor_panel.parquet"
    factor_state.to_parquet(FACTOR_OUT, index=False)

    daily = (
        asset_state.groupby("date", as_index=False)
        .agg(
            cfl_positioning_zt=("managed_money_z", "mean"),
            cfl_flow_zt=("delta", "mean"),
            cfl_crowding_zt=("crowding_index", "mean"),
        )
    )

    daily["cfl_zt_raw"] = daily[
        ["cfl_positioning_zt", "cfl_flow_zt", "cfl_crowding_zt"]
    ].mean(axis=1)

    daily["cfl_zt"] = expanding_z(daily["cfl_zt_raw"]).fillna(0)

    ZT_OUT.parent.mkdir(parents=True, exist_ok=True)
    PANEL_JSON.parent.mkdir(parents=True, exist_ok=True)

    daily.to_parquet(ZT_OUT, index=False)

    panel_payload = {
        "module": "C-FL",
        "description": "Commodity Flow positioning & crowding layer",
        "assets": sorted(df["asset"].unique().tolist()),
        "latest_date": str(daily["date"].max().date()),
        "records": daily.tail(260).assign(date=lambda x: x["date"].dt.strftime("%Y-%m-%d")).to_dict("records"),
    }

    latest_row = daily.tail(1).assign(date=lambda x: x["date"].dt.strftime("%Y-%m-%d")).to_dict("records")[0]
    latest_payload = {
        "module": "C-FL",
        "latest": latest_row,
        "note": "WTI is included as a component asset inside C-FL.",
    }

    PANEL_JSON.write_text(json.dumps(panel_payload, indent=2), encoding="utf-8")
    LATEST_JSON.write_text(json.dumps(latest_payload, indent=2), encoding="utf-8")

    print("OK | C-FL Zt + serving files built")
    print(daily.tail())


if __name__ == "__main__":
    main()

