from pathlib import Path
import json
import pandas as pd

REPO_ROOT = Path.cwd()

INPUTS = {
    "rates_zt": REPO_ROOT / "data/rates/zt/rates_core_zt.parquet",
    "fx_zt": REPO_ROOT / "data/serving/fx/fx_zt_v1.parquet",
    "cfl_zt": REPO_ROOT / "data/processed/cflow/cfl_zt_panel.parquet",
    "equities_zt": REPO_ROOT / "data/serving/equities/equities_zt.parquet",
}

OUTFILE = REPO_ROOT / "data/system/system_zt.parquet"
LATEST_JSON = REPO_ROOT / "data/system/system_latest.json"

MODULE_VALUE_COLS = {
    "rates_zt": "rates_core_zt",
    "fx_zt": "fx_zt",
    "cfl_zt": "cfl_zt",
    "equities_zt": "equities_risk_expression_zt",
}

WEIGHTS = {
    "rates_zt": 0.30,
    "fx_zt": 0.25,
    "cfl_zt": 0.25,
    "equities_zt": 0.20,
}


def expanding_z(s: pd.Series) -> pd.Series:
    return (s - s.expanding().mean()) / s.expanding().std()


def load_module(module_name: str, path: Path, value_col: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {module_name}: {path}")

    df = pd.read_parquet(path).copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    if value_col not in df.columns:
        raise KeyError(f"{module_name} missing value column: {value_col}. Found: {list(df.columns)}")

    out = df[["date", value_col]].rename(columns={value_col: module_name})
    out = out.groupby("date", as_index=False).mean()
    out = out.sort_values("date").reset_index(drop=True)

    return out


def main():
    frames = []

    for module_name, path in INPUTS.items():
        frames.append(load_module(module_name, path, MODULE_VALUE_COLS[module_name]))

    # Use RATES as primary system calendar because it is the broadest daily structural layer.
    system = frames[0].copy()

    for frame in frames[1:]:
        system = system.merge(frame, on="date", how="left")

    module_cols = list(INPUTS.keys())

    # Forward-fill lower-frequency modules onto the RATES calendar.
    system[module_cols] = system[module_cols].ffill()

    # Do not produce system Zt until all module values exist.
    system = system.dropna(subset=module_cols).reset_index(drop=True)

    # Normalize each module into comparable contribution space.
    for col in module_cols:
        system[f"{col}_norm"] = expanding_z(system[col]).fillna(0)

    norm_cols = [f"{c}_norm" for c in module_cols]

    system["system_zt_raw"] = 0.0
    for col in module_cols:
        system["system_zt_raw"] += system[f"{col}_norm"] * WEIGHTS[col]

    system["system_zt"] = expanding_z(system["system_zt_raw"]).fillna(0)

    out_cols = (
        ["date"]
        + module_cols
        + norm_cols
        + ["system_zt_raw", "system_zt"]
    )

    out = system[out_cols].copy()

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTFILE, index=False)

    latest = out.tail(1).copy()
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")

    payload = {
        "module": "SYSTEM",
        "description": "Cross-module deterministic macro system state Zt",
        "weights": WEIGHTS,
        "latest": latest.to_dict("records")[0],
    }

    LATEST_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("OK | SYSTEM Zt built")
    print("OUT:", OUTFILE)
    print("LATEST:", LATEST_JSON)
    print(out.tail())


if __name__ == "__main__":
    main()

    