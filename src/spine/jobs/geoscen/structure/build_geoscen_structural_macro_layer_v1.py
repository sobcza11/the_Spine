from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "structure"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "rbl": REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_rbl_synthesis_v1.json",
    "drift": REPO_ROOT / "data" / "serving" / "geoscen" / "drift" / "geoscen_historical_narrative_drift_engine_v1.json",
    "contradiction": REPO_ROOT / "data" / "serving" / "geoscen" / "contradiction" / "geoscen_contradiction_engine_v1.json",
    "rates": REPO_ROOT / "data" / "serving" / "rates" / "rates_zt_latest.json",
    "wti": REPO_ROOT / "data" / "serving" / "wti" / "wti_inflation_pressure.json",
    "breadth": REPO_ROOT / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet",
    "c_flow": REPO_ROOT / "data" / "serving" / "c_flow" / "c_flow_latest_v5.json",
}


STRUCTURAL_SIGNALS = {
    "inflation": ["cpi_yoy", "core_cpi_yoy", "pce_yoy"],
    "growth": ["gdp_yoy", "industrial_production_yoy", "retail_sales_yoy"],
    "labor": ["unemployment_rate", "nfp_yoy", "labor_force_participation"],
    "liquidity": ["m2_yoy", "fed_balance_sheet_yoy"],
    "rates": ["us10y", "fed_funds"],
    "consumer": ["consumer_sentiment", "real_disposable_income_yoy"],
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, dict):
        obj["available"] = True
    return obj


def latest_valid(df: pd.DataFrame, cols: list[str]) -> dict:
    out = {}

    for col in cols:
        if col in df.columns:
            series = df[col].dropna()
            out[col] = float(series.iloc[-1]) if not series.empty else None
        else:
            out[col] = None

    return out


def classify_macro_state(values: dict) -> str:
    vals = [v for v in values.values() if isinstance(v, (float, int))]

    if not vals:
        return "unavailable"

    avg = sum(vals) / len(vals)

    if avg >= 5:
        return "strong_expansion"
    if avg >= 2:
        return "moderate_expansion"
    if avg >= 0:
        return "stable_growth"
    if avg >= -2:
        return "softening"
    return "contractionary"


def build_signal_rows(df: pd.DataFrame) -> list[dict]:
    rows = []

    for family, cols in STRUCTURAL_SIGNALS.items():
        values = latest_valid(df, cols)

        rows.append({
            "signal_family": family,
            "macro_state": classify_macro_state(values),
            "signals": values,
            "available_signal_count": len(
                [v for v in values.values() if v is not None]
            ),
        })

    return rows


def main() -> None:
    rbl = read_json(SOURCE_CONTRACT["rbl"])
    drift = read_json(SOURCE_CONTRACT["drift"])
    contradiction = read_json(SOURCE_CONTRACT["contradiction"])
    rates = read_json(SOURCE_CONTRACT["rates"])
    wti = read_json(SOURCE_CONTRACT["wti"])
    c_flow = read_json(SOURCE_CONTRACT["c_flow"])

    breadth_path = SOURCE_CONTRACT["breadth"]
    breadth = {"available": False}
    if breadth_path.exists():
        bdf = pd.read_parquet(breadth_path)
        if not bdf.empty:
            breadth = bdf.tail(1).iloc[0].to_dict()
            breadth["available"] = True

    rbl_text = str(rbl.get("rbl", "")).lower()

    signal_rows = [
        {
            "signal_family": "inflation",
            "macro_state": "active_monitoring" if "inflation" in rbl_text or wti.get("available") else "unavailable",
            "source": "WTI inflation pressure / RBL",
            "available": bool(wti.get("available")),
        },
        {
            "signal_family": "growth",
            "macro_state": "softening" if "weakening manufacturing" in rbl_text else "stable_growth",
            "source": "PMI / ISM qualitative RBL",
            "available": bool(rbl.get("available")),
        },
        {
            "signal_family": "labor",
            "macro_state": "pending_source",
            "source": "NFP / unemployment not yet connected",
            "available": False,
        },
        {
            "signal_family": "liquidity",
            "macro_state": "active_monitoring" if c_flow.get("available") else "pending_source",
            "source": "C_FLOW v5",
            "available": bool(c_flow.get("available")),
        },
        {
            "signal_family": "rates",
            "macro_state": "active_monitoring" if rates.get("available") else "pending_source",
            "source": "Rates Zₜ",
            "available": bool(rates.get("available")),
        },
        {
            "signal_family": "consumer",
            "macro_state": "pending_source",
            "source": "consumer sentiment not yet connected",
            "available": False,
        },
    ]

    structural_pressure_score = round(
        sum(1 for row in signal_rows if row["macro_state"] in {"softening", "active_monitoring"})
        / len(signal_rows),
        4,
    )

    structural_regime = (
        "Moderate Structural Monitoring"
        if structural_pressure_score >= 0.50
        else "Low Structural Monitoring"
    )

    payload = {
        "component": "GeoScen Structural Macro Layer",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "structural_pressure_score": structural_pressure_score,
        "structural_regime": structural_regime,
        "signal_family_count": len(signal_rows),
        "signal_rows": signal_rows,
        "drift_available": drift.get("available"),
        "contradiction_available": contradiction.get("available"),
        "oraclechambers_ready": True,
        "governance": {
            "rules_based": True,
            "ai_last": True,
            "explainable": True,
            "uses_existing_serving_sources": True,
            "full_macro_panel_required": False,
        },
    }

    out_json = OUT_DIR / "geoscen_structural_macro_layer_v1.json"
    out_txt = OUT_DIR / "geoscen_structural_macro_layer_v1.txt"
    out_parquet = OUT_DIR / "geoscen_structural_macro_layer_panel_v1.parquet"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    pd.DataFrame(signal_rows).to_parquet(out_parquet, index=False)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN STRUCTURAL MACRO LAYER V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"structural_pressure_score: {payload['structural_pressure_score']}\n")
        f.write(f"structural_regime: {payload['structural_regime']}\n")
        f.write(f"signal_family_count: {payload['signal_family_count']}\n\n")

        f.write("STRUCTURAL SIGNAL ROWS\n")
        f.write("-" * 60 + "\n")
        for row in signal_rows:
            f.write(
                f"- {row['signal_family']} | "
                f"state={row['macro_state']} | "
                f"available={row['available']} | "
                f"source={row['source']}\n"
            )

    print("OK | GeoScen Structural Macro Layer v1 built")
    print(f"structural_pressure_score : {payload['structural_pressure_score']}")
    print(f"structural_regime         : {payload['structural_regime']}")
    print(f"signal_family_count       : {payload['signal_family_count']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)
    print(out_parquet)


if __name__ == "__main__":
    main()

