from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "c_flow"
OUT_DIR.mkdir(parents=True, exist_ok=True)

C_FLOW_V4_PATH = REPO_ROOT / "data" / "serving" / "c_flow" / "c_flow_serving_v4.parquet"
BREADTH_PATH = REPO_ROOT / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet"
EQUITIES_PATH = REPO_ROOT / "data" / "serving" / "equities" / "equities_serving_v2.parquet"
GEOSCEN_OVERLAY_PATH = REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_tier1_overlay_v1.parquet"

OUT_PARQUET = OUT_DIR / "c_flow_serving_v5.parquet"
OUT_JSON = OUT_DIR / "c_flow_latest_v5.json"
OUT_SUMMARY = OUT_DIR / "c_flow_v5_summary.txt"


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")


def read_parquet(path: Path) -> pd.DataFrame:
    require_file(path)
    return pd.read_parquet(path).copy()


def latest_row(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        raise ValueError("Input dataframe is empty.")

    date_cols = [c for c in ["date", "asof_date", "timestamp"] if c in df.columns]

    if date_cols:
        col = date_cols[0]
        df[col] = pd.to_datetime(df[col], errors="coerce")
        return df.sort_values(col).iloc[-1]

    return df.iloc[-1]


def safe_num(value, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def safe_text(value, default: str = "UNKNOWN") -> str:
    if value is None or pd.isna(value):
        return default
    return str(value)


def clamp(value: float, lower: float = -1.0, upper: float = 1.0) -> float:
    return float(max(lower, min(upper, value)))


def normalize_0_1_to_signal(value: float) -> float:
    return clamp((value - 0.5) * 2.0)


def regime_to_signal(text: str) -> float:

    t = str(text).lower()

    positive = [
        "risk-on",
        "healthy",
        "expansion",
        "strong",
        "supportive",
        "bull",
        "positive",
        "constructive",
        "improving",
    ]

    balanced = [
        "balanced",
        "monitoring",
        "neutral",
        "mixed",
    ]

    negative = [
        "risk-off",
        "weak",
        "contraction",
        "stress",
        "bear",
        "negative",
        "cautious",
        "defensive",
    ]

    if any(x in t for x in positive):
        return 0.75

    if any(x in t for x in balanced):
        return 0.15

    if any(x in t for x in negative):
        return -0.50

    return 0.0


def extract_first_available(row: pd.Series, names: list[str], default=None):
    for name in names:
        if name in row.index:
            value = row.get(name)
            if value is not None and not pd.isna(value):
                return value
    return default


def classify_c_flow(score: float) -> str:
    if score >= 0.45:
        return "Risk-On Capital Flow"
    if score >= 0.15:
        return "Constructive / Improving Capital Flow"
    if score > -0.15:
        return "Balanced Capital Flow"
    if score > -0.45:
        return "Cautious / Defensive Capital Flow"
    return "Risk-Off Capital Flow"


def main() -> None:
    df_v4 = read_parquet(C_FLOW_V4_PATH)
    df_breadth = read_parquet(BREADTH_PATH)
    df_equities = read_parquet(EQUITIES_PATH)
    df_geoscen = read_parquet(GEOSCEN_OVERLAY_PATH)

    v4 = latest_row(df_v4)
    breadth = latest_row(df_breadth)
    equities = latest_row(df_equities)
    geoscen = latest_row(df_geoscen)

    base_c_flow_score = safe_num(
        extract_first_available(
            v4,
            ["c_flow_score", "capital_flow_score", "score"],
            0.0,
        )
    )

    fx_pressure = safe_num(
        extract_first_available(
            v4,
            ["fx_pressure", "fx_score", "currency_pressure"],
            0.0,
        )
    )

    rates_pressure = safe_num(
        extract_first_available(
            v4,
            ["rates_pressure", "rates_score", "yield_pressure"],
            0.0,
        )
    )

    credit_pressure = safe_num(
        extract_first_available(
            v4,
            ["credit_pressure", "credit_score"],
            0.0,
        )
    )

    cot_pressure = safe_num(
        extract_first_available(
            v4,
            ["cot_pressure", "btc_cot_pressure", "positioning_pressure"],
            0.0,
        )
    )

    breadth_factor_score_raw = safe_num(
        extract_first_available(
            breadth,
            ["breadth_factor_score", "score", "breadth_score"],
            0.5,
        ),
        default=0.5,
    )

    breadth_signal = normalize_0_1_to_signal(breadth_factor_score_raw)

    breadth_state = safe_text(
        extract_first_available(
            breadth,
            ["breadth_state", "regime_state", "state"],
            None,
        ),
        default="UNKNOWN",
    )

    if breadth_state == "UNKNOWN":
        if breadth_factor_score_raw >= 0.70:
            breadth_state = "Healthy Breadth / Risk-On Participation"
        elif breadth_factor_score_raw >= 0.45:
            breadth_state = "Balanced Breadth"
        else:
            breadth_state = "Weak Breadth / Risk-Off Participation"

    equity_regime_text = safe_text(
        extract_first_available(
            equities,
            ["market_regime", "equity_regime", "regime_state", "state"],
            None,
        ),
        default="UNKNOWN",
    )

    if equity_regime_text == "UNKNOWN":
        if breadth_factor_score_raw >= 0.70:
            equity_regime_text = "Constructive Equity Regime"
        elif breadth_factor_score_raw >= 0.45:
            equity_regime_text = "Balanced Equity Regime"
        else:
            equity_regime_text = "Cautious Equity Regime"

    equity_regime_signal = regime_to_signal(equity_regime_text)

    geoscen_state = safe_text(
        extract_first_available(
            geoscen,
            ["tier1_state", "regime_state", "overlay_state", "state"],
            "UNKNOWN",
        )
    )

    geoscen_signal = regime_to_signal(geoscen_state)

    services_signal = safe_num(
        extract_first_available(
            geoscen,
            ["services_score", "ism_services_score"],
            0.0,
        )
    )

    manufacturing_signal = safe_num(
        extract_first_available(
            geoscen,
            ["manufacturing_score", "ism_manufacturing_score"],
            0.0,
        )
    )

    real_fund_flow_pressure = clamp(
        (0.40 * breadth_signal)
        + (0.25 * equity_regime_signal)
        + (0.20 * geoscen_signal)
        + (0.10 * services_signal)
        + (0.05 * manufacturing_signal)
    )

    c_flow_v5_score = clamp(
        (0.35 * base_c_flow_score)
        + (0.25 * real_fund_flow_pressure)
        + (0.15 * fx_pressure)
        + (0.10 * rates_pressure)
        + (0.10 * credit_pressure)
        + (0.05 * cot_pressure)
    )

    c_flow_v5_state = classify_c_flow(c_flow_v5_score)

    latest_date = extract_first_available(
        v4,
        ["date", "asof_date", "timestamp"],
        pd.Timestamp.today().normalize(),
    )

    out = pd.DataFrame(
        [
            {
                "date": pd.to_datetime(latest_date, errors="coerce"),
                "c_flow_score_v4": base_c_flow_score,
                "c_flow_score_v5": c_flow_v5_score,
                "c_flow_state_v5": c_flow_v5_state,
                "fund_flow_pressure": real_fund_flow_pressure,
                "breadth_factor_score": breadth_factor_score_raw,
                "breadth_signal": breadth_signal,
                "breadth_state": breadth_state,
                "equity_regime": equity_regime_text,
                "equity_regime_signal": equity_regime_signal,
                "geoscen_tier1_state": geoscen_state,
                "geoscen_signal": geoscen_signal,
                "services_signal": services_signal,
                "manufacturing_signal": manufacturing_signal,
                "fx_pressure": fx_pressure,
                "rates_pressure": rates_pressure,
                "credit_pressure": credit_pressure,
                "cot_pressure": cot_pressure,
                "source_contract": "c_flow_v5_real_breadth_equity_geoscen",
            }
        ]
    )

    out.to_parquet(OUT_PARQUET, index=False)

    latest_payload = out.iloc[-1].to_dict()

    for key, value in list(latest_payload.items()):
        if isinstance(value, pd.Timestamp):
            latest_payload[key] = value.isoformat()
        elif isinstance(value, np.generic):
            latest_payload[key] = value.item()

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(latest_payload, f, indent=4)

    with open(OUT_SUMMARY, "w", encoding="utf-8") as f:
        f.write("C_FLOW SERVING V5 SUMMARY\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"c_flow_score_v5: {c_flow_v5_score:.4f}\n")
        f.write(f"c_flow_state_v5: {c_flow_v5_state}\n")
        f.write(f"fund_flow_pressure: {real_fund_flow_pressure:.4f}\n")
        f.write(f"breadth_factor_score: {breadth_factor_score_raw:.4f}\n")
        f.write(f"breadth_state: {breadth_state}\n")
        f.write(f"equity_regime: {equity_regime_text}\n")
        f.write(f"geoscen_tier1_state: {geoscen_state}\n")

    print("OK | C_FLOW serving v5 built")
    print(f"c_flow_score_v5: {c_flow_v5_score:.4f}")
    print(f"c_flow_state_v5: {c_flow_v5_state}")
    print(f"fund_flow_pressure: {real_fund_flow_pressure:.4f}")
    print("\nArtifacts written:")
    print(OUT_PARQUET)
    print(OUT_JSON)
    print(OUT_SUMMARY)


if __name__ == "__main__":
    main()

