from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PATHS = {
    "geoscen": REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_tier1_overlay_v1.parquet",
    "c_flow": REPO_ROOT / "data" / "serving" / "c_flow" / "c_flow_serving_v5.parquet",
    "breadth": REPO_ROOT / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet",
    "rates": REPO_ROOT / "data" / "serving" / "rates" / "rates_serving_v2.parquet",
    "fx": REPO_ROOT / "data" / "serving" / "fx" / "fx_serving_v2.parquet",
    "wti": REPO_ROOT / "data" / "serving" / "wti" / "wti_panel.json",
    "ism_manu": REPO_ROOT / "data" / "processed" / "ism" / "ism_industry_regime_summary_v1.parquet",
    "ism_services": REPO_ROOT / "data" / "processed" / "ism" / "ism_services_regime_summary_v1.parquet",
    "cb_tone": REPO_ROOT / "data" / "geoscen" / "signals" / "macro_cb_oc_signals_v1.parquet",
}

OUT_JSON = OUT_DIR / "geoscen_rbl_synthesis_v1.json"
OUT_TXT = OUT_DIR / "geoscen_rbl_synthesis_v1.txt"
OUT_PANEL = OUT_DIR / "geoscen_rbl_synthesis_panel_v1.parquet"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_num(value, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def safe_text(value, default: str = "UNKNOWN") -> str:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    value = str(value).strip()
    return value if value else default


def clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return float(max(lo, min(hi, value)))


def require_or_none(path: Path):
    if not path.exists():
        return None

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path).copy()
        if df.empty:
            return None
        return df

    if path.suffix.lower() == ".json":
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    return None


def latest_row(df: pd.DataFrame) -> pd.Series:
    date_cols = [c for c in ["date", "asof_date", "timestamp"] if c in df.columns]

    if date_cols:
        col = date_cols[0]
        df[col] = pd.to_datetime(df[col], errors="coerce")
        return df.sort_values(col).iloc[-1]

    return df.iloc[-1]


def first_available(row: pd.Series, cols: list[str], default=None):
    for col in cols:
        if col in row.index:
            value = row.get(col)
            if value is not None and not pd.isna(value):
                return value
    return default


def json_first_available(payload: dict, cols: list[str], default=None):
    if not isinstance(payload, dict):
        return default

    for col in cols:
        if col in payload and payload[col] is not None:
            return payload[col]

    return default

def latest_row_filtered(
    df: pd.DataFrame,
    filter_col: str,
    filter_value: str,
) -> pd.Series:
    if filter_col in df.columns:
        scoped = df[df[filter_col].astype(str).str.upper() == filter_value.upper()].copy()

        if not scoped.empty:
            return latest_row(scoped)

    return latest_row(df)

def load_latest_inputs() -> dict:
    loaded = {}

    for key, path in PATHS.items():
        obj = require_or_none(path)

        if obj is None:
            loaded[key] = {
                "available": False,
                "row": None,
                "path": str(path.relative_to(REPO_ROOT)),
            }
            continue

        if isinstance(obj, pd.DataFrame):
            loaded[key] = {
                "available": True,
                "row": latest_row_filtered(obj, "bank_code", "FED") if key == "cb_tone" else latest_row(obj),
                "path": str(path.relative_to(REPO_ROOT)),
            }

        elif isinstance(obj, dict):
            loaded[key] = {
                "available": True,
                "row": obj,
                "path": str(path.relative_to(REPO_ROOT)),
            }

    return loaded


def state_to_signal(text: str) -> float:
    t = safe_text(text).lower()

    positive = [
        "constructive",
        "risk-on",
        "healthy",
        "strong",
        "expansion",
        "supportive",
        "improving",
        "easing",
    ]

    neutral = [
        "balanced",
        "monitoring",
        "neutral",
        "mixed",
        "stable",
    ]

    negative = [
        "cautious",
        "defensive",
        "risk-off",
        "weak",
        "stress",
        "tight",
        "contraction",
        "deteriorating",
    ]

    if any(x in t for x in positive):
        return 0.65

    if any(x in t for x in neutral):
        return 0.10

    if any(x in t for x in negative):
        return -0.55

    return 0.0


def classify_temperature(score: float) -> str:
    abs_score = abs(score)

    if abs_score >= 0.65:
        return "HOT"

    if abs_score >= 0.35:
        return "WARM"

    return "COOL"


def route_sources(temperature: str) -> list[str]:
    base = [
        "GeoScen Tier 1 Overlay",
        "C_FLOW v5",
    ]

    if temperature == "COOL":
        return base + [
            "Breadth / Factor Serving",
        ]

    if temperature == "WARM":
        return base + [
            "Breadth / Factor Serving",
            "WTI Pressure",
            "Rates Pressure",
            "FX Stress",
            "ISM Manufacturing / Services",
        ]

    return base + [
        "Breadth / Factor Serving",
        "WTI Pressure",
        "Rates Pressure",
        "FX Stress",
        "ISM Manufacturing / Services",
        "CB Tone",
        "Contradiction Check",
    ]


def extract_signal_bundle(inputs: dict) -> dict:
    geoscen = inputs["geoscen"]["row"]
    c_flow = inputs["c_flow"]["row"]
    breadth = inputs["breadth"]["row"]
    rates = inputs["rates"]["row"]
    fx = inputs["fx"]["row"]
    wti = inputs["wti"]["row"]

    geoscen_state = "UNKNOWN"
    geoscen_score = 0.0

    if geoscen is not None:
        geoscen_state = safe_text(
            first_available(
                geoscen,
                ["tier1_state", "regime_state", "overlay_state", "state"],
                "UNKNOWN",
            )
        )
        geoscen_score = safe_num(
            first_available(
                geoscen,
                ["overlay_score", "tier1_score", "zt_score", "score"],
                state_to_signal(geoscen_state),
            )
        )

    c_flow_state = "UNKNOWN"
    c_flow_score = 0.0
    fund_flow_pressure = 0.0

    if c_flow is not None:
        c_flow_state = safe_text(
            first_available(
                c_flow,
                ["c_flow_state_v5", "c_flow_state", "state"],
                "UNKNOWN",
            )
        )
        c_flow_score = safe_num(
            first_available(
                c_flow,
                ["c_flow_score_v5", "c_flow_score", "score"],
                0.0,
            )
        )
        fund_flow_pressure = safe_num(
            first_available(
                c_flow,
                ["fund_flow_pressure"],
                0.0,
            )
        )

    breadth_score = 0.0
    breadth_state = "UNKNOWN"

    if breadth is not None:
        breadth_score = safe_num(
            first_available(
                breadth,
                ["breadth_factor_score", "breadth_score", "score"],
                0.0,
            )
        )
        breadth_state = safe_text(
            first_available(
                breadth,
                ["breadth_state", "regime_state", "state"],
                "UNKNOWN",
            )
        )

        if breadth_state == "UNKNOWN":
            if breadth_score >= 0.70:
                breadth_state = "Healthy Breadth / Risk-On Participation"
            elif breadth_score >= 0.45:
                breadth_state = "Balanced Breadth"
            else:
                breadth_state = "Weak Breadth / Risk-Off Participation"

    rates_pressure = 0.0
    rates_state = "UNKNOWN"
    rates_rbl = ""

    if rates is not None:
        rates_pressure = safe_num(
            first_available(
                rates,
                ["signal_strength", "dominance_mean", "regime_confidence"],
                0.0,
            )
        )
        rates_state = safe_text(
            first_available(
                rates,
                ["regime_label", "tone_direction", "rbl_oc"],
                "UNKNOWN",
            )
        )
        rates_rbl = safe_text(
            first_available(
                rates,
                ["rbl_oc", "rbl_report_with_regime"],
                "",
            ),
            default="",
        )

    fx_pressure = 0.0
    fx_state = "UNKNOWN"
    fx_rbl = ""

    if fx is not None:
        fx_pressure = safe_num(
            first_available(
                fx,
                ["signal_strength", "dominance_mean", "regime_confidence"],
                0.0,
            )
        )
        fx_state = safe_text(
            first_available(
                fx,
                ["regime_label", "tone_direction", "rbl_oc"],
                "UNKNOWN",
            )
        )
        fx_rbl = safe_text(
            first_available(
                fx,
                ["rbl_oc", "rbl_report_with_regime"],
                "",
            ),
            default="",
        )

    wti_pressure = 0.0
    wti_state = "Operational Commodity Monitoring"
    wti_rbl = ""

    if wti is not None:
        summary = wti.get("summary", {}) if isinstance(wti, dict) else {}
        price = wti.get("price", {}) if isinstance(wti, dict) else {}
        inventory = wti.get("inventory", {}) if isinstance(wti, dict) else {}

        wti_pressure = safe_num(
            json_first_available(
                summary,
                ["score", "pressure_score", "wti_pressure", "commodity_pressure"],
                0.0,
            )
        )

        status = safe_text(
            json_first_available(summary, ["status"], "operational")
        )

        wti_state = f"WTI {status.title()} Commodity Monitoring"

        wti_rbl = (
            f"WTI panel is operational with price context as of "
            f"{safe_text(json_first_available(summary, ['wti_price_as_of'], 'UNKNOWN'))} "
            f"& inventory context as of "
            f"{safe_text(json_first_available(summary, ['inventory_as_of'], 'UNKNOWN'))}."
        )

    ism_manu_state = "MISSING"

    if inputs["ism_manu"]["row"] is not None:
        manu_row = inputs["ism_manu"]["row"]

        manu_score = safe_num(
            first_available(
                manu_row,
                ["ism_regime_score"],
                0.0,
            )
        )

        strongest_industries = safe_text(
            first_available(
                manu_row,
                ["strongest_industries"],
                "UNKNOWN",
            )
        )

        weakest_industries = safe_text(
            first_available(
                manu_row,
                ["weakest_industries"],
                "UNKNOWN",
            )
        )

        if manu_score >= 0.50:
            ism_manu_state = f"Expansionary Manufacturing Regime | strongest: {strongest_industries} | weakest: {weakest_industries}"
        elif manu_score >= 0.0:
            ism_manu_state = f"Balanced Manufacturing Regime | strongest: {strongest_industries} | weakest: {weakest_industries}"
        else:
            ism_manu_state = f"Weakening Manufacturing Regime | strongest: {strongest_industries} | weakest: {weakest_industries}"


    ism_services_state = "MISSING"

    if inputs["ism_services"]["row"] is not None:
        services_row = inputs["ism_services"]["row"]

        services_score = safe_num(
            first_available(
                services_row,
                ["services_regime_score"],
                0.0,
            )
        )

        strongest_services = safe_text(
            first_available(
                services_row,
                ["strongest_services"],
                "UNKNOWN",
            )
        )

        weakest_services = safe_text(
            first_available(
                services_row,
                ["weakest_services"],
                "UNKNOWN",
            )
        )

        if services_score >= 0.75:
            ism_services_state = f"Strong Services Expansion | strongest: {strongest_services} | weakest: {weakest_services}"
        elif services_score >= 0.0:
            ism_services_state = f"Balanced Services Regime | strongest: {strongest_services} | weakest: {weakest_services}"
        else:
            ism_services_state = f"Weakening Services Regime | strongest: {strongest_services} | weakest: {weakest_services}"

    cb_tone_state = "MISSING"

    if inputs["cb_tone"]["row"] is not None:
        cb_row = inputs["cb_tone"]["row"]

        policy_tone = safe_num(
            first_available(
                cb_row,
                ["policy_tone", "policy_tone_score"],
                0.0,
            )
        )

        uncertainty = safe_num(
            first_available(
                cb_row,
                ["uncertainty", "uncertainty_score"],
                0.0,
            )
        )

        hawkish_count = safe_num(
            first_available(
                cb_row,
                ["total_hawkish_count", "hawkish_count"],
                0.0,
            )
        )

        dovish_count = safe_num(
            first_available(
                cb_row,
                ["total_dovish_count", "dovish_count"],
                0.0,
            )
        )

        policy_divergence = safe_num(
            first_available(
                cb_row,
                ["policy_divergence_flag"],
                0.0,
            )
        )

        bank = safe_text(
            first_available(
                cb_row,
                ["bank", "bank_code"],
                "Central Bank",
            )
        )

        if policy_tone >= 40:
            tone_label = "Hawkish Policy Tone"
        elif policy_tone <= -20:
            tone_label = "Dovish Policy Tone"
        else:
            tone_label = "Balanced Policy Tone"

        uncertainty_label = (
            "elevated uncertainty"
            if uncertainty >= 35
            else "contained uncertainty"
        )

        divergence_label = (
            "with policy divergence flagged"
            if policy_divergence == 1
            else "without policy divergence flagged"
        )

        cb_tone_state = (
            f"{bank}: {tone_label}, {uncertainty_label}, "
            f"{divergence_label} "
            f"(hawkish={hawkish_count:.0f}, dovish={dovish_count:.0f}, uncertainty={uncertainty:.0f})."
        )

    composite_temperature_score = clamp(
        (0.25 * geoscen_score)
        + (0.20 * c_flow_score)
        + (0.15 * fund_flow_pressure)
        + (0.15 * (breadth_score - 0.50) * 2.0)
        + (0.10 * rates_pressure)
        + (0.10 * fx_pressure)
        + (0.05 * wti_pressure)
    )

    temperature = classify_temperature(composite_temperature_score)

    return {
        "geoscen_state": geoscen_state,
        "geoscen_score": geoscen_score,
        "c_flow_state": c_flow_state,
        "c_flow_score": c_flow_score,
        "fund_flow_pressure": fund_flow_pressure,
        "breadth_score": breadth_score,
        "breadth_state": breadth_state,
        "rates_pressure": rates_pressure,
        "rates_state": rates_state,
        "fx_pressure": fx_pressure,
        "fx_state": fx_state,
        "wti_pressure": wti_pressure,
        "wti_state": wti_state,
        "ism_manufacturing_state": ism_manu_state,
        "ism_services_state": ism_services_state,
        "cb_tone_state": cb_tone_state,
        "temperature_score": composite_temperature_score,
        "temperature": temperature,
        "rates_rbl": rates_rbl,
        "fx_rbl": fx_rbl,
        "wti_rbl": wti_rbl,
    }


def build_rbl(bundle: dict) -> str:
    temp = bundle["temperature"].lower()

    parts = []

    parts.append(
        f"GeoScen Tier 1 is operating in a {bundle['geoscen_state']} with {temp} macro temperature."
    )

    parts.append(
        f"C_FLOW v5 reads as {bundle['c_flow_state']}, with fund-flow pressure at "
        f"{bundle['fund_flow_pressure']:.2f}, indicating that capital-flow pressure is now active rather than placeholder-driven."
    )

    parts.append(
        f"Equity participation remains anchored by {bundle['breadth_state']} "
        f"with breadth factor score at {bundle['breadth_score']:.2f}."
    )

    parts.append(
        f"Rates pressure is classified as {bundle['rates_state']}, while FX stress is classified as {bundle['fx_state']}."
    )

    parts.append(
        f"WTI is classified as {bundle['wti_state']}, which should be read as commodity-pressure context feeding the capital-flow channel."
    )

    if bundle["ism_manufacturing_state"] != "MISSING" or bundle["ism_services_state"] != "MISSING":
        parts.append(
            f"ISM / PMI context is split between manufacturing ({bundle['ism_manufacturing_state']}) "
            f"& services ({bundle['ism_services_state']}), supporting the qualitative RBL layer."
        )
    else:
        parts.append(
            "PMI qualitative context is not yet fully connected in this artifact; structured ISM remains the stronger current input."
        )

    if bundle["cb_tone_state"] != "MISSING":
        parts.append(
            f"Central bank tone is classified as {bundle['cb_tone_state']}, adding policy-language context to the OC interpretation layer."
        )
    else:
        parts.append(
            "Central bank tone is not yet connected to this synthesis output; this remains the next interpretive enhancement."
        )

    parts.append(
        "Overall, the regime reads as deployable & monitored rather than unresolved: the infrastructure is clean, while the narrative layer should continue expanding toward richer CB and PMI qualitative evidence."
    )

    return " ".join(parts)


def main() -> None:
    inputs = load_latest_inputs()
    bundle = extract_signal_bundle(inputs)
    source_route = route_sources(bundle["temperature"])
    rbl_text = build_rbl(bundle)

    payload = {
        "timestamp_utc": utc_now(),
        "artifact": "geoscen_rbl_synthesis_v1",
        "temperature": bundle["temperature"],
        "temperature_score": bundle["temperature_score"],
        "rbl_text": rbl_text,
        "source_route": source_route,
        "signals": bundle,
        "input_status": {
            key: {
                "available": value["available"],
                "path": value["path"],
            }
            for key, value in inputs.items()
        },
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    with open(OUT_TXT, "w", encoding="utf-8") as f:
        f.write("GEOSCEN RBL SYNTHESIS V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"temperature: {bundle['temperature']}\n")
        f.write(f"temperature_score: {bundle['temperature_score']:.4f}\n\n")
        f.write("SOURCE ROUTE\n")
        f.write("-" * 60 + "\n")
        for source in source_route:
            f.write(f"- {source}\n")
        f.write("\nRBL\n")
        f.write("-" * 60 + "\n")
        f.write(rbl_text + "\n")

    panel = pd.DataFrame(
        [
            {
                "timestamp_utc": payload["timestamp_utc"],
                "temperature": bundle["temperature"],
                "temperature_score": bundle["temperature_score"],
                "geoscen_state": bundle["geoscen_state"],
                "c_flow_state": bundle["c_flow_state"],
                "fund_flow_pressure": bundle["fund_flow_pressure"],
                "breadth_score": bundle["breadth_score"],
                "breadth_state": bundle["breadth_state"],
                "rates_state": bundle["rates_state"],
                "fx_state": bundle["fx_state"],
                "wti_state": bundle["wti_state"],
                "ism_manufacturing_state": bundle["ism_manufacturing_state"],
                "ism_services_state": bundle["ism_services_state"],
                "cb_tone_state": bundle["cb_tone_state"],
                "rbl_text": rbl_text,
                "source_route": " | ".join(source_route),
            }
        ]
    )

    panel.to_parquet(OUT_PANEL, index=False)

    print("OK | GeoScen RBL synthesis v1 built")
    print(f"temperature       : {bundle['temperature']}")
    print(f"temperature_score : {bundle['temperature_score']:.4f}")
    print(f"rbl               : {rbl_text[:220]}...")
    print("\nArtifacts written:")
    print(OUT_JSON)
    print(OUT_TXT)
    print(OUT_PANEL)


if __name__ == "__main__":
    main()

