from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.spine.jobs.llm.load_local_inputs_v1 import load_manifest, read_approved_inputs


REPO_ROOT = Path.cwd()
OUTPUT_PATH = REPO_ROOT / "data" / "llm" / "qualayer" / "qualayer_packet_v1.json"


def direction(value: Any) -> str:
    if value is None:
        return "unknown"

    try:
        if pd.isna(value):
            return "unknown"
        value = float(value)
    except (TypeError, ValueError):
        return "unknown"

    if value > 0:
        return "positive"
    if value < 0:
        return "negative"
    return "neutral"


def latest_row(df: pd.DataFrame) -> dict[str, Any]:
    if "date" not in df.columns:
        return df.tail(1).iloc[0].to_dict()

    clean = df.dropna(subset=["date"]).sort_values("date")
    if clean.empty:
        return df.tail(1).iloc[0].to_dict()

    return clean.tail(1).iloc[0].to_dict()


def json_safe(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if pd.isna(value) if not isinstance(value, (list, dict, str)) else False:
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def clean_record(record: dict[str, Any]) -> dict[str, Any]:
    return {k: json_safe(v) for k, v in record.items()}


def build_qualayer_packet(
    manifest: dict[str, Any],
    inputs: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    cb = clean_record(latest_row(inputs["isovector_macro_cb_view"]))
    pmi = clean_record(latest_row(inputs["pmi_geoscen_zt_input"]))
    rates = clean_record(
    latest_non_null_row(
        inputs["isovector_macro_cb_rates_join"],
        ["it_de_10y_spread", "it_de_10y_spread_z"],
            )
        )

    packet = {
        "packet_version": "v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "system": "the_Spine",
        "layer": "QuaLayer",
        "definition": "structured_rbl_packet",
        "rules": manifest["rules"],
        "approved_inputs": [
            item["input_id"] for item in manifest["inputs"]
        ],
        "evidence": {
            "central_bank": {
                "source": "isovector_macro_cb_view",
                "date": cb.get("date"),
                "bank_code": cb.get("bank_code"),
                "policy_tone_score": cb.get("policy_tone"),
                "uncertainty_score": cb.get("uncertainty"),
                "policy_tone_direction": direction(cb.get("policy_tone")),
                "uncertainty_direction": direction(cb.get("uncertainty"))
            },
            "pmi": {
                "source": "pmi_geoscen_zt_input",
                "date": pmi.get("date"),
                "pmi_pressure": pmi.get("pmi_geoscen_zt_input"),
                "labor_tag": pmi.get("tag_labor"),
                "inflation_tag": pmi.get("tag_inflation"),
                "demand_tag": pmi.get("tag_demand"),
                "pmi_pressure_direction": direction(pmi.get("pmi_geoscen_zt_input"))
            },
            "rates": {
                "source": "isovector_macro_cb_rates_join",
                "date": rates.get("date"),
                "bank_code": rates.get("bank_code"),
                "rates_alignment": rates.get("it_de_10y_spread_z"),
                "curve_spread": rates.get("it_de_10y_spread"),
                "rates_alignment_direction": direction(rates.get("it_de_10y_spread_z")),
                "curve_spread_direction": direction(rates.get("it_de_10y_spread"))
            }
        },
        "rbl_scaffold": {
            "macro_tags": [
                "policy",
                "rates",
                "pmi",
                "labor",
                "inflation",
                "demand"
            ],
            "compressed_observations": [
                "CB tone captured from approved IsoVector view",
                "PMI pressure captured from GeoScen input",
                "Rates alignment captured from IsoVector join"
            ],
 "semantic_controls": {
    "modality_required": True,
    "negation_required": True,
    "positive_wording_not_equal_positive_intent": True,
    "hknsl_mirror": True
},
"validation_hooks": {
    "requires_structure_validation": True,
    "requires_semantic_adjustor": True,
    "requires_event_monitor": True,
    "requires_no_forecast_check": True
}
        }
    }

    return packet


def main() -> None:
    manifest = load_manifest()
    inputs = read_approved_inputs(manifest)

    packet = build_qualayer_packet(manifest, inputs)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(packet, f, indent=2)

    print(f"QuaLayer packet written: {OUTPUT_PATH}")

def latest_non_null_row(df: pd.DataFrame, required_cols: list[str]) -> dict[str, Any]:
    clean = df.copy()

    if "date" in clean.columns:
        clean = clean.dropna(subset=["date"]).sort_values("date")

    for col in required_cols:
        if col not in clean.columns:
            return latest_row(df)

        clean = clean[clean[col].notna()]

    if clean.empty:
        return latest_row(df)

    return clean.tail(1).iloc[0].to_dict()

if __name__ == "__main__":
    main()

