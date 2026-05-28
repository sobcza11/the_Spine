from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "cb"
OUT_DIR.mkdir(parents=True, exist_ok=True)

HIERARCHY_PATH = OUT_DIR / "geoscen_multi_cb_hierarchy_v1.json"


POLICY_BEHAVIOR_PROFILES = {
    "FED": {
        "zone": "United States",
        "policy_behavior_model": "Labor / inflation / financial-conditions reaction function",
        "dominant_transmission": ["rates", "credit", "equities", "USD"],
        "cognition_note": "FED interpretation anchors global liquidity and risk appetite.",
    },
    "ECB": {
        "zone": "Euro Area",
        "policy_behavior_model": "Inflation credibility + sovereign fragmentation management",
        "dominant_transmission": ["sovereign spreads", "EUR", "banks", "periphery risk"],
        "cognition_note": "ECB interpretation must account for multi-country coordination and fragmentation pressure.",
    },
    "BOJ": {
        "zone": "Japan",
        "policy_behavior_model": "Deflation legacy + yield control + FX sensitivity",
        "dominant_transmission": ["JPY", "JGBs", "carry trade", "global duration"],
        "cognition_note": "BOJ interpretation is culturally and structurally shaped by deflation history and yield stability.",
    },
    "PBOC": {
        "zone": "China",
        "policy_behavior_model": "State-guided liquidity + growth stabilization + property-cycle sensitivity",
        "dominant_transmission": ["CNY", "credit impulse", "commodities", "EM risk"],
        "cognition_note": "PBOC interpretation should be read through managed liquidity, growth support, and state-directed credit channels.",
    },
    "BOE": {
        "zone": "United Kingdom",
        "policy_behavior_model": "Inflation credibility + housing sensitivity + imported inflation",
        "dominant_transmission": ["GBP", "gilts", "mortgage channel", "consumer pressure"],
        "cognition_note": "BOE interpretation is highly sensitive to inflation credibility, housing costs, and FX pass-through.",
    },
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, dict):
        obj["available"] = True
    return obj


def classify_cognition_state(policy_classification: str, available: bool) -> str:
    if not available:
        return "missing_source_tracked"

    text = str(policy_classification).lower()

    if "hawkish" in text and "uncertain" in text:
        return "restrictive_uncertain"
    if "hawkish" in text:
        return "restrictive"
    if "dovish" in text:
        return "supportive"
    if "uncertain" in text:
        return "uncertain_monitoring"

    return "neutral_monitoring"


def main() -> None:
    hierarchy_payload = read_json(HIERARCHY_PATH)

    hierarchy_rows = hierarchy_payload.get("hierarchy", [])
    hierarchy_by_cb = {
        row.get("bank_code"): row
        for row in hierarchy_rows
        if row.get("bank_code")
    }

    missing_cbs = set(
        hierarchy_payload
        .get("coverage", {})
        .get("missing_cbs", [])
    )

    cognition_rows = []

    for cb_code, profile in POLICY_BEHAVIOR_PROFILES.items():
        row = hierarchy_by_cb.get(cb_code, {})
        available = cb_code in hierarchy_by_cb

        cognition_rows.append({
            "bank_code": cb_code,
            "zone": profile["zone"],
            "available": available,
            "missing_source_tracked": cb_code in missing_cbs,
            "cb_weight": row.get("cb_weight"),
            "policy_classification": row.get("policy_classification", "Unavailable"),
            "policy_tone": row.get("policy_tone"),
            "uncertainty": row.get("uncertainty"),
            "policy_behavior_model": profile["policy_behavior_model"],
            "dominant_transmission": profile["dominant_transmission"],
            "cognition_state": classify_cognition_state(
                policy_classification=row.get("policy_classification", "Unavailable"),
                available=available,
            ),
            "cognition_note": profile["cognition_note"],
        })

    active_notes = [
        f"{row['bank_code']} ({row['zone']}): {row['cognition_state']} — {row['cognition_note']}"
        for row in cognition_rows
        if row["available"]
    ]

    missing_notes = [
        f"{row['bank_code']} ({row['zone']}): source missing but tracked for future weighted routing."
        for row in cognition_rows
        if not row["available"]
    ]

    payload = {
        "component": "GeoScen Cross-Country Policy Cognition",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(HIERARCHY_PATH),
        "active_cb_count": len([r for r in cognition_rows if r["available"]]),
        "tracked_cb_count": len(cognition_rows),
        "cognition_rows": cognition_rows,
        "interpretation_summary": " ".join(active_notes + missing_notes),
        "historical_drift_ready": True,
        "oraclechambers_ready": True,
        "governance": {
            "rules_based": True,
            "ai_last": True,
            "explainable": True,
            "cultural_policy_context": True,
            "full_country_macro_model": False,
            "lightweight_v1_before_historical_drift": True,
        },
    }

    out_json = OUT_DIR / "geoscen_cross_country_policy_cognition_v1.json"
    out_txt = OUT_DIR / "geoscen_cross_country_policy_cognition_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN CROSS-COUNTRY POLICY COGNITION V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"active_cb_count: {payload['active_cb_count']}\n")
        f.write(f"tracked_cb_count: {payload['tracked_cb_count']}\n")
        f.write(f"historical_drift_ready: {payload['historical_drift_ready']}\n")
        f.write(f"oraclechambers_ready: {payload['oraclechambers_ready']}\n\n")

        f.write("POLICY COGNITION ROWS\n")
        f.write("-" * 60 + "\n")
        for row in cognition_rows:
            f.write(
                f"- {row['bank_code']} | "
                f"{row['zone']} | "
                f"available={row['available']} | "
                f"state={row['cognition_state']} | "
                f"model={row['policy_behavior_model']}\n"
            )

        f.write("\nINTERPRETATION SUMMARY\n")
        f.write("-" * 60 + "\n")
        f.write(payload["interpretation_summary"] + "\n")

    print("OK | GeoScen Cross-Country Policy Cognition v1 built")
    print(f"active_cb_count  : {payload['active_cb_count']}")
    print(f"tracked_cb_count : {payload['tracked_cb_count']}")
    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()
    