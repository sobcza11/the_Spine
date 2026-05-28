from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class ApprovedSource:
    name: str
    category: str
    approved: bool
    role: str


APPROVED_SOURCES: list[ApprovedSource] = [
    ApprovedSource("Tiingo", "market_data", True, "equities_fx_prices"),
    ApprovedSource("FRED", "macro_data", True, "macro_rates_credit"),
    ApprovedSource("Treasury", "rates_data", True, "yield_curve"),
    ApprovedSource("Polygon", "market_data", True, "market_prices"),
    ApprovedSource("EIA", "energy_data", True, "commodities_energy"),
    ApprovedSource("WRDS", "institutional_data", True, "research_validation"),
    ApprovedSource("NBIS", "rates_data", True, "china_rates_policy_proxy"),
]


BANNED_SOURCES: list[str] = [
    "Yahoo Finance",
]


def build_approved_sources_contract_v1() -> dict[str, Any]:
    return {
        "artifact": "oc_approved_sources_contract_v1",
        "layer": "OracleChambers Online Data Source Governance",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "online_runtime_ready": False,
        "approved_sources": [source.__dict__ for source in APPROVED_SOURCES],
        "banned_sources": BANNED_SOURCES,
        "governance_rule": (
            "Online data integration begins only after frontend stabilization "
            "and must preserve deterministic routing, provenance, and source "
            "governance."
        ),
    }


if __name__ == "__main__":
    output = build_approved_sources_contract_v1()

    for key, value in output.items():
        print(f"{key}: {value}")

        