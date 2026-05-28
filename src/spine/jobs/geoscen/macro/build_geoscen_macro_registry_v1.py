from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "macro"
OUT_DIR.mkdir(parents=True, exist_ok=True)


MACRO_REGISTRY = [
    ("CPI YoY", "Inflation", "FRED", "CPIAUCSL", "Monthly", "Mid-month", "Yes", "fred"),
    ("Core CPI YoY", "Inflation", "FRED", "CPILFESL", "Monthly", "Mid-month", "Yes", "fred"),
    ("PCE YoY", "Inflation", "FRED", "PCEPI", "Monthly", "End-month", "Yes", "fred"),
    ("Core PCE YoY", "Inflation", "FRED", "PCEPILFE", "Monthly", "End-month", "Yes", "fred"),
    ("WTI Pressure", "Inflation", "Existing GeoScen WTI serving layer", "wti_inflation_pressure", "Daily / Intraday", "Market close", "Mixed", "existing_serving"),

    ("GDP QoQ / YoY", "Growth", "FRED", "GDPC1", "Quarterly", "Quarterly BEA release", "Yes", "fred"),
    ("Industrial Production", "Growth", "FRED", "INDPRO", "Monthly", "Mid-month", "Yes", "fred"),
    ("Retail Sales", "Growth", "FRED", "RSAFS", "Monthly", "Mid-month", "Yes", "fred"),
    ("ISM Manufacturing PMI", "Growth", "Existing ISM qualitative layer", "ism_manufacturing_qual", "Monthly", "1st business day", "No", "existing_serving"),
    ("ISM Services PMI", "Growth", "Existing ISM qualitative layer", "ism_services_qual", "Monthly", "3rd business day", "No", "existing_serving"),

    ("M2 Money Supply", "Liquidity", "FRED", "M2SL", "Weekly / Monthly", "Weekly", "Yes", "fred"),
    ("M2 Velocity", "Liquidity", "FRED", "M2V", "Quarterly", "Quarterly", "Yes", "fred"),
    ("Fed Balance Sheet", "Liquidity", "FRED", "WALCL", "Weekly", "Thursday", "Yes", "fred"),
    ("Excess Liquidity Proxy", "Liquidity", "Derived internal factor", "excess_liquidity_proxy", "Daily", "Market close", "Internal", "derived"),

    ("NFP", "Labor", "FRED", "PAYEMS", "Monthly", "First Friday", "Yes", "fred"),
    ("Unemployment Rate", "Labor", "FRED", "UNRATE", "Monthly", "First Friday", "Yes", "fred"),
    ("Initial Claims", "Labor", "FRED", "ICSA", "Weekly", "Thursday", "Yes", "fred"),
    ("Continuing Claims", "Labor", "FRED", "CCSA", "Weekly", "Thursday", "Yes", "fred"),
    ("Labor Force Participation", "Labor", "FRED", "CIVPART", "Monthly", "First Friday", "Yes", "fred"),
    ("Average Hourly Earnings", "Labor", "FRED", "CES0500000003", "Monthly", "First Friday", "Yes", "fred"),

    ("US 10Y Yield", "Rates", "Existing Rates Zₜ layer", "rates_zt_latest", "Daily / Intraday", "Market close", "Mixed", "existing_serving"),
    ("Fed Funds Rate", "Rates", "FRED", "FEDFUNDS", "Monthly", "Monthly", "Yes", "fred"),
    ("2s10s Curve", "Rates", "Derived internal spread", "twos_tens_curve", "Daily", "Market close", "Internal", "derived"),
    ("Credit Spreads", "Rates / Credit", "FRED", "BAMLH0A0HYM2", "Daily", "Market close", "Yes", "fred"),

    ("Consumer Sentiment", "Consumer", "University of Michigan / FRED", "UMCSENT", "Monthly", "Mid & end-month", "Yes", "fred"),
    ("Conference Board Confidence", "Consumer", "Conference Board", "conference_board_confidence", "Monthly", "End-month", "No", "premium_external"),
    ("Real Disposable Income", "Consumer", "FRED", "DSPIC96", "Monthly", "End-month", "Yes", "fred"),
    ("Personal Savings Rate", "Consumer", "FRED", "PSAVERT", "Monthly", "End-month", "Yes", "fred"),

    ("Capacity Utilization", "Industrial", "FRED", "TCU", "Monthly", "Mid-month", "Yes", "fred"),
    ("Manufacturing Utilization", "Industrial", "FRED", "MCUMFN", "Monthly", "Mid-month", "Yes", "fred"),
    ("Durable Goods Orders", "Industrial", "FRED", "DGORDER", "Monthly", "Late-month", "Yes", "fred"),

    ("Housing Starts", "Housing", "FRED", "HOUST", "Monthly", "Mid-month", "Yes", "fred"),
    ("Building Permits", "Housing", "FRED", "PERMIT", "Monthly", "Mid-month", "Yes", "fred"),
    ("Existing Home Sales", "Housing", "FRED / NAR", "existing_home_sales", "Monthly", "Late-month", "Partial", "partial_external"),

    ("USD Index", "FX", "Existing FX layer", "usd_index", "Intraday / Daily", "Market close", "Mixed", "existing_serving"),
    ("EUR/USD", "FX", "Existing FX layer", "eur_usd", "Intraday / Daily", "Market close", "Mixed", "existing_serving"),
    ("USD/JPY", "FX", "Existing FX layer", "usd_jpy", "Intraday / Daily", "Market close", "Mixed", "existing_serving"),

    ("Crude Oil", "Commodities", "Existing WTI layer", "wti_price", "Intraday / Daily", "Market close", "Mixed", "existing_serving"),
    ("Gold", "Commodities", "FRED", "GOLDAMGBD228NLBM", "Daily", "Market close", "Yes", "fred"),

    ("Financial Conditions", "Cross-Asset", "Existing GeoScen / Rates fusion", "financial_conditions", "Daily", "Market close", "Internal", "existing_serving"),
    ("Breadth Factor", "Equities", "Existing breadth_factor_serving_v1", "breadth_factor_serving_v1", "Daily", "Market close", "Internal", "existing_serving"),
    ("C_FLOW", "Capital Flow", "Existing C_FLOW v5", "c_flow_latest_v5", "Daily", "Market close", "Internal", "existing_serving"),
    ("Contradiction Score", "Cognition", "Existing contradiction engine", "geoscen_contradiction_engine_v1", "Daily / Event-driven", "GeoScen runtime", "Internal", "existing_serving"),
    ("Narrative Drift Score", "Cognition", "Existing drift engine", "geoscen_historical_narrative_drift_engine_v1", "Daily / Monthly hybrid", "GeoScen runtime", "Internal", "existing_serving"),
    ("Cross-Country Policy Cognition", "Cognition", "Existing CB cognition layer", "geoscen_cross_country_policy_cognition_v1", "Event-driven", "CB event cycle", "Internal", "existing_serving"),
]


def main() -> None:
    df = pd.DataFrame(
        MACRO_REGISTRY,
        columns=[
            "name",
            "section",
            "source",
            "source_key",
            "update_frequency",
            "last_typical_update",
            "free_access",
            "ingestion_route",
        ],
    )

    df["is_free"] = df["free_access"].eq("Yes")
    df["is_internal"] = df["free_access"].eq("Internal")
    df["is_existing_serving"] = df["ingestion_route"].eq("existing_serving")
    df["requires_external_subscription"] = df["free_access"].isin(["No", "Partial"])
    df["geoscen_ready"] = df["ingestion_route"].isin(["fred", "existing_serving", "derived"])

    payload = {
        "component": "GeoScen Macro Registry",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "signal_count": int(len(df)),
        "free_signal_count": int(df["is_free"].sum()),
        "internal_signal_count": int(df["is_internal"].sum()),
        "existing_serving_count": int(df["is_existing_serving"].sum()),
        "subscription_required_count": int(df["requires_external_subscription"].sum()),
        "geoscen_ready_count": int(df["geoscen_ready"].sum()),
        "sections": sorted(df["section"].unique().tolist()),
        "governance": {
            "registry_first": True,
            "ai_last": True,
            "source_provenance_required": True,
            "release_calendar_aware": True,
            "serving_layer_ready": True,
        },
    }

    out_json = OUT_DIR / "geoscen_macro_registry_v1.json"
    out_txt = OUT_DIR / "geoscen_macro_registry_v1.txt"
    out_csv = OUT_DIR / "geoscen_macro_registry_v1.csv"
    out_parquet = OUT_DIR / "geoscen_macro_registry_v1.parquet"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    df.to_csv(out_csv, index=False)
    df.to_parquet(out_parquet, index=False)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN MACRO REGISTRY V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"signal_count: {payload['signal_count']}\n")
        f.write(f"free_signal_count: {payload['free_signal_count']}\n")
        f.write(f"internal_signal_count: {payload['internal_signal_count']}\n")
        f.write(f"existing_serving_count: {payload['existing_serving_count']}\n")
        f.write(f"subscription_required_count: {payload['subscription_required_count']}\n")
        f.write(f"geoscen_ready_count: {payload['geoscen_ready_count']}\n\n")

        f.write("REGISTRY ROWS\n")
        f.write("-" * 60 + "\n")
        for row in df.to_dict("records"):
            f.write(
                f"- {row['name']} | "
                f"{row['section']} | "
                f"{row['source_key']} | "
                f"{row['update_frequency']} | "
                f"free={row['free_access']} | "
                f"route={row['ingestion_route']}\n"
            )

    print("OK | GeoScen Macro Registry v1 built")
    print(f"signal_count                  : {payload['signal_count']}")
    print(f"free_signal_count             : {payload['free_signal_count']}")
    print(f"existing_serving_count        : {payload['existing_serving_count']}")
    print(f"subscription_required_count   : {payload['subscription_required_count']}")
    print(f"geoscen_ready_count           : {payload['geoscen_ready_count']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)
    print(out_csv)
    print(out_parquet)


if __name__ == "__main__":
    main()

    