from pathlib import Path
import pandas as pd


PRIORITY_MARKETS = {

    # =====================================================
    # EQUITIES
    # =====================================================

    "ES": [
        "S&P 500",
        "E-MINI S&P",
    ],

    "NQ": [
        "NASDAQ",
        "E-MINI NASDAQ",
    ],

    "RTY": [
        "RUSSELL",
    ],

    # =====================================================
    # RATES / FX
    # =====================================================

    "USD": [
        "U.S. DOLLAR INDEX",
    ],

    "EUR": [
        "EURO FX",
    ],

    "JPY": [
        "JAPANESE YEN",
    ],

    "GBP": [
        "BRITISH POUND",
        "POUND STERLING",
    ],

    "CAD": [
        "CANADIAN DOLLAR",
    ],

    "AUD": [
        "AUSTRALIAN DOLLAR",
    ],

    "CHF": [
        "SWISS FRANC",
    ],

    "US10Y": [
        "10-YEAR U.S. TREASURY",
    ],

    "SOFR": [
        "SOFR",
    ],

    # =====================================================
    # ENERGY
    # =====================================================

    "CL": [
        "WTI FINANCIAL CRUDE OIL",
        "CRUDE OIL, LIGHT SWEET-WTI",
        "WTI-PHYSICAL",
    ],

    "NG": [
        "NAT GAS NYME",
        "HENRY HUB PENULTIMATE NAT GAS",
        "NAT GAS ICE",
    ],

    # =====================================================
    # METALS
    # =====================================================

    "GC": [
        "GOLD - COMMODITY EXCHANGE INC.",
        "MICRO GOLD",
    ],

    "SI": [
        "SILVER - COMMODITY EXCHANGE INC.",
    ],

    "HG": [
        "COPPER- #1 - COMMODITY EXCHANGE INC.",
    ],

    # =====================================================
    # CRYPTO
    # =====================================================

    "BTC": [
        "BITCOIN",
        "MICRO BITCOIN",
    ],

    "ETH": [
        "ETHER",
        "ETHEREUM",
    ],
}


def find_col(df, candidates):

    lower_map = {
        c.lower(): c
        for c in df.columns
    }

    for candidate in candidates:

        for lower_name, original_name in lower_map.items():

            if candidate.lower() in lower_name:
                return original_name

    return None


def assign_instrument(market_name: str):

    if pd.isna(market_name):
        return None

    market_upper = str(market_name).upper()

    for instrument, patterns in PRIORITY_MARKETS.items():

        for pattern in patterns:

            if pattern.upper() in market_upper:
                return instrument

    return None


def build_cot_instrument_panels_v1():

    repo_root = Path.cwd()

    raw_dir = (
        repo_root
        / "data"
        / "cot"
        / "raw_cftc"
    )

    live_input_path = (
        raw_dir
        / "cftc_raw_combined_live_v1.parquet"
    )

    historical_input_path = (
        raw_dir
        / "cftc_raw_combined_v1.parquet"
    )

    if live_input_path.exists():

        input_path = live_input_path
        input_source = "live_combined"

    elif historical_input_path.exists():

        input_path = historical_input_path
        input_source = "historical_combined"

    else:
        raise FileNotFoundError(
            f"No CFTC raw file found. Checked: {live_input_path} and {historical_input_path}"
        )

    out_dir = (
        repo_root
        / "data"
        / "cot"
        / "panels"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    df = pd.read_parquet(input_path).copy()

    market_col = find_col(
        df,
        [
            "market_and_exchange_names",
            "market",
            "contract_market_name",
        ],
    )

    date_col = find_col(
        df,
        [
            "report_date_as_yyyy_mm_dd",
            "report_date",
            "as_of_date",
        ],
    )

    if market_col is None:
        raise KeyError(
            f"Could not find market column. Columns: {list(df.columns)}"
        )

    if date_col is None:
        raise KeyError(
            f"Could not find date column. Columns: {list(df.columns)}"
        )

    df["date"] = pd.to_datetime(
        df[date_col],
        errors="coerce",
    )

    df["market_name"] = (
        df[market_col]
        .astype(str)
    )

    df["instrument"] = (
        df["market_name"]
        .apply(assign_instrument)
    )

    df_panel = (
        df[df["instrument"].notna()]
        .sort_values(["instrument", "date"])
        .reset_index(drop=True)
        .copy()
    )

    df_panel["input_source"] = input_source

    parquet_path = (
        out_dir
        / "cot_instrument_panel_v1.parquet"
    )

    json_path = (
        out_dir
        / "cot_instrument_panel_v1.json"
    )

    df_panel.to_parquet(
        parquet_path,
        index=False,
    )

    df_panel.head(1000).to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    print("COT instrument panel complete")
    print("Input Source:", input_source)
    print("Input Path:", input_path)
    print("Rows:", len(df_panel))
    print("Date Min:", df_panel["date"].min())
    print("Date Max:", df_panel["date"].max())

    print(
        "Instruments:",
        sorted(
            df_panel["instrument"]
            .dropna()
            .unique()
            .tolist()
        )
    )

    print("")
    print("INSTRUMENT COUNTS:")
    print(
        df_panel["instrument"]
        .value_counts()
        .sort_index()
    )

    print("")
    print("PARQUET:", parquet_path)
    print("JSON SAMPLE:", json_path)

    return df_panel


if __name__ == "__main__":
    build_cot_instrument_panels_v1()
