from pathlib import Path
import pandas as pd


SEARCH_TERMS = [
    "CRUDE",
    "OIL",
    "WTI",
    "NATURAL GAS",
    "NAT GAS",
    "GOLD",
    "SILVER",
    "COPPER",
    "COMEX",
    "NYMEX",
]


def find_col(df, candidates):
    lower_map = {c.lower(): c for c in df.columns}

    for candidate in candidates:
        for lower_name, original_name in lower_map.items():
            if candidate.lower() in lower_name:
                return original_name

    return None


def inspect_cot_commodity_market_names_v1():
    repo_root = Path.cwd()

    input_path = (
        repo_root
        / "data"
        / "cot"
        / "raw_cftc"
        / "cftc_raw_combined_live_v1.parquet"
    )

    out_dir = (
        repo_root
        / "data"
        / "cot"
        / "diagnostics"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    market_col = find_col(
        df,
        [
            "market_and_exchange_names",
            "market",
            "contract_market_name",
        ],
    )

    if market_col is None:
        raise KeyError(f"Could not find market column. Columns: {list(df.columns)}")

    df["market_name"] = df[market_col].astype(str)

    unique_markets = (
        df[["market_name"]]
        .drop_duplicates()
        .sort_values("market_name")
        .reset_index(drop=True)
    )

    matches = []

    for term in SEARCH_TERMS:
        subset = unique_markets[
            unique_markets["market_name"]
            .str.upper()
            .str.contains(term, na=False)
        ].copy()

        subset["search_term"] = term

        matches.append(subset)

    if matches:
        matched_df = (
            pd.concat(matches, ignore_index=True)
            .drop_duplicates()
            .sort_values(["search_term", "market_name"])
            .reset_index(drop=True)
        )
    else:
        matched_df = pd.DataFrame(columns=["market_name", "search_term"])

    all_markets_path = out_dir / "cot_all_market_names_v1.csv"
    matched_path = out_dir / "cot_commodity_market_name_matches_v1.csv"
    matched_json = out_dir / "cot_commodity_market_name_matches_v1.json"

    unique_markets.to_csv(all_markets_path, index=False)
    matched_df.to_csv(matched_path, index=False)
    matched_df.to_json(
        matched_json,
        orient="records",
        indent=2,
    )

    print("COT commodity market-name inspection complete")
    print("Market column:", market_col)
    print("Unique markets:", len(unique_markets))
    print("Matches:", len(matched_df))
    print("ALL MARKETS CSV:", all_markets_path)
    print("MATCHED CSV:", matched_path)
    print("MATCHED JSON:", matched_json)

    print("")
    print("MATCHED MARKET NAMES:")
    print(matched_df.to_string(index=False))

    return matched_df


if __name__ == "__main__":
    inspect_cot_commodity_market_names_v1()
