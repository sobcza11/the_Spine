"""
analytics/merge_metadata_into_panel.py

Adds Sector and Industry from df_zacks.csv into the panel file:
    panel_prices_dividends.parquet

Outputs:
    panel_prices_dividends_with_meta.parquet
"""

import pandas as pd

UNIVERSE_FILE = "df_zacks.csv"
PANEL_FILE = "panel_prices_dividends.parquet"
OUTPUT_PANEL = "panel_prices_dividends_with_meta.parquet"

def main():

    print("[STEP] Loading universe metadata df_zacks.csv")
    meta = pd.read_csv(UNIVERSE_FILE)

    # Normalize ticker column name
    meta['Ticker'] = meta['Ticker'].astype(str).str.strip()

    print("[STEP] Loading panel parquet")
    panel = pd.read_parquet(PANEL_FILE)

    # Merge metadata onto panel
    print("[STEP] Merging Sector & Industry onto panel")
    merged = panel.merge(
        meta[['Ticker','Sector','Industry']],
        left_on='ticker',
        right_on='Ticker',
        how='left'
    )

    # Cleanup
    merged = merged.drop(columns=['Ticker'])

    print("[STEP] Saving...")
    merged.to_parquet(OUTPUT_PANEL, index=False)

    print(f"[DONE] Created {OUTPUT_PANEL}")

if __name__ == "__main__":
    main()

