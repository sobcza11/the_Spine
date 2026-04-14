from pathlib import Path
import pandas as pd

OUTPUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\spine_us\mappings"
)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = OUTPUT_DIR / "etf_pmi_mapping.csv"

ETF_PMI_MAPPING = {
    "manu": {
        "XLB": {
            "Chemical Products": 1.00,
            "Nonmetallic Mineral Products": 1.00,
            "Paper Products": 1.00,
            "Plastics & Rubber Products": 0.70,
            "Primary Metals": 1.00,
            "Wood Products": 0.70,
            "Fabricated Metal Products": 0.30,
        },
        "XLI": {
            "Machinery": 1.00,
            "Transportation Equipment": 1.00,
            "Electrical Equipment, Appliances & Components": 0.70,
            "Fabricated Metal Products": 0.70,
            "Miscellaneous Manufacturing": 0.60,
            "Printing & Related Support Activities": 0.40,
            "Plastics & Rubber Products": 0.30,
        },
        "XLK": {
            "Computer & Electronic Products": 1.00,
        },
        "XLY": {
            "Apparel, Leather & Allied Products": 1.00,
            "Furniture & Related Products": 1.00,
            "Textile Mills": 0.70,
            "Electrical Equipment, Appliances & Components": 0.30,
            "Miscellaneous Manufacturing": 0.40,
            "Wood Products": 0.30,
        },
        "XLP": {
            "Food, Beverage & Tobacco Products": 1.00,
            "Textile Mills": 0.30,
        },
        "XLE": {
            "Petroleum & Coal Products": 1.00,
        },
        "XLC": {
            "Printing & Related Support Activities": 0.60,
        },
    },
    "nonmanu": {
        "XLY": {
            "Accommodation & Food Services": 0.70,
            "Arts, Entertainment & Recreation": 0.70,
            "Retail Trade": 0.70,
            "Other Services": 0.50,
            "Wholesale Trade": 0.40,
            "Educational Services": 0.50,
        },
        "XLP": {
            "Accommodation & Food Services": 0.30,
            "Retail Trade": 0.30,
            "Other Services": 0.50,
        },
        "XLF": {
            "Finance & Insurance": 1.00,
            "Management of Companies & Support Services": 0.50,
        },
        "XLV": {
            "Health Care & Social Assistance": 1.00,
        },
        "XLK": {
            "Information": 0.60,
            "Professional, Scientific & Technical Services": 0.60,
        },
        "XLC": {
            "Information": 0.40,
            "Arts, Entertainment & Recreation": 0.30,
            "Educational Services": 0.50,
        },
        "XLI": {
            "Transportation & Warehousing": 1.00,
            "Management of Companies & Support Services": 0.50,
            "Professional, Scientific & Technical Services": 0.40,
            "Public Administration": 0.50,
            "Wholesale Trade": 0.60,
        },
        "XLB": {
            "Agriculture, Forestry, Fishing & Hunting": 1.00,
            "Construction": 0.60,
            "Mining": 0.70,
        },
        "XLE": {
            "Mining": 0.30,
        },
        "XLRE": {
            "Real Estate, Rental & Leasing": 1.00,
            "Construction": 0.40,
        },
        "XLU": {
            "Utilities": 1.00,
            "Public Administration": 0.50,
        },
    },
}

rows = []

for pmi_type, etf_map in ETF_PMI_MAPPING.items():
    for etf_symbol, industry_map in etf_map.items():
        for industry_name, weight in industry_map.items():
            rows.append(
                {
                    "pmi_type": pmi_type,
                    "industry_name": industry_name,
                    "etf_symbol": etf_symbol,
                    "weight": float(weight),
                    "active_flag": 1,
                }
            )

df = pd.DataFrame(rows).sort_values(
    ["pmi_type", "etf_symbol", "industry_name"]
).reset_index(drop=True)

df.to_csv(OUTPUT_PATH, index=False)

print(f"saved: {OUTPUT_PATH}")
print(df.head(20).to_string(index=False))
print("\nrows:", len(df))
print("etfs:", sorted(df["etf_symbol"].unique().tolist()))
print("pmi_types:", sorted(df["pmi_type"].unique().tolist()))
