from pathlib import Path
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup


MONTH_URLS = {
    "2026-01": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/january/",
    "2026-02": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/february/",
    "2026-03": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/march/",
    "2026-04": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/april/",
}


COMPONENT_MAP = {
    "NEW ORDERS": "_no",
    "PRODUCTION": "_pro",
    "EMPLOYMENT": "_emp",
    "SUPPLIER DELIVERIES": "_sd",
    "INVENTORIES": "_inv",
    "CUSTOMERS' INVENTORIES": "_cin",
    "PRICES": "_p",
    "BACKLOG OF ORDERS": "_bkl",
    "NEW EXPORT ORDERS": "_exp",
    "IMPORTS": "_imp",
    "BUYING POLICY": "_bp",
}


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
    )
}


INDUSTRY_NORMALIZATION = {
    "Apparel, Leather & Allied Products": "Apparel, Leather & Allied Products",
    "Chemical Products": "Chemical Products",
    "Computer & Electronic Products": "Computer & Electronic Products",
    "Electrical Equipment, Appliances & Components": "Electrical Equipment, Appliances & Components",
    "Fabricated Metal Products": "Fabricated Metal Products",
    "Food, Beverage & Tobacco Products": "Food, Beverage & Tobacco Products",
    "Furniture & Related Products": "Furniture & Related Products",
    "Machinery": "Machinery",
    "Miscellaneous Manufacturing": "Miscellaneous Manufacturing",
    "Nonmetallic Mineral Products": "Nonmetallic Mineral Products",
    "Paper Products": "Paper Products",
    "Petroleum & Coal Products": "Petroleum & Coal Products",
    "Plastics & Rubber Products": "Plastics & Rubber Products",
    "Primary Metals": "Primary Metals",
    "Printing & Related Support Activities": "Printing & Related Support Activities",
    "Textile Mills": "Textile Mills",
    "Transportation Equipment": "Transportation Equipment",
    "Wood Products": "Wood Products",
}


ALL_INDUSTRIES = list(INDUSTRY_NORMALIZATION.values())


POSITIVE_WORDS = [
    "higher",
    "better",
    "faster",
    "growing",
    "increase",
    "increased",
    "too low",
]

NEGATIVE_WORDS = [
    "lower",
    "worse",
    "slower",
    "decreasing",
    "decrease",
    "decreased",
    "too high",
]


MONTH_LABELS = {
    "2026-01": "Jan-26",
    "2026-02": "Feb-26",
    "2026-03": "Mar-26",
    "2026-04": "Apr-26",
}


SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def normalize_text(x: str) -> str:
    return re.sub(r"\s+", " ", x).strip()


def clean_industry_name(x: str) -> str:
    x = normalize_text(x)
    x = x.replace(";", "")
    return INDUSTRY_NORMALIZATION.get(x, x)


def score_sentence(sentence: str) -> int:
    s = sentence.lower()

    pos = sum(w in s for w in POSITIVE_WORDS)
    neg = sum(w in s for w in NEGATIVE_WORDS)

    return pos - neg


def extract_component_block(text: str, component_name: str):
    pattern = rf"{re.escape(component_name)}(.*?)(?:\n[A-Z][A-Z\s\'&/-]+\n|$)"

    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)

    if not match:
        return ""

    return match.group(1)


def extract_industries(block_text: str):
    industries = {}

    sentences = re.split(r"(?<=[.!?])\s+", block_text)

    for sentence in sentences:
        for industry in ALL_INDUSTRIES:
            if industry.lower() in sentence.lower():
                industries[industry] = score_sentence(sentence)

    return industries


rows = []

for month_key, url in MONTH_URLS.items():
    print(f"reading: {month_key} | {url}")

    response = SESSION.get(url, timeout=60)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    text = soup.get_text("\n")
    text = normalize_text(text)

    row = {"date": MONTH_LABELS[month_key]}

    for component_name, suffix in COMPONENT_MAP.items():
        block = extract_component_block(text, component_name)

        industries = extract_industries(block)

        for industry in ALL_INDUSTRIES:
            col = f"{industry}{suffix}"
            row[col] = industries.get(industry, 0)

    rows.append(row)


df = pd.DataFrame(rows)

cols = ["date"] + sorted([c for c in df.columns if c != "date"])
df = df[cols]


repo_root = Path.cwd()

out_dir = repo_root / "data" / "ism"
out_dir.mkdir(parents=True, exist_ok=True)

out_path = out_dir / "ism_manufacturing_components_2026_v1.xlsx"

with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="manufacturing", index=False)


print("OK | ISM manufacturing components v1")
print(f"output: {out_path}")
print(df.head().to_string(index=False))

