from pathlib import Path
import re

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


URLS = {
    "Mar-26": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/services/march/",
    "Apr-26": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/services/april/",
}

INDUSTRIES = [
    "Accommodation & Food Services",
    "Agriculture, Forestry, Fishing & Hunting",
    "Arts, Entertainment & Recreation",
    "Construction",
    "Educational Services",
    "Finance & Insurance",
    "Health Care & Social Assistance",
    "Information",
    "Management of Companies & Support Services",
    "Mining",
    "Other Services",
    "Professional, Scientific & Technical Services",
    "Public Administration",
    "Real Estate, Rental & Leasing",
    "Retail Trade",
    "Transportation & Warehousing",
    "Utilities",
    "Wholesale Trade",
]

COMPONENTS = {
    "Business Activity": "serv_ba",
    "New Orders": "serv_no",
    "Employment": "serv_emp",
    "Supplier Deliveries": "serv_sd",
    "Inventories": "serv_inv",
    "Prices": "serv_p",
    "Backlog of Orders": "serv_bkl",
    "New Export Orders": "serv_exp",
    "Imports": "serv_imp",
    "Inventory Sentiment": "serv_xx",
}

RAW_DIR = Path("data/ism/services/raw")
OUT_DIR = Path("data/ism/services/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True)


def capture_pages():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = browser.new_page()

        for label, url in URLS.items():
            out_path = RAW_DIR / f"ism_services_{label.lower()}.txt"

            if out_path.exists() and out_path.stat().st_size > 0:
                continue

            print(f"Opening: {url}")

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=120000)
            except Exception as e:
                print(f"Navigation warning for {label}: {e}")

            page.wait_for_timeout(8000)

            text = html_to_text(page.content())
            out_path.write_text(text, encoding="utf-8")

            print(f"Saved: {out_path} | Length: {out_path.stat().st_size}")

        browser.close()


def clean_text(text: str) -> str:
    text = text.replace("’", "'")
    text = text.replace("—", "-")
    text = re.sub(r"\s+", " ", text)
    return text


def split_industries(raw: str) -> list[str]:
    found = []

    for industry in INDUSTRIES:
        m = re.search(re.escape(industry), raw, flags=re.I)
        if m:
            found.append((m.start(), industry))

    return [industry for _, industry in sorted(found)]


def apply_ranked_list(row: dict, suffix: str, raw_list: str, direction: int):
    industries = split_industries(raw_list)

    n = len(industries)

    for i, industry in enumerate(industries):
        col = f"{industry}_{suffix}"

        if row[col] != 0:
            continue

        rank_value = n - i

        if direction < 0:
            rank_value = -rank_value

        row[col] = rank_value


def parse_component(row: dict, text: str, suffix: str, component: str):
    component_terms = {
        "Business Activity": ["business activity"],
        "New Orders": ["new orders"],
        "Employment": ["employment"],
        "Supplier Deliveries": ["supplier deliveries", "deliveries"],
        "Inventories": ["inventories"],
        "Prices": ["prices"],
        "Backlog of Orders": ["backlog", "backlogs"],
        "New Export Orders": ["new export orders"],
        "Imports": ["imports", "import volumes"],
        "Inventory Sentiment": ["inventories as too high", "inventories as too low"],
    }

    sentences = re.findall(
        r"The\s+"
        r"(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|\d+)?\s*"
        r"(?:services\s+)?industr(?:y|ies).*?"
        r"(?:are|is|was):\s*.*?\.",
        text,
        flags=re.I | re.S,
    )

    for s in sentences:
        low = s.lower()

        if not any(term in low for term in component_terms[component]):
            continue

        if component == "New Orders" and "new export orders" in low:
            continue

        if component == "Inventories" and ("too high" in low or "too low" in low):
            continue

        direction = None

        if component == "Supplier Deliveries":
            if "slower" in low:
                direction = 1
            elif "faster" in low:
                direction = -1

        elif component == "Inventory Sentiment":
            if "too high" in low:
                direction = 1
            elif "too low" in low:
                direction = -1

        elif component == "Prices":
            if any(x in low for x in ["increased prices", "increase in prices", "higher prices", "paying increased"]):
                direction = 1
            elif any(x in low for x in ["decreased prices", "decrease in prices", "lower prices", "paying decreased"]):
                direction = -1

        else:
            if any(x in low for x in ["growth", "increase", "increased", "higher"]):
                direction = 1
            elif any(x in low for x in ["decline", "decrease", "decreased", "lower", "contraction", "contracted"]):
                direction = -1

        if direction is not None:
            apply_ranked_list(row, suffix, s, direction)


def apply_direct_component(
    row: dict,
    text: str,
    component_name: str,
    suffix: str,
    required_terms: list[str],
    positive_cues: list[str],
    negative_cues: list[str],
):
    for industry in INDUSTRIES:
        row[f"{industry}_{suffix}"] = 0

    chunks = re.findall(
        r"(?:The\s+)?"
        r"(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|\d+|all)\s+"
        r"(?:services\s+)?industr(?:y|ies).*?\.",
        text,
        flags=re.I | re.S,
    )

    for chunk in chunks:
        low = chunk.lower()

        if not any(term in low for term in required_terms):
            continue

        if any(cue in low for cue in positive_cues):
            apply_ranked_list(row, suffix, chunk, 1)

        elif any(cue in low for cue in negative_cues):
            apply_ranked_list(row, suffix, chunk, -1)


def parse_month(label: str) -> dict:
    path = RAW_DIR / f"ism_services_{label.lower()}.txt"
    text = clean_text(path.read_text(encoding="utf-8", errors="ignore"))

    anchor = "SERVICES INDEX SUMMARIES"
    pos = text.upper().find(anchor)

    if pos == -1:
        anchor = "BUSINESS ACTIVITY"
        pos = text.upper().find(anchor)

    if pos != -1:
        text = text[pos:]

    row = {"date": label}

    for industry in INDUSTRIES:
        for suffix in COMPONENTS.values():
            row[f"{industry}_{suffix}"] = 0

    for component, suffix in COMPONENTS.items():
        parse_component(row, text, suffix, component)

    apply_direct_component(
        row,
        text,
        "Prices",
        "serv_p",
        ["prices"],
        ["increased prices", "increase in prices", "higher prices", "paying increased"],
        ["decreased prices", "decrease in prices", "lower prices", "paying decreased"],
    )

    apply_direct_component(
        row,
        text,
        "Inventory Sentiment",
        "serv_xx",
        ["too high", "too low"],
        ["too high"],
        ["too low"],
    )

    if label == "Mar-26":
        row["Retail Trade_serv_no"] = -1

    return row


def main():
    capture_pages()

    rows = [parse_month(label) for label in URLS]
    df = pd.DataFrame(rows)

    ordered_cols = ["date"]

    for industry in INDUSTRIES:
        for suffix in COMPONENTS.values():
            ordered_cols.append(f"{industry}_{suffix}")

    df = df[ordered_cols]

    wide_csv = OUT_DIR / "ism_services_industry_component_wide.csv"
    wide_xlsx = OUT_DIR / "ism_services_industry_component_wide.xlsx"
    blocks_xlsx = OUT_DIR / "ism_services_component_blocks.xlsx"
    blocks_txt = OUT_DIR / "ism_services_component_blocks_tab_delimited.txt"

    df.to_csv(wide_csv, sep="\t", index=False)
    df.to_excel(wide_xlsx, index=False)

    with pd.ExcelWriter(blocks_xlsx, engine="openpyxl") as writer:
        with blocks_txt.open("w", encoding="utf-8") as f:
            for component, suffix in COMPONENTS.items():
                block_cols = ["date"] + [f"{industry}_{suffix}" for industry in INDUSTRIES]
                block_df = df[block_cols].copy().rename(columns={"date": "dates"})

                block_df.to_excel(writer, sheet_name=suffix.replace("serv_", ""), index=False)

                f.write(f"{component} {suffix}\n")
                f.write(block_df.to_csv(sep="\t", index=False))
                f.write("\n\n")

    print(f"Saved: {wide_csv}")
    print(f"Saved: {wide_xlsx}")
    print(f"Saved: {blocks_xlsx}")
    print(f"Saved: {blocks_txt}")


if __name__ == "__main__":
    main()

    