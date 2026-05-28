import re
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


URLS = {
    "Mar-26": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/march/",
    "Apr-26": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/april/",
}

INDUSTRIES = [
    "Apparel, Leather & Allied Products",
    "Chemical Products",
    "Computer & Electronic Products",
    "Electrical Equipment, Appliances & Components",
    "Fabricated Metal Products",
    "Food, Beverage & Tobacco Products",
    "Furniture & Related Products",
    "Machinery",
    "Miscellaneous Manufacturing",
    "Nonmetallic Mineral Products",
    "Paper Products",
    "Petroleum & Coal Products",
    "Plastics & Rubber Products",
    "Primary Metals",
    "Printing & Related Support Activities",
    "Textile Mills",
    "Transportation Equipment",
    "Wood Products",
]

COMPONENTS = {
    "New Orders": "_no",
    "Production": "_pro",
    "Employment": "_emp",
    "Supplier Deliveries": "_sd",
    "Inventories": "_inv",
    "Customers' Inventories": "_cin",
    "Prices": "_p",
    "Backlog of Orders": "_bkl",
    "New Export Orders": "_exp",
    "Imports": "_imp",
    "Buying Policy": "_bp",
}

HEADERS = {"User-Agent": "Mozilla/5.0"}


RAW_DIR = Path("data/ism/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


LOCAL_TEXT = {
    "Mar-26": RAW_DIR / "ism_manufacturing_march_2026.txt",
    "Apr-26": RAW_DIR / "ism_manufacturing_april_2026.txt",
}


def fetch_text(label: str, url: str) -> str:
    local_path = LOCAL_TEXT[label]

    if local_path.exists() and local_path.stat().st_size > 0:
        return local_path.read_text(encoding="utf-8", errors="ignore")

    r = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=30,
        allow_redirects=False,
    )

    if r.status_code in [301, 302, 303, 307, 308]:
        raise RuntimeError(
            f"ISM redirected to login/SSO for {label}. "
            f"Paste the report text into: {local_path}"
        )

    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n", strip=True)

    local_path.write_text(text, encoding="utf-8")
    return text

def clean_text(text: str) -> str:
    text = text.replace("Customers’", "Customers'")
    text = text.replace("—", "-")
    text = re.sub(r"\s+", " ", text)
    return text


def extract_section(text: str, start: str, end: str | None) -> str:
    start_match = re.search(rf"\b{re.escape(start)}\b", text, flags=re.I)
    if not start_match:
        return ""

    section_start = start_match.start()

    if end:
        end_match = re.search(rf"\b{re.escape(end)}\b", text[section_start + 1:], flags=re.I)
        if end_match:
            section_end = section_start + 1 + end_match.start()
            return text[section_start:section_end]

    return text[section_start:]


def split_industries(raw: str) -> list[str]:
    raw = raw.replace("; and ", "; ")
    raw = raw.replace(";and ", "; ")
    raw = raw.replace(" and ", "; ")
    parts = [p.strip(" .;:") for p in raw.split(";")]
    return [p for p in parts if p in INDUSTRIES]


def apply_ranked_list(row: dict, suffix: str, raw_list: str, direction: int):
    industries = split_industries(raw_list)

    n = len(industries)
    for i, industry in enumerate(industries):
        col = f"{industry}{suffix}"

        if row[col] != 0:
            continue

        rank_value = n - i

        if direction < 0:
            rank_value = -rank_value

        row[col] = rank_value


def capture_after(section: str, pattern: str) -> str:
    m = re.search(pattern, section, flags=re.I | re.S)
    return m.group(1).strip() if m else ""


def parse_component(row: dict, text: str, suffix: str, component: str):
    if component == "Buying Policy":
        return

    rules = {
        "New Orders": [
            (r"reported growth in new orders.*?are:\s*(.*?)\.", 1),
            (r"reporting a decline in new orders.*?are:\s*(.*?)\.", -1),
        ],
        "Production": [
            (r"reporting growth in production.*?are:\s*(.*?)\.", 1),
            (r"reporting a decrease in production.*?are:\s*(.*?)\.", -1),
        ],
        "Supplier Deliveries": [
            (r"reported slower supplier deliveries.*?are:\s*(.*?)\.", 1),
            (r"reported faster supplier deliveries.*?are:\s*(.*?)\.", -1),
            (r"reported faster deliveries.*?are:\s*(.*?)\.", -1),
        ],
        "Customers' Inventories": [
            (r"reported customers'? inventories as too high.*?are:\s*(.*?)\.", 1),
            (r"reported customers'? inventories as too low.*?are:\s*(.*?)\.", -1),
            (r"customers'? inventories.*?too high.*?are:\s*(.*?)\.", 1),
            (r"customers'? inventories.*?too low.*?are:\s*(.*?)\.", -1),
            (r"industries reporting customers'? inventories as too high.*?are:\s*(.*?)\.", 1),
            (r"industries reporting customers'? inventories as too low.*?are:\s*(.*?)\.", -1),
        ],
        "Employment": [
            (r"reported employment growth.*?are:\s*(.*?)\.", 1),
            (r"reporting a decrease in employment.*?are:\s*(.*?)\.", -1),
        ],
        "Inventories": [
            (r"reporting higher inventories.*?are:\s*(.*?)\.", 1),
            (r"reporting lower inventories.*?are:\s*(.*?)\.", -1),
        ],
        "Prices": [
            (r"reported paying increased prices.*?are:\s*(.*?)\.", 1),
            (r"reported paying decreased prices.*?are:\s*(.*?)\.", -1),
            (r"reported paying decreased prices.*?was\s*(.*?)\.", -1),
        ],
        "Backlog of Orders": [
            (r"reporting higher backlogs.*?are:\s*(.*?)\.", 1),
            (r"reporting lower backlogs.*?are:\s*(.*?)\.", -1),
        ],
        "New Export Orders": [
            (r"reported growth in new export orders.*?are:\s*(.*?)\.", 1),
            (r"reported a decrease in new export orders.*?are:\s*(.*?)\.", -1),
        ],
        "Imports": [
            (r"reporting higher imports.*?are:\s*(.*?)\.", 1),
            (r"reporting lower import volumes.*?are:\s*(.*?)\.", -1),
            (r"reported lower volumes.*?are:\s*(.*?)\.", -1),
        ],
    }

    for pattern, direction in rules[component]:
        m = re.search(pattern, text, flags=re.I | re.S)
        if m:
            apply_ranked_list(row, suffix, m.group(1), direction)


def parse_month(label: str, url: str) -> dict:
    raw = fetch_text(label, url)
    text = clean_text(raw)

    start_anchor = "MANUFACTURING INDEX SUMMARIES"
    text = text[text.upper().find(start_anchor):]

    ordered = list(COMPONENTS.keys())

    row = {"date": label}
    for industry in INDUSTRIES:
        for suffix in COMPONENTS.values():
            row[f"{industry}{suffix}"] = 0

    for component in ordered:
        parse_component(row, text, COMPONENTS[component], component)

    return row


def main():
    rows = [parse_month(label, url) for label, url in URLS.items()]
    df = pd.DataFrame(rows)

    ordered_cols = ["date"]
    for industry in INDUSTRIES:
        for suffix in COMPONENTS.values():
            ordered_cols.append(f"{industry}{suffix}")

    df = df[ordered_cols]

    out_dir = Path("data/ism/processed")
    out_dir.mkdir(parents=True, exist_ok=True)

    wide_csv_path = out_dir / "ism_manufacturing_industry_component_wide.csv"
    wide_xlsx_path = out_dir / "ism_manufacturing_industry_component_wide.xlsx"
    preview_path = out_dir / "ism_manufacturing_industry_component_preview.txt"

    df.to_csv(wide_csv_path, sep="\t", index=False)
    df.to_excel(wide_xlsx_path, index=False)
    preview_path.write_text(df.to_csv(sep="\t", index=False), encoding="utf-8")

    blocks_xlsx_path = out_dir / "ism_manufacturing_component_blocks.xlsx"
    blocks_txt_path = out_dir / "ism_manufacturing_component_blocks_tab_delimited.txt"

    with pd.ExcelWriter(blocks_xlsx_path, engine="openpyxl") as writer:
        with blocks_txt_path.open("w", encoding="utf-8") as f:
            for component_name, suffix in COMPONENTS.items():
                block_cols = ["date"] + [f"{industry}{suffix}" for industry in INDUSTRIES]
                block_df = df[block_cols].copy()
                block_df = block_df.rename(columns={"date": "dates"})

                sheet_name = suffix.replace("_", "")
                block_df.to_excel(writer, sheet_name=sheet_name, index=False)

                f.write(f"{component_name} {suffix}\n")
                f.write(block_df.to_csv(sep="\t", index=False))
                f.write("\n\n")

    print(f"Saved: {wide_csv_path}")
    print(f"Saved: {wide_xlsx_path}")
    print(f"Saved: {preview_path}")
    print(f"Saved: {blocks_xlsx_path}")
    print(f"Saved: {blocks_txt_path}")


if __name__ == "__main__":
    main()

    