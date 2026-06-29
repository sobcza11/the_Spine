from pathlib import Path
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup


INPUT_PATH = Path("data/geoscen/cb/boc/boc_rate_announcements_t1.parquet")
OUTPUT_PATH = Path("data/geoscen/cb/boc/boc_rate_text_t1.parquet")


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()


def extract_html_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    node = (
        soup.find("main")
        or soup.find("article")
        or soup.find("div", class_=re.compile("post|content|article|body", re.I))
        or soup.body
    )

    return clean_text(node.get_text(" ")) if node else ""


def fetch_text(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return extract_html_text(r.text)


def run() -> None:
    df = pd.read_parquet(INPUT_PATH)

    rows = []
    for _, row in df.iterrows():
        url = row.get("url", "")
        text = ""

        try:
            text = fetch_text(url)
        except Exception as e:
            print(f"FAILED: {url} | {e}")

        out = row.to_dict()
        out["text"] = text
        out["text_chars"] = len(text)
        rows.append(out)

    out_df = pd.DataFrame(rows)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(OUTPUT_PATH, index=False)

    print(f"BoC text rows: {len(out_df)}")
    print(f"Wrote: {OUTPUT_PATH}")


if __name__ == "__main__":
    run()

    