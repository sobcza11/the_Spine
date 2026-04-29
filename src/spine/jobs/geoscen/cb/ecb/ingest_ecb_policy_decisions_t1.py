import os
import re
from datetime import datetime, UTC
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .ecb_constants import *

BASE_URL = "https://www.ecb.europa.eu"

PAGES = [
    "https://www.ecb.europa.eu/press/govcdec/mopo/2026/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/govcdec/mopo/2025/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/govcdec/mopo/2024/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/govcdec/mopo/2023/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/govcdec/mopo/2022/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/govcdec/mopo/2021/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/govcdec/mopo/2020/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/govcdec/mopo/2019/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/govcdec/mopo/2018/html/index_include.en.html",
]


def fetch_policy_links():
    links = []

    for url in PAGES:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a[href]"):
            href = a["href"]
            text = a.get_text(" ", strip=True).lower()
            full = urljoin(BASE_URL, href)

            if (
                (".en.html" in full or "index_include.en.html" in full)
                and "ecb.europa.eu" in full
                and (
                    "monetary policy decision" in text
                    or "key ecb interest rates" in text
                    or "/press/pr/date/" in full
                    or "/press/govcdec/mopo/" in full
                )
            ):
                links.append(full)

    links = sorted(set(links))
    print("Policy links found:", len(links))
    return links


def parse_policy_page(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    title = soup.find("h1")
    title_text = title.get_text(" ", strip=True) if title else ""

    page_text = soup.get_text(" ", strip=True)

    date_value = extract_ecb_date(url, title_text, page_text)

    paragraphs = soup.select("main p") or soup.select("p")
    text = " ".join(p.get_text(" ", strip=True) for p in paragraphs)
    text = re.sub(r"\s+", " ", text).strip()

    return {
        "bank": ECB_BANK,
        "bank_code": ECB_BANK_CODE,
        "currency": ECB_CURRENCY,
        "document_type": "policy_decision",
        "title": title.get_text(" ", strip=True) if title else None,
        "date": date_value,
        "url": url,
        "text": text,
        "text_chars": len(text),
        "ingested_at_utc": datetime.now(UTC),
    }


def run():
    links = fetch_policy_links()
    rows = [parse_policy_page(url) for url in links]

    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(ECB_OUTPUT_POLICY_PATH), exist_ok=True)
    df.to_parquet(ECB_OUTPUT_POLICY_PATH, index=False)

    print("ECB Policy Decisions rows:", len(df))

def extract_ecb_date(url, title_text, page_text):
    match = re.search(r"(?:mp|pr)(\d{6})", url)
    if match:
        return pd.to_datetime(match.group(1), format="%y%m%d").date().isoformat()

    match = re.search(r"\b(\d{1,2} \w+ \d{4})\b", title_text + " " + page_text)
    if match:
        return pd.to_datetime(match.group(1), errors="coerce").date().isoformat()

    return None


if __name__ == "__main__":
    run()

