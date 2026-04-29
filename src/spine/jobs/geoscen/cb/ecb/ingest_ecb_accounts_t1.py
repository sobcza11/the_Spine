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
    "https://www.ecb.europa.eu/press/accounts/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2026/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2025/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2024/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2023/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2022/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2021/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2020/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2019/html/index_include.en.html",
    "https://www.ecb.europa.eu/press/accounts/2018/html/index_include.en.html",
]

def fetch_account_links():
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
                    "account of the monetary policy meeting" in text
                    or "monetary policy account" in text
                    or "/press/accounts/" in full
                )
            ):
                links.append(full)

    links = sorted(set(links))
    print("Accounts links found:", len(links))
    return links


def parse_account_page(url):
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
        "document_type": "account",
        "title": title.get_text(" ", strip=True) if title else None,
        "date": date_value,
        "url": url,
        "text": text,
        "text_chars": len(text),
        "ingested_at_utc": datetime.now(UTC),
    }

def extract_ecb_date(url, title_text, page_text):
    match = re.search(r"(\d{6})", url)
    if match:
        return pd.to_datetime(match.group(1), format="%y%m%d").date().isoformat()

    match = re.search(r"\b(\d{1,2}(?:-\d{1,2})? \w+ \d{4})\b", title_text + " " + page_text)
    if match:
        raw = match.group(1)
        raw = re.sub(r"^\d{1,2}-", "", raw)
        return pd.to_datetime(raw, errors="coerce").date().isoformat()

    return None

def run():
    links = fetch_account_links()
    rows = [parse_account_page(url) for url in links]

    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(ECB_OUTPUT_ACCOUNTS_PATH), exist_ok=True)
    df.to_parquet(ECB_OUTPUT_ACCOUNTS_PATH, index=False)

    print("ECB Accounts rows:", len(df))


if __name__ == "__main__":
    run()

