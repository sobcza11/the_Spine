import re
import requests
from bs4 import BeautifulSoup

from spine.jobs.geoscen.cb.boc.boc_constants import (
    BOC_BASE_URL,
    BOC_START_YEAR,
    BOC_END_YEAR,
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def full_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BOC_BASE_URL + href
    return f"{BOC_BASE_URL}/{href}"


def clean_text(value: str) -> str:
    return " ".join(str(value or "").split())


def extract_links_from_page(url: str, include_patterns: list[str]) -> list[dict]:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    rows = []

    for link in soup.find_all("a", href=True):
        title = clean_text(link.get_text(" "))
        href = full_url(link.get("href", ""))

        if not title or not href:
            continue

        href_lower = href.lower()

        if not any(pattern in href_lower for pattern in include_patterns):
            continue

        rows.append({"title": title, "url": href})

    return rows


def discover_boc_rate_announcement_urls() -> list[dict]:
    rows = []

    archive_pages = [
        f"{BOC_BASE_URL}/press/press-releases/",
        f"{BOC_BASE_URL}/core-functions/monetary-policy/key-interest-rate/",
    ]

    for year in range(BOC_START_YEAR, BOC_END_YEAR + 1):
        archive_pages.extend(
            [
                f"{BOC_BASE_URL}/{year}/",
                f"{BOC_BASE_URL}/press/press-releases/?mt_page={year}",
            ]
        )

    for page in archive_pages:
        try:
            rows.extend(
                extract_links_from_page(
                    page,
                    include_patterns=["fad-press-release"],
                )
            )
        except requests.RequestException:
            continue

    deduped = {row["url"]: row for row in rows}
    return list(deduped.values())


def discover_boc_mpr_urls() -> list[dict]:
    rows = []

    archive_pages = [
        f"{BOC_BASE_URL}/publications/mpr/",
    ]

    for year in range(BOC_START_YEAR, BOC_END_YEAR + 1):
        archive_pages.extend(
            [
                f"{BOC_BASE_URL}/publications/mpr/{year}/",
                f"{BOC_BASE_URL}/{year}/",
            ]
        )

    for page in archive_pages:
        try:
            rows.extend(
                extract_links_from_page(
                    page,
                    include_patterns=[
                        "/publications/mpr/mpr-",
                        "monetary-policy-report",
                    ],
                )
            )
        except requests.RequestException:
            continue

    deduped = {row["url"]: row for row in rows}

    return [
        row for row in deduped.values()
        if (
            re.search(r"/publications/mpr/mpr-\d{4}-\d{2}-\d{2}/?$", row["url"])
            or "monetary-policy-report" in row["url"].lower()
        )
    ]