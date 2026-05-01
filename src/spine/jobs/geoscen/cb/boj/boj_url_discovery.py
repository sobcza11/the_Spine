import requests
from bs4 import BeautifulSoup

from spine.jobs.geoscen.cb.boj.boj_constants import BOJ_BASE_URL

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def clean_text(value: str) -> str:
    return " ".join(str(value or "").split())


def full_url(href: str) -> str:
    if not href:
        return ""

    if href.startswith("http"):
        return href

    if href.startswith("/"):
        return BOJ_BASE_URL + href

    return f"{BOJ_BASE_URL}/{href}"


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

        rows.append(
            {
                "title": title,
                "url": href,
            }
        )

    return list({row["url"]: row for row in rows}.values())

