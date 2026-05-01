import requests

from spine.jobs.geoscen.cb.boe.boe_constants import (
    BOE_BASE_URL,
    BOE_MONTH_SLUGS,
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def candidate_urls(path_root: str, start_year: int, end_year: int) -> list[str]:
    urls = []

    for year in range(start_year, end_year + 1):
        for month in BOE_MONTH_SLUGS:
            urls.append(
                f"{BOE_BASE_URL}/{path_root}/{year}/{month}-{year}"
            )

    return urls


def is_live_boe_page(url: str) -> bool:
    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        if response.status_code != 200:
            return False

        text = response.text.lower()

        if "/error/404.html" in response.url.lower():
            return False

        if "page not found" in text:
            return False

        return True

    except requests.RequestException:
        return False
    
