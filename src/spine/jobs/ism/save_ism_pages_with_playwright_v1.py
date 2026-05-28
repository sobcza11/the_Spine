# src/spine/jobs/ism/save_ism_pages_with_playwright_v1.py

from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


URLS = {
    "ism_manufacturing_march_2026.txt": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/march/",
    "ism_manufacturing_april_2026.txt": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/april/",
}

OUT_DIR = Path("data/ism/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )        
        page = browser.new_page()

        for filename, url in URLS.items():
            print(f"Opening: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(8000)

            html = page.content()
            text = html_to_text(html)

            out_path = OUT_DIR / filename
            out_path.write_text(text, encoding="utf-8")

            print(f"Saved: {out_path}")
            print(f"Length: {out_path.stat().st_size}")

        browser.close()


if __name__ == "__main__":
    main()
