from pathlib import Path
import requests
from bs4 import BeautifulSoup


URLS = {
    "2026-03": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/march/",
    "2026-04": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/april/",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def main():
    repo_root = Path.cwd()
    out_dir = repo_root / "data" / "ism" / "_debug"
    out_dir.mkdir(parents=True, exist_ok=True)

    for month, url in URLS.items():
        print(f"reading {month}: {url}")

        response = requests.get(url, headers=HEADERS, timeout=60)
        print("status:", response.status_code)
        print("html length:", len(response.text))

        raw_html_path = out_dir / f"ism_manufacturing_{month}_raw_html.html"
        raw_text_path = out_dir / f"ism_manufacturing_{month}_raw_text.txt"

        raw_html_path.write_text(response.text, encoding="utf-8")

        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text("\n", strip=True)

        raw_text_path.write_text(text, encoding="utf-8")

        print("raw html:", raw_html_path)
        print("raw text:", raw_text_path)
        print("text length:", len(text))
        print("contains New Orders:", "New Orders" in text)
        print("contains Production:", "Production" in text)
        print("")


if __name__ == "__main__":
    main()

