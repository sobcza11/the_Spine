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
        print("=" * 100)
        print(month, url)
        print("=" * 100)

        html = requests.get(url, headers=HEADERS, timeout=60).text
        soup = BeautifulSoup(html, "html.parser")

        debug_path = out_dir / f"ism_manufacturing_{month}_structure.txt"

        lines = []

        for tag in soup.find_all(["h1", "h2", "h3", "h4", "p", "li", "table"]):
            name = tag.name.upper()
            text = " ".join(tag.get_text(" ", strip=True).split())

            if not text:
                continue

            if any(
                key.lower() in text.lower()
                for key in [
                    "New Orders",
                    "Production",
                    "Employment",
                    "Supplier Deliveries",
                    "Inventories",
                    "Customers' Inventories",
                    "Prices",
                    "Backlog of Orders",
                    "New Export Orders",
                    "Imports",
                    "Buying Policy",
                ]
            ):
                lines.append(f"\n[{name}] {text[:1500]}")

        debug_path.write_text("\n".join(lines), encoding="utf-8")

        print(f"debug output: {debug_path}")


if __name__ == "__main__":
    main()
