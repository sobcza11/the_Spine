"""
fed_text_downloader.py

One-shot auto-downloader for Federal Reserve policy-related texts:

- Beige Book
- FOMC Meeting Minutes
- FOMC Statements
- SEP (Summary of Economic Projections) text
- Fed Speeches

Usage:

    python fed_text_downloader.py --base-dir data

Optional limits:

    python fed_text_downloader.py --base-dir data --max-beige 20 --max-minutes 20 --max-statements 20 --max-sep 20 --max-speeches 50
"""

import argparse
import re
import time
from pathlib import Path
from typing import List, Tuple, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.federalreserve.gov"


# ---------- Generic helpers ----------

def safe_get(url: str, sleep: float = 0.5) -> Optional[requests.Response]:
    """GET with basic error handling + small delay to be polite."""
    try:
        print(f"[GET] {url}")
        resp = requests.get(url, timeout=20)
        time.sleep(sleep)
        if resp.status_code == 200:
            return resp
        print(f"  ! Non-200 status: {resp.status_code}")
        return None
    except Exception as e:
        print(f"  ! Error fetching {url}: {e}")
        return None


def html_to_text(html: str) -> str:
    """Convert HTML to a reasonably clean text representation."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove script/style
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text("\n")
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sanitize_filename(name: str) -> str:
    name = name.strip().replace(" ", "_")
    name = re.sub(r"[^A-Za-z0-9_\-\.]", "", name)
    return name


def save_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")
    print(f"  -> Saved: {path}")


def extract_meeting_date_from_text(text: str) -> Optional[str]:
    """
    Best-effort attempt to find a YYYY-MM-DD-like date in the page text.
    """
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", text)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None


# ---------- Beige Book ----------

def download_beige_books(base_dir: Path, max_docs: int = 50) -> None:
    """Download Beige Book reports from the main Beige Book page."""
    print("\n=== Downloading Beige Book reports ===")
    target_dir = base_dir / "beige_book"
    ensure_dir(target_dir)

    # ✅ Correct Beige Book index URL
    index_url = urljoin(BASE_URL, "/monetarypolicy/publications/beige-book-default.htm")

    resp = safe_get(index_url)
    if not resp:
        print("! Could not fetch Beige Book index page.")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    links: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "monetarypolicy/beigebook" in href and href.lower().endswith(".htm"):
            full_url = urljoin(BASE_URL, href)
            label = a.get_text(strip=True) or Path(urlparse(href).path).name
            links.append((label, full_url))

    seen = set()
    unique_links: List[Tuple[str, str]] = []
    for label, full_url in links:
        if full_url not in seen:
            seen.add(full_url)
            unique_links.append((label, full_url))

    unique_links = unique_links[:max_docs]

    for label, url in unique_links:
        resp = safe_get(url)
        if not resp:
            continue
        text = html_to_text(resp.text)

        # Try to pull YYYYMM from the URL, e.g. beigebook202511-summary.htm
        m = re.search(r"beigebook(\d{4})(\d{2})", url)
        if m:
            date_hint = f"{m.group(1)}-{m.group(2)}"
        else:
            date_hint = extract_meeting_date_from_text(text) or sanitize_filename(label)

        filename = f"{date_hint}_beige_book.txt"
        out_path = target_dir / filename
        save_text(out_path, text)


# ---------- FOMC Minutes ----------

def download_minutes(base_dir: Path, max_docs: int = 50) -> None:
    """Download FOMC Minutes via the FOMC calendars & information page."""
    print("\n=== Downloading FOMC Minutes ===")
    target_dir = base_dir / "fomc_minutes"
    ensure_dir(target_dir)

    calendar_url = urljoin(BASE_URL, "/monetarypolicy/fomccalendars.htm")
    resp = safe_get(calendar_url)
    if not resp:
        print("! Could not fetch FOMC calendar page for minutes.")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Minutes pages look like: /monetarypolicy/fomcminutes20250917.htm
        if "monetarypolicy/fomcminutes" in href and href.lower().endswith(".htm"):
            full_url = urljoin(BASE_URL, href)
            links.append(full_url)

    # Deduplicate and limit
    links = list(dict.fromkeys(links))[:max_docs]

    if not links:
        print("  ! No minutes links found on calendar page.")
        return

    for url in links:
        resp = safe_get(url)
        if not resp:
            continue
        text = html_to_text(resp.text)
        date_hint = extract_meeting_date_from_text(text) or Path(urlparse(url).path).stem
        filename = f"{date_hint}_minutes.txt"
        out_path = target_dir / filename
        save_text(out_path, text)


# ---------- FOMC Statements & SEP (via calendar pages) ----------

def download_statements_and_sep(base_dir: Path,
                                max_statements: int = 50,
                                max_sep: int = 50) -> None:
    """Download FOMC Statements and SEP text via FOMC calendars."""
    print("\n=== Downloading FOMC Statements & SEP text ===")
    statements_dir = base_dir / "fomc_statements"
    sep_dir = base_dir / "sep_dotplots"
    ensure_dir(statements_dir)
    ensure_dir(sep_dir)

    calendar_url = urljoin(BASE_URL, "/monetarypolicy/fomccalendars.htm")
    resp = safe_get(calendar_url)
    if not resp:
        print("! Could not fetch FOMC calendar page.")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    statement_links: List[str] = []
    sep_links: List[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()

        if "pressreleases" in href and "monetary" in href and href.endswith(".htm"):
            full_url = urljoin(BASE_URL, href)
            statement_links.append(full_url)

        if "projtabl" in href or "sep" in href:
            full_url = urljoin(BASE_URL, href)
            sep_links.append(full_url)

    statement_links = list(dict.fromkeys(statement_links))[:max_statements]
    sep_links = list(dict.fromkeys(sep_links))[:max_sep]

    print(f"  Found {len(statement_links)} candidate statement links.")
    for url in statement_links:
        resp = safe_get(url)
        if not resp:
            continue
        text = html_to_text(resp.text)
        date_hint = extract_meeting_date_from_text(text) or Path(urlparse(url).path).stem
        filename = f"{date_hint}_statement.txt"
        out_path = statements_dir / filename
        save_text(out_path, text)

    print(f"  Found {len(sep_links)} candidate SEP links.")
    for url in sep_links:
        resp = safe_get(url)
        if not resp:
            continue
        text = html_to_text(resp.text)
        date_hint = extract_meeting_date_from_text(text) or Path(urlparse(url).path).stem
        filename = f"{date_hint}_sep.txt"
        out_path = sep_dir / filename
        save_text(out_path, text)


# ---------- Fed Speeches ----------

def download_speeches(base_dir: Path, max_docs: int = 100) -> None:
    """Download Fed speeches from the main speeches page."""
    print("\n=== Downloading Fed speeches ===")
    target_dir = base_dir / "speeches"
    ensure_dir(target_dir)

    index_url = urljoin(BASE_URL, "/newsevents/speeches.htm")
    resp = safe_get(index_url)
    if not resp:
        print("! Could not fetch speeches index page.")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    links: List[str] = []
    titles: List[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "newsevents/speech" in href and href.lower().endswith(".htm"):
            full_url = urljoin(BASE_URL, href)
            title = a.get_text(strip=True) or Path(urlparse(href).path).stem
            links.append(full_url)
            titles.append(title)

    combined = []
    seen = set()
    for url, title in zip(links, titles):
        if url not in seen:
            seen.add(url)
            combined.append((url, title))

    combined = combined[:max_docs]

    for url, title in combined:
        resp = safe_get(url)
        if not resp:
            continue
        text = html_to_text(resp.text)
        date_hint = extract_meeting_date_from_text(text) or sanitize_filename(title)
        filename = f"{date_hint}_speech.txt"
        out_path = target_dir / filename
        save_text(out_path, text)


# ---------- CLI main ----------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Fed Beige Book, FOMC Minutes, Statements, SEP text, and speeches into plain .txt files."
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default="data",
        help="Base directory under which subfolders (beige_book, fomc_minutes, etc.) will be created.",
    )
    parser.add_argument(
        "--max-beige",
        type=int,
        default=50,
        help="Maximum number of Beige Book documents to download.",
    )
    parser.add_argument(
        "--max-minutes",
        type=int,
        default=50,
        help="Maximum number of Minutes documents to download.",
    )
    parser.add_argument(
        "--max-statements",
        type=int,
        default=50,
        help="Maximum number of Statements to download.",
    )
    parser.add_argument(
        "--max-sep",
        type=int,
        default=50,
        help="Maximum number of SEP-related documents to download.",
    )
    parser.add_argument(
        "--max-speeches",
        type=int,
        default=100,
        help="Maximum number of speeches to download.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir)

    print(f"\nBase directory: {base_dir.resolve()}")
    ensure_dir(base_dir)

    download_beige_books(base_dir, max_docs=args.max_beige)
    download_minutes(base_dir, max_docs=args.max_minutes)
    download_statements_and_sep(
        base_dir,
        max_statements=args.max_statements,
        max_sep=args.max_sep,
    )
    download_speeches(base_dir, max_docs=args.max_speeches)

    print("\nAll done. Text files are stored under:")
    print(f"  {base_dir.resolve()}")


if __name__ == "__main__":
    main()
