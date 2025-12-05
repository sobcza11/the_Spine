from __future__ import annotations

import io
import logging
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional, Iterable, Dict, Tuple, List

import requests


@dataclass(frozen=True)
class CandidateURL:
    url: str
    last_modified: Optional[datetime]  # tz-aware UTC


class CFTCDataDownloader:
    """
    Downloads CFTC historical yearly ZIPs and writes extracted yearly XLS into:
        data/cot/xls/{year}.xls

    Strategy:
      - Try disaggregated COM Excel zip (works for 2004+ in your environment):
          https://www.cftc.gov/files/dea/history/dea_com_xls_{year}.zip
      - Fallback to legacy futures-only Excel zip (works for 2000–2003):
          https://www.cftc.gov/files/dea/history/deafut_xls_{year}.zip

    Notes:
      - We store the extracted Excel file as {year}.xls regardless of source format.
      - Legacy years will not have the same column set; feature engineering will map later.
    """

    BASE = "https://www.cftc.gov/files/dea/history"

    def __init__(self, years: Iterable[int]):
        self.years = list(years)

        # Ensure dirs exist
        self.xls_dir = Path("data") / "cot" / "xls"
        self.xls_dir.mkdir(parents=True, exist_ok=True)

        self.log_dir = Path("log")
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "the_OracleChambers COT downloader (+https://github.com/)",
                "Accept": "*/*",
            }
        )

        self._configure_logging()

    def _configure_logging(self) -> None:
        # Avoid emoji/unicode in log text; Windows cp1252 console is fragile.
        handlers = [
            logging.StreamHandler(),
            logging.FileHandler(self.log_dir / "cftc_downloader.log", encoding="utf-8"),
        ]
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - [CFTCDataDownloader] %(message)s",
            handlers=handlers,
        )

    # ---------- URL patterns ----------

    def _candidate_urls_for_year(self, year: int) -> List[str]:
        """
        Ordered best-first.
        """
        # Disaggregated COM (works for 2004+ as you’ve already proven)
        com = f"{self.BASE}/dea_com_xls_{year}.zip"

        # Legacy futures-only Excel (needed for 2000–2003)
        fut = f"{self.BASE}/deafut_xls_{year}.zip"

        # You can add more patterns here later if you discover other archives.
        return [com, fut]

    # ---------- HTTP helpers ----------

    def _head_last_modified(self, url: str, timeout: int = 25) -> Tuple[bool, Optional[datetime]]:
        """
        Returns (ok, last_modified_utc).
        If HEAD fails but URL might still be downloadable, we return (False, None).
        """
        try:
            r = self.session.head(url, allow_redirects=True, timeout=timeout)
        except Exception:
            return False, None

        if r.status_code >= 400:
            return False, None

        lm_raw = r.headers.get("Last-Modified")
        if not lm_raw:
            return True, None

        try:
            dt = parsedate_to_datetime(lm_raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return True, dt.astimezone(timezone.utc)
        except Exception:
            return True, None

    def _get_bytes(self, url: str, timeout: int = 60) -> bytes:
        r = self.session.get(url, allow_redirects=True, timeout=timeout)
        r.raise_for_status()
        return r.content

    # ---------- local file helpers ----------

    def _local_mtime_utc(self, path: Path) -> Optional[datetime]:
        if not path.exists():
            return None
        ts = path.stat().st_mtime
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    # ---------- extraction ----------

    def _extract_first_excel_from_zip(self, zbytes: bytes, year: int) -> bytes:
        """
        Most of these zips contain a single .xls; sometimes multiple.
        We extract the first *.xls or *.xlsx found.
        """
        with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
            names = zf.namelist()
            excel_names = [n for n in names if n.lower().endswith((".xls", ".xlsx"))]
            if not excel_names:
                raise RuntimeError(f"No Excel files found inside zip for year {year}. Contents={names[:10]}...")
            chosen = excel_names[0]
            return zf.read(chosen)

    def _write_year_xls(self, year: int, excel_bytes: bytes) -> Path:
        out = self.xls_dir / f"{year}.xls"
        out.write_bytes(excel_bytes)
        return out

    # ---------- core logic ----------

    def _choose_best_url(self, year: int) -> Optional[CandidateURL]:
        """
        Tries candidate URL patterns and returns the first reachable URL with parsed Last-Modified (if available).
        """
        for url in self._candidate_urls_for_year(year):
            ok, lm = self._head_last_modified(url)
            if ok:
                return CandidateURL(url=url, last_modified=lm)
        return None

    def update(self) -> None:
        logging.info("Checking years: %s", self.years)

        updated: List[int] = []
        for year in self.years:
            target = self.xls_dir / f"{year}.xls"
            local_mtime = self._local_mtime_utc(target)

            chosen = self._choose_best_url(year)
            if chosen is None:
                logging.warning("Year %s missing (no working URL under known patterns).", year)
                continue

            # Decide download vs skip
            should_download = False
            if local_mtime is None:
                should_download = True
            elif chosen.last_modified is None:
                # If remote LM unknown, we play it safe: do NOT redownload unless file missing.
                should_download = False
            else:
                # Both are tz-aware UTC
                should_download = chosen.last_modified > local_mtime

            if not should_download:
                logging.info("Year %s up-to-date: %s", year, target.as_posix())
                continue

            logging.info("Downloading year %s from %s", year, chosen.url)
            try:
                zbytes = self._get_bytes(chosen.url)
                xbytes = self._extract_first_excel_from_zip(zbytes, year)
                out = self._write_year_xls(year, xbytes)
                updated.append(year)
                logging.info("Saved %s", out.as_posix())
            except Exception as e:
                logging.exception("Failed downloading/extracting year %s: %s", year, e)

        if updated:
            logging.info("Updated years: %s", updated)
        else:
            logging.info("No updates detected for any years.")

