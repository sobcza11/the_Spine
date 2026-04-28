"""
Extract selected FOMC historical PDF text into a separate v2 text parquet.

Run:
python -m spine.jobs.geoscen.fomc.historical.extract_fomc_historical_pdf_text_v2
"""

from __future__ import annotations

import hashlib
import io
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from pypdf import PdfReader

from spine.jobs.geoscen.fomc.historical.fomc_historical_constants import LOCAL_OUTPUT_PATH


TEXT_OUTPUT_PATH = "data/geoscen/fomc/fomc_historical_pdf_text_v2.parquet"

TARGET_DOCUMENT_TYPES = {
    "minutes",
    "minutes_of_actions",
    "record_of_policy_actions",
    "transcript",
    "conference_call",
}

MIN_TEXT_CHARS = 500


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _download_pdf(url: str) -> bytes:
    response = requests.get(
        url,
        timeout=60,
        headers={"User-Agent": "the_Spine GeoScen FOMC PDF text v2"},
    )
    response.raise_for_status()
    return response.content


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages = []

    for page in reader.pages:
        pages.append(page.extract_text() or "")

    return _clean_text(" ".join(pages))


def main() -> None:
    ingested_at_utc = datetime.now(timezone.utc).isoformat()

    df = pd.read_parquet(LOCAL_OUTPUT_PATH)

    subset = df[
        (df["file_format"] == "pdf")
        & (df["document_type"].isin(TARGET_DOCUMENT_TYPES))
    ].copy()

    if subset.empty:
        raise RuntimeError("No target FOMC historical PDF rows found.")

    rows = []

    for i, row in subset.iterrows():
        url = row["url"]
        print(f"[GeoScen:FOMC:PDF:v2] extracting {i + 1}/{len(subset)} {row['document_type']} {url}")

        try:
            pdf_bytes = _download_pdf(url)
            text = _extract_pdf_text(pdf_bytes)
            status = "ok" if len(text) >= MIN_TEXT_CHARS else "short_text"
            error = ""
        except Exception as exc:
            text = ""
            status = "error"
            error = repr(exc)

        text_hash = _sha256(text) if text else ""

        rows.append(
            {
                "document_id": row["document_id"],
                "date": row["date"],
                "year": row["year"],
                "source": row["source"],
                "document_family": row["document_family"],
                "document_type": row["document_type"],
                "title": row["title"],
                "url": url,
                "file_format": row["file_format"],
                "text": text,
                "text_sha256": text_hash,
                "text_chars": len(text),
                "extract_status": status,
                "extract_error": error,
                "extracted_at_utc": ingested_at_utc,
                "version": "fomc_historical_pdf_text_v2",
            }
        )

    out = pd.DataFrame(rows).sort_values(["date", "document_type", "url"]).reset_index(drop=True)

    output_path = Path(TEXT_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)

    print(f"[GeoScen:FOMC:PDF:v2] rows={len(out)}")
    print(f"[GeoScen:FOMC:PDF:v2] ok={(out['extract_status'] == 'ok').sum()}")
    print(f"[GeoScen:FOMC:PDF:v2] short_text={(out['extract_status'] == 'short_text').sum()}")
    print(f"[GeoScen:FOMC:PDF:v2] error={(out['extract_status'] == 'error').sum()}")
    print(f"[GeoScen:FOMC:PDF:v2] wrote={output_path}")


if __name__ == "__main__":
    main()

