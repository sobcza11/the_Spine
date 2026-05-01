import pandas as pd
from datetime import datetime, timezone

from spine.jobs.geoscen.cb.boj.boj_constants import (
    BANK_CODE,
    BANK_NAME,
    CURRENCY,
    LANGUAGE,
    BOJ_OUTLOOK_URL,
    OUTLOOK_OUTPUT,
)

from spine.jobs.geoscen.cb.boj.boj_url_discovery import (
    extract_links_from_page,
)

DOCUMENT_TYPE = "outlook_report"


def ingest_boj_outlook() -> pd.DataFrame:
    discovered = extract_links_from_page(
        BOJ_OUTLOOK_URL,
        include_patterns=[
            "/en/mopo/outlook/",
            ".pdf",
        ],
    )

    filtered = []

    for item in discovered:
        url = item["url"].lower()
        title = item["title"].lower()

        if "outlook" in url or "outlook" in title:
            filtered.append(item)

    deduped = list({row["url"]: row for row in filtered}.values())

    rows = []

    for item in deduped:
        rows.append(
            {
                "bank_code": BANK_CODE,
                "bank_name": BANK_NAME,
                "currency": CURRENCY,
                "language": LANGUAGE,
                "document_type": DOCUMENT_TYPE,
                "title": item["title"],
                "url": item["url"],
                "source": BANK_NAME,
                "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

    df = pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

    OUTLOOK_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTLOOK_OUTPUT, index=False)

    print(f"[DISCOVERY] {len(deduped)} BoJ Outlook links")
    print(f"[OK] Wrote {len(df)} BoJ Outlook rows")
    print(f"[PATH] {OUTLOOK_OUTPUT}")

    return df


if __name__ == "__main__":
    ingest_boj_outlook()

