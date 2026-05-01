import pandas as pd
from datetime import datetime, timezone

from spine.jobs.geoscen.cb.boc.boc_constants import (
    BANK_CODE,
    BANK_NAME,
    CURRENCY,
    LANGUAGE,
    RATE_ANNOUNCEMENTS_OUTPUT,
)

from spine.jobs.geoscen.cb.boc.boc_url_discovery import (
    discover_boc_rate_announcement_urls,
)

DOCUMENT_TYPE = "rate_announcement"


def ingest_boc_rate_announcements() -> pd.DataFrame:
    discovered = discover_boc_rate_announcement_urls()
    rows = []

    for item in discovered:
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

    RATE_ANNOUNCEMENTS_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(RATE_ANNOUNCEMENTS_OUTPUT, index=False)

    print(f"[DISCOVERY] {len(discovered)} BoC rate announcement links")
    print(f"[OK] Wrote {len(df)} BoC rate announcement rows")
    print(f"[PATH] {RATE_ANNOUNCEMENTS_OUTPUT}")

    return df


if __name__ == "__main__":
    ingest_boc_rate_announcements()
