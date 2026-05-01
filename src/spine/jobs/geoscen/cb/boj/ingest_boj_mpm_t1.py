import pandas as pd
from datetime import datetime, timezone

from spine.jobs.geoscen.cb.boj.boj_constants import (
    BANK_CODE,
    BANK_NAME,
    CURRENCY,
    LANGUAGE,
    BOJ_MPM_INDEX_URL,
    BOJ_MPM_PAST_URL,
    BOJ_MINUTES_ALL_URL,
    MPM_OUTPUT,
)

from spine.jobs.geoscen.cb.boj.boj_url_discovery import (
    extract_links_from_page,
)

DOCUMENT_TYPE = "monetary_policy_meeting"


def ingest_boj_mpm() -> pd.DataFrame:
    source_pages = [
        BOJ_MPM_INDEX_URL,
        BOJ_MPM_PAST_URL,
        BOJ_MINUTES_ALL_URL,
    ]

    include_patterns = [
        "/en/mopo/mpmsche_minu/",
        "/en/mopo/mpmdeci/",
        "/en/mopo/mpmsche_minu/minu_",
    ]

    discovered = []

    for page in source_pages:
        discovered.extend(
            extract_links_from_page(
                page,
                include_patterns=include_patterns,
            )
        )

    deduped = list({row["url"]: row for row in discovered}.values())

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

    MPM_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(MPM_OUTPUT, index=False)

    print(f"[DISCOVERY] {len(deduped)} BoJ MPM links")
    print(f"[OK] Wrote {len(df)} BoJ MPM rows")
    print(f"[PATH] {MPM_OUTPUT}")

    return df


if __name__ == "__main__":
    ingest_boj_mpm()

