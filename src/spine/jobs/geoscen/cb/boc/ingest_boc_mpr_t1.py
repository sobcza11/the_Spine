import pandas as pd
from datetime import datetime, timezone

from spine.jobs.geoscen.cb.boc.boc_constants import (
    BANK_CODE,
    BANK_NAME,
    CURRENCY,
    LANGUAGE,
    MPR_OUTPUT,
)

from spine.jobs.geoscen.cb.boc.boc_url_discovery import (
    discover_boc_mpr_urls,
)

DOCUMENT_TYPE = "monetary_policy_report"


def ingest_boc_mpr() -> pd.DataFrame:
    discovered = discover_boc_mpr_urls()
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

    MPR_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(MPR_OUTPUT, index=False)

    print(f"[DISCOVERY] {len(discovered)} BoC MPR links")
    print(f"[OK] Wrote {len(df)} BoC MPR rows")
    print(f"[PATH] {MPR_OUTPUT}")

    return df


if __name__ == "__main__":
    ingest_boc_mpr()

