import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

from spine.jobs.geoscen.cb.boe.boe_constants import (
    BANK_CODE,
    BANK_NAME,
    CURRENCY,
    LANGUAGE,
    BOE_BASE_URL,
    BOE_MPR_PATH,
    BOE_MPR_START_YEAR,
    BOE_END_YEAR,
    MPR_OUTPUT,
)

from spine.jobs.geoscen.cb.boe.boe_url_discovery import (
    candidate_urls,
    is_live_boe_page,
    HEADERS,
)

DOCUMENT_TYPE = "monetary_policy_report"


def clean_text(value: str) -> str:
    return " ".join(str(value or "").split())


def full_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return BOE_BASE_URL + href


def get_live_mpr_urls() -> list[str]:
    urls = candidate_urls(
        BOE_MPR_PATH,
        BOE_MPR_START_YEAR,
        BOE_END_YEAR,
    )

    live_urls = [u for u in urls if is_live_boe_page(u)]

    print(f"[DISCOVERY] {len(live_urls)} live BoE MPR pages")

    return live_urls


def ingest_boe_mpr() -> pd.DataFrame:
    rows = []

    for page_url in get_live_mpr_urls():
        response = requests.get(
            page_url,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        page_title = clean_text(soup.title.get_text(" ")) if soup.title else ""

        rows.append(
            {
                "bank_code": BANK_CODE,
                "bank_name": BANK_NAME,
                "currency": CURRENCY,
                "language": LANGUAGE,
                "document_type": DOCUMENT_TYPE,
                "title": page_title,
                "url": page_url,
                "source": BANK_NAME,
                "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

        for link in soup.find_all("a", href=True):
            title = clean_text(link.get_text(" "))
            href = link.get("href", "")

            if not title:
                continue

            if "monetary-policy-report" not in href:
                continue

            rows.append(
                {
                    "bank_code": BANK_CODE,
                    "bank_name": BANK_NAME,
                    "currency": CURRENCY,
                    "language": LANGUAGE,
                    "document_type": DOCUMENT_TYPE,
                    "title": title,
                    "url": full_url(href),
                    "source": BANK_NAME,
                    "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            )

    df = pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

    MPR_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(MPR_OUTPUT, index=False)

    print(f"[OK] Wrote {len(df)} BoE MPR rows")
    print(f"[PATH] {MPR_OUTPUT}")

    return df


if __name__ == "__main__":
    ingest_boe_mpr()


    