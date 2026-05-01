from pathlib import Path

BANK_CODE = "BOJ"
BANK_NAME = "Bank of Japan"
CURRENCY = "JPY"
LANGUAGE = "en"

BOJ_BASE_URL = "https://www.boj.or.jp"

BOJ_MPM_INDEX_URL = "https://www.boj.or.jp/en/mopo/mpmsche_minu/index.htm"
BOJ_MPM_PAST_URL = "https://www.boj.or.jp/en/mopo/mpmsche_minu/past.htm"
BOJ_MINUTES_ALL_URL = "https://www.boj.or.jp/en/mopo/mpmsche_minu/minu_all/index.htm"
BOJ_OUTLOOK_URL = "https://www.boj.or.jp/en/mopo/outlook/index.htm"

LOCAL_DATA_DIR = Path("data/geoscen/cb/boj")

MPM_OUTPUT = LOCAL_DATA_DIR / "boj_mpm_t1.parquet"
OUTLOOK_OUTPUT = LOCAL_DATA_DIR / "boj_outlook_t1.parquet"
COMBINED_CANONICAL_OUTPUT = LOCAL_DATA_DIR / "boj_combined_canonical_v1.parquet"

R2_PREFIX = "spine_global/leaves/geoscen/cb/boj"

