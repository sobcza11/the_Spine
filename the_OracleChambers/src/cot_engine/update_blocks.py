"""
Block-wise COT downloader for the_OracleChambers.

Usage (from the_OracleChambers root):

    $env:PYTHONPATH = "$PWD\\src"
    python -m cot_engine.update_blocks block1
    python -m cot_engine.update_blocks block2
    python -m cot_engine.update_blocks block3

Blocks (current definition):
    block1: 2010–2019
    block2: 2000–2009
    block3: 1990–1999

Note:
    - For 2010–2019 we expect disaggregated COM XLS zips like:
        https://www.cftc.gov/files/dea/history/dea_com_xls_2010.zip
      which match your existing 2004+ schema.
    - For 2000–2003 we expect legacy "Futures Only" XLS zips like:
        https://www.cftc.gov/files/dea/history/deafut_xls_2001.zip
      (different schema; we’ll map later in feature engineering).
"""

import sys
import argparse
import logging

from cot_engine.zip_checker import CFTCDataDownloader

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BLOCKS = {
    "block1": range(2010, 2020),  # 2010–2019
    "block2": range(2000, 2010),  # 2000–2009 (2000–2003 are legacy)
    "block3": range(1990, 2000),  # 1990–1999 (likely legacy)
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download COT XLS data by 10-year blocks."
    )
    parser.add_argument(
        "block",
        choices=sorted(BLOCKS.keys()),
        help="Which block to fetch: block1, block2, block3",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = list(BLOCKS[args.block])

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [update_blocks] %(message)s",
    )

    logging.info("Requested %s for years: %s", args.block, years)

    downloader = CFTCDataDownloader(years=years)
    downloader.update()

    logging.info("Finished %s. Check data/cot/xls for new/updated XLS files.", args.block)
    logging.info(
        "You can now re-run: python -m cot_engine.build_cot_store_from_xls "
        "after you’re satisfied with all blocks you've fetched."
    )


if __name__ == "__main__":
    main()



