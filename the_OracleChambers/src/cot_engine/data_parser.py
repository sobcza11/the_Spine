"""
Parsing helpers for CFTC COT 'Other (Combined)' text.

This module is designed to work with the CFTC "Other (Combined) - Long Report"
(https://www.cftc.gov/dea/futures/other_lf.htm).

It provides:

- extract_commodity_reports: split the full text into per-commodity blocks,
  with metadata (commodity name, exchange, code, date, raw block).

- parse_positions_simple: parse the "Positions" table for a single commodity,
  returning one row per trader category (Managed Money, Swap Dealers, etc.)
  plus an "All" row with total open interest.

- parse_changes_simple: parse the "Changes in Commitments" section to extract
  the change in open interest (single-row DataFrame).

- parse_percents_simple: parse the "Percent of Open Interest Represented by
  Each Category of Trader" section and return one row per trader category
  with "Long %" and "Short %" columns.

- parse_traders_simple: currently a minimal stub; can be expanded if needed.

- parse_largest_traders_simple: parse the "Percent of Open Interest Held by
  the Indicated Number of the Largest Traders" section, returning rows with
  columns like "4 or Less Long %" and "4 or Less Short %".

These outputs are shaped to interoperate with build_cot_row.build_cot_row().
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup  # pip install beautifulsoup4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_pre_text(raw_html: str) -> str:
    """
    Extract the <pre> block from the CFTC HTML. If not found, return original.
    """
    soup = BeautifulSoup(raw_html, "html.parser")
    pre = soup.find("pre")
    if pre is None:
        return unescape(raw_html)
    return unescape(pre.get_text("\n"))


def _strip_line(line: str) -> str:
    """Normalize a CFTC line: strip trailing spaces & control characters."""
    return line.rstrip("\r\n")


def _is_header_line(line: str) -> bool:
    """
    Identify lines that start a new commodity block.

    Pattern looks like:
        SILVER - COMMODITY EXCHANGE INC. ... Code-084691
    """
    return "Code-" in line and "-" in line


def _parse_header_line(line: str) -> dict[str, Any]:
    """
    Parse a commodity header line into:
        - commodity
        - exchange
        - code
    """
    # Remove leading spaces & "decorative" colons, if any
    raw = line.strip()

    # Extract code
    m_code = re.search(r"Code-([0-9A-Za-z]+)", raw)
    code = m_code.group(1) if m_code else ""

    # Remove the Code- segment to isolate name/exchange part
    name_part = re.sub(r"\s+Code-[0-9A-Za-z]+.*$", "", raw)

    # Commodity name & exchange are separated by " - "
    # Use rpartition to split on the last occurrence
    left, sep, right = name_part.rpartition(" - ")
    if sep:
        commodity = left.strip()
        exchange = right.strip()
    else:
        # Fallback: treat entire string as commodity, unknown exchange
        commodity = name_part.strip()
        exchange = ""

    return {
        "commodity": commodity,
        "exchange": exchange,
        "code": code,
    }


def _parse_date_line(line: str) -> str | None:
    """
    Parse date from the second line of a block, e.g.:

        Disaggregated Commitments of Traders - Futures Only, November 10, 2025
    """
    m = re.search(r"Disaggregated Commitments of Traders.*?,\s*(.+)$", line.strip())
    if not m:
        return None
    return m.group(1).strip()


def _split_reports(pre_text: str, debug: bool = False) -> list[dict[str, Any]]:
    """
    Split pre-text into per-commodity report blocks.

    Each block is a dict with:
        - start_idx, end_idx (line indices in pre_text)
        - header_meta (commodity/exchange/code)
        - date (string or None)
    """
    lines = [_strip_line(l) for l in pre_text.splitlines()]
    n = len(lines)

    header_indices: list[int] = []
    for i, line in enumerate(lines):
        if _is_header_line(line):
            header_indices.append(i)

    if debug:
        print(f"[_split_reports] Found {len(header_indices)} header lines")

    blocks: list[dict[str, Any]] = []
    for idx, start in enumerate(header_indices):
        end = header_indices[idx + 1] if idx + 1 < len(header_indices) else n

        header_line = lines[start]
        date_line = lines[start + 1] if start + 1 < n else ""

        header_meta = _parse_header_line(header_line)
        report_date = _parse_date_line(date_line)

        raw_block = "\n".join(lines[start:end])

        block = {
            "commodity": header_meta["commodity"],
            "exchange": header_meta["exchange"],
            "code": header_meta["code"],
            "date": report_date,
            "raw": raw_block,
            "display_name": f"{header_meta['commodity']} - {header_meta['exchange']} ({header_meta['code']})".strip(),
            "contract_type": "",
            "report_type": "Other (Futures Only)",
        }
        blocks.append(block)

    return blocks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_commodity_reports(raw_text: str, debug: bool = False) -> list[dict[str, Any]]:
    """
    Split full CFTC 'Other (Combined)' page into per-commodity reports.

    Returns:
        List of dicts, each with keys:
            - commodity
            - exchange
            - code
            - date
            - raw
            - display_name
            - contract_type
            - report_type
    """
    pre_text = _extract_pre_text(raw_text)
    blocks = _split_reports(pre_text, debug=debug)

    if debug:
        print(f"[extract_commodity_reports] Parsed {len(blocks)} report blocks")

    return blocks


# ---------------------------------------------------------------------------
# Table parsers for a single commodity block
# ---------------------------------------------------------------------------

def parse_positions_simple(raw_block: str, debug: bool = False) -> pd.DataFrame | None:
    """
    Parse positions table for a single commodity.

    The relevant section looks like:

        :          :    Positions
        All  :   159,649:  7,790  30,915  26,794  55,885  10,985  35,613 ...

    We only use the 'All' row, decomposed into trader categories:
        - Producer/Merchant/Processor/User
        - Swap Dealers
        - Managed Money
        - Other Reportables
        - Nonreportable Positions

    Returns:
        DataFrame with columns:
            - Category
            - Open Interest
            - Long
            - Short

        with one row for:
            - "All" (only Open Interest populated)
            - "Producer/Merchant/Processor/User"
            - "Swap Dealers"
            - "Managed Money"
            - "Other Reportables"
            - "Nonreportable Positions"
    """
    lines = [_strip_line(l) for l in raw_block.splitlines()]

    # Find the "Positions" header line
    idx_positions = None
    for i, line in enumerate(lines):
        if "Positions" in line and "Number of Traders" not in line:
            idx_positions = i
            break

    if idx_positions is None:
        if debug:
            print("[parse_positions_simple] Positions header not found.")
        return None

    # Look for the "All" row after the Positions header
    all_line = None
    for line in lines[idx_positions + 1 :]:
        if not line.strip():
            continue
        if line.strip().startswith("All"):
            all_line = line
            break
    if all_line is None:
        if debug:
            print("[parse_positions_simple] 'All' row not found.")
        return None

    # Parse the All row
    # Example pattern:
    # All  :   159,649:     7,790     30,915     26,794     55,885 ...
    m = re.match(r"\s*All\s*:(.*)$", all_line)
    if not m:
        if debug:
            print("[parse_positions_simple] Could not parse 'All' line.")
        return None

    rest = m.group(1)
    parts = rest.split(":")
    if len(parts) < 2:
        if debug:
            print("[parse_positions_simple] Unexpected 'All' line format.")
        return None

    open_interest_str = parts[0].strip()
    try:
        open_interest = float(open_interest_str.replace(",", ""))
    except ValueError:
        open_interest = float("nan")

    # The rest of the line contains numeric tokens for trader groups
    num_tokens = re.findall(r"[-\d.,]+", ":".join(parts[1:]))
    # Expecting 13 numbers: PM (L,S), SD (L,S,Spread), MM (L,S,Spread),
    #                        OR (L,S,Spread), NR (L,S)
    if len(num_tokens) < 13 and debug:
        print(f"[parse_positions_simple] Warning: expected >=13 tokens, got {len(num_tokens)}")

    def _num(idx: int) -> float:
        if idx >= len(num_tokens):
            return float("nan")
        try:
            return float(num_tokens[idx].replace(",", ""))
        except ValueError:
            return float("nan")

    rows = []

    # 'All' row (total OI)
    rows.append(
        {
            "Category": "All",
            "Open Interest": open_interest,
            "Long": float("nan"),
            "Short": float("nan"),
        }
    )

    # Producer/Merchant/Processor/User
    rows.append(
        {
            "Category": "Producer/Merchant/Processor/User",
            "Open Interest": open_interest,
            "Long": _num(0),
            "Short": _num(1),
        }
    )

    # Swap Dealers
    rows.append(
        {
            "Category": "Swap Dealers",
            "Open Interest": open_interest,
            "Long": _num(2),
            "Short": _num(3),
        }
    )

    # Managed Money
    rows.append(
        {
            "Category": "Managed Money",
            "Open Interest": open_interest,
            "Long": _num(5),
            "Short": _num(6),
        }
    )

    # Other Reportables
    rows.append(
        {
            "Category": "Other Reportables",
            "Open Interest": open_interest,
            "Long": _num(8),
            "Short": _num(9),
        }
    )

    # Nonreportable Positions
    rows.append(
        {
            "Category": "Nonreportable Positions",
            "Open Interest": open_interest,
            "Long": _num(11),
            "Short": _num(12),
        }
    )

    return pd.DataFrame(rows)


def parse_changes_simple(raw_block: str, debug: bool = False) -> pd.DataFrame | None:
    """
    Parse "Changes in Commitments from:" section.

    We only extract the change in open interest (first number in the line
    immediately following the 'Changes in Commitments from:' header).

    Returns:
        DataFrame with a single column:
            - "Change in Open Interest"
    """
    lines = [_strip_line(l) for l in raw_block.splitlines()]

    idx_changes = None
    for i, line in enumerate(lines):
        if "Changes in Commitments from:" in line:
            idx_changes = i
            break

    if idx_changes is None or idx_changes + 1 >= len(lines):
        if debug:
            print("[parse_changes_simple] Changes header not found.")
        return None

    line = lines[idx_changes + 1]
    # First numeric token on the line is change in open interest
    m = re.search(r"([-]?\d[\d,]*)", line)
    if not m:
        if debug:
            print("[parse_changes_simple] No numeric token found for OI change.")
        return None

    try:
        change_oi = float(m.group(1).replace(",", ""))
    except ValueError:
        change_oi = float("nan")

    df = pd.DataFrame(
        [{"Change in Open Interest": change_oi}]
    )
    return df


def parse_percents_simple(raw_block: str, debug: bool = False) -> pd.DataFrame | None:
    """
    Parse "Percent of Open Interest Represented by Each Category of Trader".

    We only use the "All" row, decomposed by trader category.

    Returns:
        DataFrame with one row per Category and columns:
            - "Category"
            - "Long %"
            - "Short %"
    """
    lines = [_strip_line(l) for l in raw_block.splitlines()]

    idx_pct = None
    for i, line in enumerate(lines):
        if "Percent of Open Interest Represented by Each Category of Trader" in line:
            idx_pct = i
            break

    if idx_pct is None:
        if debug:
            print("[parse_percents_simple] Percent section header not found.")
        return None

    # Find 'All' row after this header
    all_line = None
    for line in lines[idx_pct + 1 :]:
        if not line.strip():
            continue
        if line.strip().startswith("All"):
            all_line = line
            break
    if all_line is None:
        if debug:
            print("[parse_percents_simple] 'All' row not found.")
        return None

    m = re.match(r"\s*All\s*:(.*)$", all_line)
    if not m:
        if debug:
            print("[parse_percents_simple] Could not parse 'All' line.")
        return None

    rest = m.group(1)
    parts = rest.split(":")
    if len(parts) < 2:
        if debug:
            print("[parse_percents_simple] Unexpected 'All' line format.")
        return None

    # first numeric token is overall 100.0 (total OI %)
    num_tokens = re.findall(r"[-\d.]+", ":".join(parts[1:]))
    if len(num_tokens) < 13 and debug:
        print(f"[parse_percents_simple] Warning: expected >=13 tokens, got {len(num_tokens)}")

    def _num(idx: int) -> float:
        if idx >= len(num_tokens):
            return float("nan")
        try:
            return float(num_tokens[idx])
        except ValueError:
            return float("nan")

    rows = []

    # Producer/Merchant/Processor/User
    rows.append(
        {
            "Category": "Producer/Merchant/Processor/User",
            "Long %": _num(1),   # index 0 is 100.0 total
            "Short %": _num(2),
        }
    )

    # Swap Dealers
    rows.append(
        {
            "Category": "Swap Dealers",
            "Long %": _num(3),
            "Short %": _num(4),
        }
    )

    # Managed Money
    rows.append(
        {
            "Category": "Managed Money",
            "Long %": _num(6),
            "Short %": _num(7),
        }
    )

    # Other Reportables
    rows.append(
        {
            "Category": "Other Reportables",
            "Long %": _num(9),
            "Short %": _num(10),
        }
    )

    # Nonreportable Positions
    rows.append(
        {
            "Category": "Nonreportable Positions",
            "Long %": _num(12),
            "Short %": _num(13) if len(num_tokens) > 13 else float("nan"),
        }
    )

    return pd.DataFrame(rows)


def parse_traders_simple(raw_block: str, debug: bool = False) -> pd.DataFrame | None:
    """
    Parse "Number of Traders in Each Category".

    Currently optional for the engine. Returns None by default to keep things simple.
    You can expand this if you want trader counts.

    Returns:
        None or DataFrame with trader counts by category.
    """
    if debug:
        print("[parse_traders_simple] Not implemented (optional for engine).")
    return None


def parse_largest_traders_simple(raw_block: str, debug: bool = False) -> pd.DataFrame | None:
    """
    Parse "Percent of Open Interest Held by the Indicated Number of the Largest Traders".

    We only extract the 'All' row, which contains:

        4 or Less Traders Long
        4 or Less Traders Short
        8 or Less Traders Long
        8 or Less Traders Short
        (Net metrics follow; we ignore for now)

    Returns:
        DataFrame with one row:
            - Category = "All"
            - "4 or Less Long %"
            - "4 or Less Short %"
            - "8 or Less Long %"
            - "8 or Less Short %"
    """
    lines = [_strip_line(l) for l in raw_block.splitlines()]

    idx_largest = None
    for i, line in enumerate(lines):
        if "Percent of Open Interest Held by the Indicated Number of the Largest Traders" in line:
            idx_largest = i
            break

    if idx_largest is None:
        if debug:
            print("[parse_largest_traders_simple] Section header not found.")
        return None

    # Find 'All' row after the header
    all_line = None
    for line in lines[idx_largest + 1 :]:
        if not line.strip():
            continue
        if line.strip().startswith("All"):
            all_line = line
            break

    if all_line is None:
        if debug:
            print("[parse_largest_traders_simple] 'All' row not found.")
        return None

    m = re.match(r"\s*All\s*:(.*)$", all_line)
    if not m:
        if debug:
            print("[parse_largest_traders_simple] Could not parse 'All' line.")
        return None

    rest = m.group(1)
    num_tokens = re.findall(r"[-\d.]+", rest)
    if len(num_tokens) < 4 and debug:
        print(f"[parse_largest_traders_simple] Warning: expected >=4 tokens, got {len(num_tokens)}")

    def _num(idx: int) -> float:
        if idx >= len(num_tokens):
            return float("nan")
        try:
            return float(num_tokens[idx])
        except ValueError:
            return float("nan")

    row = {
        "Category": "All",
        "4 or Less Long %": _num(0),
        "4 or Less Short %": _num(1),
        "8 or Less Long %": _num(2),
        "8 or Less Short %": _num(3),
    }

    return pd.DataFrame([row])

