import re
from datetime import datetime, date, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup

FED_RATE_MONITOR_URL = "https://www.investing.com/central-banks/fed-rate-monitor"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

LEAF_GROUP = "US_Rates"
LEAF_NAME = "Fed_Rate_Monitor"


# ---------------------------------------------------------------------
# Helpers for dates and timestamps
# ---------------------------------------------------------------------
def _extract_first_date(text: str) -> date | None:
    """Grab first 'Mon DD, YYYY' pattern from a block of text."""
    m = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}",
        text,
    )
    if not m:
        return None
    return datetime.strptime(m.group(0), "%b %d, %Y").date()


def _extract_updated_ts(text: str) -> datetime | None:
    """
    Grab 'Updated: Mon DD, YYYY HH:MMAM EST' pattern if present.
    Fallback: None (we still have as_of_date).
    """
    m = re.search(
        r"Updated:\s+"
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}[AP]M\s+EST",
        text,
    )
    if not m:
        return None
    ts_str = m.group(0).replace("Updated:", "").strip()
    # treat as naive Eastern for now
    return datetime.strptime(ts_str, "%b %d, %Y %I:%M%p EST")


# ---------------------------------------------------------------------
# Helpers for probability parsing
# ---------------------------------------------------------------------
def _extract_last_percent(val: str) -> str | None:
    """
    Extract the LAST percentage pattern from any messy string.

    Examples:
        "28.5% 43.9%" -> "43.9%"
        "59.9% 62.9%" -> "62.9%"
        "43.9%  ▲"    -> "43.9%"
    """
    if val is None:
        return None

    matches = re.findall(r"\d+(?:\.\d+)?%", str(val))
    return matches[-1] if matches else None


def _clean_prob_col(series: pd.Series) -> pd.Series:
    """
    Convert probability strings like '44.4%' to floats in [0, 1].
    """
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace({"": None, "None": None})
        .astype(float)
        / 100.0
    )


# ---------------------------------------------------------------------
# Core HTML → DataFrame parser for one meeting block
# ---------------------------------------------------------------------
def _parse_meeting_block(div, section_index: int) -> pd.DataFrame:
    """
    Parse a single search_section_X div into a normalized DataFrame.

    section_index = 1,2,3,... corresponding to search_section_0,1,2...
    """
    text = div.get_text(" ", strip=True)
    meeting_date = _extract_first_date(text)
    updated_at = _extract_updated_ts(text)

    table = div.find("table", class_="genTbl openTbl fedRateTbl")
    if table is None:
        return pd.DataFrame()

    body = table.find("tbody") or table

    rows = []
    row_num = 0
    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue

        row_num += 1

        target_band_text = cells[0].get_text(" ", strip=True)
        current_text = cells[1].get_text(" ", strip=True)
        prev_day_text = cells[2].get_text(" ", strip=True)
        prev_week_text = cells[3].get_text(" ", strip=True)

        rows.append(
            {
                "row_num": row_num,
                "target_rate_band": target_band_text,
                "prob_current_raw": current_text,
                "prob_prev_day_raw": prev_day_text,
                "prob_prev_week_raw": prev_week_text,
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Clean probability columns (raw → last % → float 0–1)
    for raw_col, clean_col in [
        ("prob_current_raw", "prob_current"),
        ("prob_prev_day_raw", "prob_prev_day"),
        ("prob_prev_week_raw", "prob_prev_week"),
    ]:
        df[clean_col] = df[raw_col].apply(_extract_last_percent)
        df[clean_col] = _clean_prob_col(df[clean_col])

    # Split target band "3.50 - 3.75" into numeric bounds if possible
    band_split = df["target_rate_band"].str.split("-", n=1, expand=True)
    df["target_lower"] = (
        band_split[0].str.strip().replace({"": None}).astype(float)
        if 0 in band_split
        else None
    )
    df["target_upper"] = (
        band_split[1].str.strip().replace({"": None}).astype(float)
        if 1 in band_split
        else None
    )

    df["meeting_date"] = meeting_date
    df["updated_at"] = updated_at
    df["as_of_date"] = datetime.now(timezone.utc).date()

    df["leaf_group"] = LEAF_GROUP
    df["leaf_name"] = LEAF_NAME

    # tag which search_section this came from
    df["section_index"] = section_index  # 1,2,3,...

    # Final column order for R2 / Spine
    df = df[
        [
            "as_of_date",
            "section_index",
            "row_num",
            "meeting_date",
            "target_rate_band",
            "target_lower",
            "target_upper",
            "prob_current_raw",
            "prob_prev_day_raw",
            "prob_prev_week_raw",
            "prob_current",
            "prob_prev_day",
            "prob_prev_week",
            "updated_at",
            "leaf_group",
            "leaf_name",
        ]
    ]
    return df


# ---------------------------------------------------------------------
# Public builder (full leaf)
# ---------------------------------------------------------------------
def build_fed_rate_monitor_leaf(max_sections: int = 3) -> pd.DataFrame:
    """
    Core builder: fetch page, parse top N meeting sections into a long DF.
    """
    resp = requests.get(FED_RATE_MONITOR_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    frames = []
    for i in range(max_sections):
        div = soup.find("div", id=f"search_section_{i}")
        if div is None:
            continue
        # section_index = 1,2,3,...
        frames.append(_parse_meeting_block(div, section_index=i + 1))

    if not frames:
        return pd.DataFrame(
            columns=[
                "as_of_date",
                "section_index",
                "row_num",
                "meeting_date",
                "target_rate_band",
                "target_lower",
                "target_upper",
                "prob_current_raw",
                "prob_prev_day_raw",
                "prob_prev_week_raw",
                "prob_current",
                "prob_prev_day",
                "prob_prev_week",
                "updated_at",
                "leaf_group",
                "leaf_name",
            ]
        )

    df = pd.concat(frames, ignore_index=True)

    df = df.sort_values(
        ["as_of_date", "meeting_date", "section_index", "row_num"],
        ascending=[True, True, True, True],
    ).reset_index(drop=True)

    return df


def build_us_fedmonitor_leaf(max_sections: int = 3) -> pd.DataFrame:
    """
    Spine-standard wrapper name for this leaf.
    """
    return build_fed_rate_monitor_leaf(max_sections=max_sections)


# ---------------------------------------------------------------------
# R2 writers: full history + front-2 tracker
# (local file versions only; R2 write handled by CLI via write_parquet_to_r2)
# ---------------------------------------------------------------------
def update_front2_tracker_parquet(tracker_path: str, max_sections: int = 3) -> None:
    """
    Maintain a small tracking parquet for ONLY the nearest 2 Fed meetings.
    """
    new_df = build_fed_rate_monitor_leaf(max_sections=max_sections)
    if new_df.empty:
        return

    # Find the 2 earliest meeting dates
    valid_dates = sorted(new_df["meeting_date"].dropna().unique())
    if not valid_dates:
        return

    top_two_dates = valid_dates[:2]
    new_df = new_df[new_df["meeting_date"].isin(top_two_dates)].copy()

    # Load existing tracker parquet if present
    try:
        existing = pd.read_parquet(tracker_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    except FileNotFoundError:
        combined = new_df

    combined = (
        combined.sort_values(
            ["as_of_date", "meeting_date", "target_rate_band", "updated_at"],
            na_position="last",
        )
        .drop_duplicates(
            subset=["as_of_date", "meeting_date", "target_rate_band"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    combined.to_parquet(tracker_path, index=False)
