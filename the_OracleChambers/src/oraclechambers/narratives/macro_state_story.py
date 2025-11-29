"""
Macro State Story v0

Uses the latest macro_state_spine_us snapshot plus the Inflation Lens
to generate a simple macro brief.
"""
from pathlib import Path
from ..config import PROCESSED_DIR
from ..exporters.to_markdown import save_markdown

from typing import Optional
import pandas as pd
from datetime import datetime, timezone

from ..registry import build_context
from ..lenses.inflation_lens import interpret_inflation
from ..utils.time_windows import latest_row
from ..utils.formatting import section, bullet_list


def build_macro_brief(df_macro: Optional[pd.DataFrame]) -> str:
    """
    Turn the latest macro_state_spine_us row into a markdown brief.
    """
    latest = latest_row(df_macro, date_col="date")
    infl = interpret_inflation(latest)

    if isinstance(latest, pd.Series) and "date" in latest.index:
        date_str = pd.to_datetime(latest["date"]).strftime("%Y-%m-%d")
    else:
        # Fallback: use current UTC date
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Core sections
    intro_body = (
        f"As of {date_str}, the macro engine (the_Spine) reports the most recent "
        f"macro state and associated inflation/energy signals."
    )

    inflation_body = bullet_list(
        [
            infl.get("headline"),
            infl.get("inflation_state"),
            infl.get("energy_state"),
        ]
    )

    risks_body = infl.get("risk_flags", "")

    parts = [
        f"# Macro State Brief – {date_str}\n",
        section("Overview", intro_body),
        section("Inflation & Energy Lens", inflation_body),
        section("Key Inflation Risk Flags", risks_body),
    ]

    return "\n".join(parts)


def generate_and_save_latest_macro_brief() -> Path:
    """
    Build the latest macro brief and save it as a markdown file under data/processed/.

    Returns the Path to the saved file.
    """
    ctx = build_context()
    text = build_macro_brief(ctx.spine.macro_state_spine_us)

    # Use today's date for filename
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = PROCESSED_DIR / f"macro_state_brief_{today_str}.md"
    save_markdown(text, out_path)
    return out_path

if __name__ == "__main__":
    path = generate_and_save_latest_macro_brief()
    print(f"# Saved macro brief to: {path}")


