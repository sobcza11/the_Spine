# scripts/run_beige_features_for_spine.py

"""
Generate Spine-ready features from Beige Book summaries.

Input:
    data/beige_book/*.txt

Output:
    data/beige_book_features_for_spine.json

Each row:
    {
      "beige_id": ...,
      "beige_date": ... (YYYY-MM or YYYY-MM-DD or null),
      "beige_growth_tone": ... in [-1, 1],
      "beige_price_tone": ... in [-1, 1],
      "beige_wage_tone": ... in [-1, 1],
      "beige_uncertainty": ... >= 0
    }
"""

import json
import re
from datetime import date
from pathlib import Path
from typing import Dict, List


def infer_date_from_name(stem: str) -> str | None:
    """
    Try to infer YYYY-MM or YYYY-MM-DD from filenames like 2025-01_beige_book.
    """
    m = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})?", stem)
    if not m:
        return None
    y = int(m.group(1))
    mth = int(m.group(2))
    d_str = m.group(3)
    if d_str:
        d = int(d_str)
        try:
            return date(y, mth, d).isoformat()
        except ValueError:
            return None
    else:
        # fallback to first of month
        try:
            return date(y, mth, 1).isoformat()
        except ValueError:
            return None


def count_hits(text: str, phrases: List[str]) -> int:
    t = text.lower()
    total = 0
    for p in phrases:
        total += t.count(p.lower())
    return total


def signed_tone(text: str, up_terms: List[str], down_terms: List[str]) -> float:
    up = count_hits(text, up_terms)
    down = count_hits(text, down_terms)
    total = up + down
    if total == 0:
        return 0.0
    return (up - down) / total  # in [-1, 1]


def score_beige(text: str) -> Dict[str, float]:
    """
    Simple Beige Book scoring for growth, prices, wages, and uncertainty.
    """

    growth_up = [
        "expanded moderately", "expanded modestly", "grew modestly",
        "growth was strong", "solid growth", "increased", "improved"
    ]
    growth_down = [
        "declined", "contracted", "softened", "weakened",
        "slowed", "stagnated", "flat to down"
    ]

    price_up = [
        "prices increased", "price increases", "elevated price pressures",
        "inflation remains elevated", "strong price pressures"
    ]
    price_down = [
        "price increases slowed", "price pressures eased",
        "disinflation", "slower price growth"
    ]

    wage_up = [
        "wage growth increased", "wage pressures", "wages rose",
        "wage growth remained strong"
    ]
    wage_down = [
        "wage growth moderated", "wage pressures eased",
        "wage growth slowed"
    ]

    uncertainty_terms = [
        "uncertain", "uncertainty", "mixed reports",
        "varied by district", "range of outcomes"
    ]

    growth_tone = signed_tone(text, growth_up, growth_down)
    price_tone = signed_tone(text, price_up, price_down)
    wage_tone = signed_tone(text, wage_up, wage_down)

    tokens = len(text.split()) or 1
    beige_uncertainty = count_hits(text, uncertainty_terms) / tokens

    return {
        "beige_growth_tone": float(growth_tone),
        "beige_price_tone": float(price_tone),
        "beige_wage_tone": float(wage_tone),
        "beige_uncertainty": float(beige_uncertainty),
    }


def main() -> None:
    base_dir = Path("data")
    beige_dir = base_dir / "beige_book"
    out_path = base_dir / "beige_book_features_for_spine.json"

    if not beige_dir.exists():
        print(f"! Beige directory not found: {beige_dir}")
        return

    files = sorted(beige_dir.glob("*.txt"))
    if not files:
        print(f"! No Beige .txt files found in {beige_dir}")
        return

    print(f"Found {len(files)} Beige summaries.")
    rows: List[Dict] = []

    for p in files:
        print(f"Processing: {p}")
        text = p.read_text(encoding="utf-8", errors="ignore")

        beige_id = p.stem
        beige_date = infer_date_from_name(beige_id)

        scores = score_beige(text)

        row = {
            "beige_id": beige_id,
            "beige_date": beige_date,
        }
        row.update(scores)
        rows.append(row)

    out_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"\nExported {len(rows)} Beige summaries to {out_path}")


if __name__ == "__main__":
    main()

