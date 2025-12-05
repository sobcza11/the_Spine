# scripts/run_statements_features_for_spine.py

"""
Generate Spine-ready features from FOMC policy statements.

Input:
    data/fomc_statements/*.txt

Output:
    data/fomc_statements_features_for_spine.json

Each row:
    {
      "statement_id": ...,
      "statement_date": ... (YYYY-MM-DD or null),
      "fed_stmt_policy_bias": ... in [-1, 1],
      "fed_stmt_inflation_focus": ... >= 0,
      "fed_stmt_growth_focus": ... >= 0,
      "fed_stmt_uncertainty": ... >= 0
    }
"""

import json
import re
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple


def infer_date_from_name(stem: str) -> str | None:
    """
    Try to infer a YYYY-MM-DD date from a filename stem.
    e.g. monetary20250319a_statement -> 2025-03-19
    """
    m = re.search(r"(\d{4})(\d{2})(\d{2})", stem)
    if not m:
        return None
    y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(y, mth, d).isoformat()
    except ValueError:
        return None


def count_hits(text: str, phrases: List[str]) -> int:
    """Count approximate occurrences of any phrase in text."""
    t = text.lower()
    total = 0
    for p in phrases:
        total += t.count(p.lower())
    return total


def score_statement(text: str) -> Dict[str, float]:
    """
    Simple lexicon-based scoring for policy bias, inflation/growth focus, and uncertainty.
    """

    # Very rough, interpretable keyword buckets
    hawkish_terms = [
        "elevated inflation", "inflation remains elevated",
        "upside risks to inflation", "strong demand", "tight labor market",
        "further tightening", "restrictive", "higher for longer",
        "additional policy firming", "inflation pressures"
    ]
    dovish_terms = [
        "slowing", "softening", "weaker", "moderating",
        "downside risks to growth", "easing of policy",
        "less restrictive", "accommodative", "labor market conditions have cooled"
    ]

    inflation_terms = [
        "inflation", "price pressures", "price stability",
        "core inflation", "headline inflation"
    ]
    growth_terms = [
        "economic activity", "real gdp", "output",
        "growth", "business investment", "household spending"
    ]
    uncertainty_terms = [
        "uncertain", "uncertainty", "range of views",
        "risks are highly uncertain", "elevated uncertainty"
    ]

    hawk = count_hits(text, hawkish_terms)
    dove = count_hits(text, dovish_terms)
    polar_total = hawk + dove

    if polar_total == 0:
        policy_bias = 0.0
    else:
        policy_bias = (hawk - dove) / polar_total  # in [-1, 1]

    tokens = len(text.split()) or 1

    inflation_focus = count_hits(text, inflation_terms) / tokens
    growth_focus = count_hits(text, growth_terms) / tokens
    uncertainty = count_hits(text, uncertainty_terms) / tokens

    return {
        "fed_stmt_policy_bias": float(policy_bias),
        "fed_stmt_inflation_focus": float(inflation_focus),
        "fed_stmt_growth_focus": float(growth_focus),
        "fed_stmt_uncertainty": float(uncertainty),
    }


def main() -> None:
    base_dir = Path("data")
    stmts_dir = base_dir / "fomc_statements"
    out_path = base_dir / "fomc_statements_features_for_spine.json"

    if not stmts_dir.exists():
        print(f"! Statements directory not found: {stmts_dir}")
        return

    files = sorted(stmts_dir.glob("*.txt"))
    if not files:
        print(f"! No statement .txt files found in {stmts_dir}")
        return

    print(f"Found {len(files)} statement files.")
    rows: List[Dict] = []

    for p in files:
        print(f"Processing: {p}")
        text = p.read_text(encoding="utf-8", errors="ignore")

        stmt_id = p.stem
        stmt_date = infer_date_from_name(stmt_id)

        scores = score_statement(text)

        row = {
            "statement_id": stmt_id,
            "statement_date": stmt_date,
        }
        row.update(scores)
        rows.append(row)

    out_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"\nExported {len(rows)} statements to {out_path}")


if __name__ == "__main__":
    main()

