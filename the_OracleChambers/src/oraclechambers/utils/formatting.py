"""
Formatting helpers for macro briefs, risk summaries, and scenario commentary.
"""

from typing import List


def bullet_list(lines: List[str]) -> str:
    lines = [line for line in lines if line and line.strip()]
    if not lines:
        return ""
    return "\n".join(f"- {line}" for line in lines)


def section(title: str, body: str) -> str:
    body = (body or "").strip()
    return f"## {title}\n\n{body}\n" if body else f"## {title}\n\n_(no data)_\n"
