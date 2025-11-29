"""
Simple helpers to save narrative text to disk.
"""

from pathlib import Path


def save_markdown(text: str, path: Path) -> None:
    """
    Save markdown text to the given path, creating parent dirs if needed.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")