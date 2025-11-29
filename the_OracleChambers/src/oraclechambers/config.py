from pathlib import Path
import os

# Project root = repo root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
PROMPTS_DIR = DATA_DIR / "prompts"
VOCAB_DIR = DATA_DIR / "vocab"

# Location of upstream macro / policy engines
# Override via env vars if needed.
SPINE_DATA_DIR = Path(
    os.getenv("SPINE_DATA_DIR", PROJECT_ROOT.parent / "the_Spine" / "data" / "processed")
)
FEDSPEAK_DATA_DIR = Path(
    os.getenv("FEDSPEAK_DATA_DIR", PROJECT_ROOT.parent / "fedspeak_hknsl" / "data" / "processed")
)

NARRATIVE_SNAPSHOTS_PARQUET = PROCESSED_DIR / "narrative_snapshots.parquet"
MACRO_STATE_BRIEFS_PARQUET = PROCESSED_DIR / "macro_state_briefs.parquet"

for d in [DATA_DIR, PROCESSED_DIR, PROMPTS_DIR, VOCAB_DIR]:
    d.mkdir(parents=True, exist_ok=True)
