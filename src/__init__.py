"""
Spine jobs package.

This package contains all deterministic ingestion, validation,
and derivation jobs that form the measurement layer of the_Spine.

Structure

spine.jobs
├─ fx
├─ rates
├─ energy
├─ equity
└─ ...

Each domain contains its own governed job modules.
"""

__all__ = [
    "fx",
    "rates",
    "energy",
    "equity",
]
