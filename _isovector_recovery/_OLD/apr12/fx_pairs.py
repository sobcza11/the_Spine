from __future__ import annotations

# Governed IsoVector FX universe.
# Add pairs here only when they are supported across:
# 1) governed FX price source
# 2) governed FX spreads source
# 3) downstream UI / diagnostics requirements

FX_UNIVERSE = {
    "AUDUSD",
    "EURUSD",
    "GBPUSD",
    "USDCAD",
    "USDCHF",
    "USDJPY",
}


def get_fx_universe() -> list[str]:
    return sorted(FX_UNIVERSE)