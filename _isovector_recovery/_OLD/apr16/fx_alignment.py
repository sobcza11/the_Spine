from __future__ import annotations


def align_fx_universe(
    price_symbols: list[str] | set[str],
    spread_pairs: list[str] | set[str],
    registry: list[str] | set[str],
) -> list[str]:
    price_set = {str(x).upper().strip() for x in price_symbols if str(x).strip()}
    spread_set = {str(x).upper().strip() for x in spread_pairs if str(x).strip()}
    registry_set = {str(x).upper().strip() for x in registry if str(x).strip()}

    return sorted(price_set & spread_set & registry_set)