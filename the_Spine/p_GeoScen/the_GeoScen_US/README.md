# GeoScen™ US — Geographic Scent Module (Skeleton)

## Mission

GeoScen™ is a geography-aware macro leaf for *the*_Spine.

It is designed to capture place-based economic dynamics such as:

- migration pressure (inflows / outflows)
- housing turnover and burden
- sectoral labor import / export
- demographic shifts
- regional macro "heat" or "scent"

In later phases, GeoScen is intended to incorporate **global** data back to
the 1950s, including demographic and migration series for advanced economies
and key emerging markets.

## CPMAI Phase Status

- Phase I–II (Business & Data Understanding): **Concept complete**
- Phase III (Data Preparation): **Skeleton only** — no binding to specific
  historical datasets yet.
- Phase IV (Modeling): **Interfaces defined** (GeoScenLeafRow, geo_scen_leaf).
- Phase V (Evaluation): **To be defined once data sources are integrated.**
- Phase VI (Deployment & Governance): **To be defined.**

## Current Implementation

- `config.py`
  - Defines `GeoScenConfig` and `default_geoscen_config()`
  - Documents supported regions and leaf location.

- `geo_scen_leaf_template.py`
  - Defines the GeoScen leaf schema (indices + composite).
  - Provides a neutral placeholder leaf so MAIN_p can consume a GeoScen signal.
  - Offers an extension point for future demographic / migration / housing
    datasets without changing the external interface.

## Future Growth (1950+ Global Ambition)

This module is explicitly designed to scale to:

- long-history demographic and migration series (1950+)
- regional housing and wage data
- global AE/EM expansions via additional GeoScen pipes

The goal is to turn GeoScen into a **structural driver** of inflation and
growth asymmetry inside MAIN_p, and a key input into *the*_OracleChambers
GeoScen narratives.

