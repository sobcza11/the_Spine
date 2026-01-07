# FT-GMI — Data Source Manifest
(Fourth Turning · Global Macro-Stability Index)

System Family: the_Spine → FT-GMI  
Prepared by: Rand Sobczak Jr  
Standard: PMI – CPMAI  
Purpose: Data lineage & licensing transparency for audit & sponsor review  
Date: 2026-01-07  
Status: Diagnostic Model (Non-Production)

---

## 1) Purpose & Boundaries

This manifest enumerates **all external data sources referenced by FT-GMI**, their role in the system, and explicit usage constraints.

FT-GMI:
- is a **diagnostic macro-stability model**
- does **not** produce forecasts, trading signals, or recommendations
- does **not** redistribute licensed data
- does **not** operate in production

---

## 2) Authoritative Execution Scope

Canonical implementation:
- `src/ft_gmi/`

This manifest applies **only** to FT-GMI components within the_Spine.

---

## 3) Data Sources (Declared)

| Source | Data Type | Usage in FT-GMI | Storage | Licensing / Notes |
|---|---|---|---|---|
| FRED (St. Louis Fed) | macro time series | regime diagnostics, normalization | local cache (non-redistributed) | public-use |
| BLS | inflation, labor indicators | macro stability context | local cache | public-use |
| BEA | GDP, income aggregates | structural regime inputs | local cache | public-use |
| CFTC (COT) | positioning aggregates | stress & leverage diagnostics | parquet (derived only) | public-use |
| EIA | energy supply/demand | inflation pressure context | local cache | public-use |

> **Note:** All datasets are used in **derived, aggregated, or normalized form only**.

---

## 4) Explicit Exclusions

FT-GMI does **not**:
- use proprietary market price feeds
- depend on WRDS, CRSP, Compustat, or Bloomberg
- ingest client data or PII
- store raw licensed datasets for redistribution
- No market price feeds are used directly; all indicators are macroeconomic or aggregated positioning data.

---

## 5) Data Handling Rules

- All pulls are reproducible via pinned environments
- Secrets (API keys) are injected via environment variables only
- No credentials are committed to version control
- Derived artifacts may be stored; raw licensed data is not redistributed

---

## 6) Known Gaps (Declared)

- Formal vendor terms appendix (if sponsor requires)
- Automated data freshness checks (non-production)

---

## 7) Audit Mapping

| CPMAI Control | Evidence |
|---|---|
| Data Governance | This manifest |
| Licensing Discipline | Sections 3–5 |
| Scope Control | Sections 1 & 4 |
| Reproducibility | requirements.core.lock.txt |

