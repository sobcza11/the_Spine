# FT-GMI — Freeze Manifest (Sponsor Review Baseline)
(Fourth Turning · Global Macro-Stability Index)

System Family: the_Spine → FT-GMI  
Prepared by: Rand Sobczak Jr  
Standard: PMI – CPMAI  
Purpose: Immutable baseline for SCI review & Pitt Business sponsor safety  
Status: Diagnostic Model (Non-Production)  
Freeze Version: v1.0  
Freeze Date: 2026-01-07  

---

## 1) Freeze Intent (What This Is)
This freeze defines a sponsor-safe baseline for FT-GMI review.

This baseline:
- supports governance review, reproducibility review, & scope review
- is not a production release
- is not a forecasting or trading system

---

## 2) Baseline Identity
- Repo: the_Spine
- Branch: main
- Commit SHA: <FILL AFTER COMMIT>
- Tag: ft-gmi_freeze_v1 (planned)

---

## 3) In-Scope Components (Authoritative)
- `src/ft_gmi/` (canonical implementation)

Governance artifacts:
- `governance/FT-GMI_CPMAI_Audit.md`
- `governance/FT-GMI_CPMAI_Evidence_Map.md`
- `governance/FT-GMI_Data_Source_Manifest.md`

Reproducibility:
- `requirements.core.txt`
- `requirements.core.lock.txt`
- `.github/workflows/` (provenance & scheduled runs, where applicable)

---

## 4) Out of Scope (Explicit)
- production deployment
- forecasts, signals, recommendations
- WRDS/CRSP/Compustat integration
- redistribution of licensed datasets
- any Yahoo Finance dependency
- personal credentials or secrets (.env)

---

## 5) Data Posture
Data sources are declared in:
- `governance/FT-GMI_Data_Source_Manifest.md`

Rules:
- secrets via env vars only
- no credentials committed
- derived artifacts permitted; raw licensed redistribution prohibited

---

## 6) Known Gaps (Declared)
- minimal smoke tests not yet formalized (planned)
- automated freshness checks not yet formalized (planned)

---

## 7) Sponsor Review Checklist (Pass/Fail)
- [ ] Scope is non-production & diagnostic
- [ ] Evidence map points to real repo artifacts
- [ ] Data sources declared & exclusions explicit
- [ ] Reproducible environment is pinned (core + lock)
- [ ] No secrets committed

