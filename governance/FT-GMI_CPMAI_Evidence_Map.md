# CPMAI Evidence Map
FT-GMI (Fourth Turning · Global Macro-Stability Index)

System Family: the_Spine → FT-GMI  
Prepared by: Rand Sobczak Jr  
Standard: PMI – CPMAI  
Purpose: Evidence traceability for audit, SCI review, & sponsor safety  
Date: 2026-01-07  
Status: Diagnostic Model (Non-Production)

---

## 1) Why This Matters (Scorecard — SCI Included)

| Audience | What They Evaluate | What This Evidence Map Demonstrates | Score (0–10) |
|---|---|---|---:|
| Pitt SCI (Capstone) | reproducibility, scope control, rigor | pinned env, lifecycle clarity, explicit gaps | **8.5** |
| Pitt Business / Katz | sponsor safety, licensing discipline | non-production stance, vendor boundaries | **8.0** |
| CPMAI Audit Lens | governance & lifecycle traceability | controls mapped to concrete artifacts | **8.5** |

This evidence map allows reviewers to validate governance, reproducibility, & lifecycle discipline **directly from repository artifacts**, without interpretation or narrative dependency.

---

## 2) Evidence Inventory (Paths Only)
List the strongest artifacts first. Use repo-relative paths.

### Governance
- governance/FT-GMI_CPMAI_Audit.md
- governance/FT-GMI_CPMAI_Evidence_Map.md

### Architecture & System Orientation
- spine_skeleton_public.txt
- (add) README(s) that explain system boundaries & non-forecasting posture

### Data Lineage & Sources
- data/ (describe subtrees that matter)
- (add) data dictionaries, source manifests, pull scripts, vendor terms notes

### Reproducibility
- requirements.core.txt
- requirements.core.lock.txt
- requirements.ci.txt
- .github/workflows/ (CI provenance)

### Model Definition (FT-GMI)
- src/ft_gmi/ (canonical FT-GMI implementation within the_Spine)
- FT-GMI/src/ft_gmi/ (packaging mirror; not audit-authoritative)
- (add) any scoring formulas, gates, regime definitions, diagnostic metrics

### Testing & Validation
- (add) tests/ if exists
- (add) sanity checks, smoke runs, seed controls, evaluation scripts

### Risk, Limits, & “What This Is Not”
- governance/FT-GMI_CPMAI_Audit.md (scope exclusions section)
- (add) disclaimers in reports/ & slides if present

### Change Control
- git log (implicit)
- (add) freeze manifests, version tags, changelog if present

---

## 3) CPMAI Controls → Evidence (Traceability Table)

| CPMAI Control Area | What Auditor Verifies | Primary Evidence (repo paths) | Notes |
|---|---|---|---|
| Scope & Intended Use | Non-forecasting diagnostic posture, correct framing | governance/FT-GMI_CPMAI_Audit.md | Sponsor-safe stance |
| Data Governance | Sources, lineage, transformations, storage controls | (fill) | Include vendor constraints |
| Reproducibility | Environment pinning & deterministic rebuild | requirements.core.txt; requirements.core.lock.txt | Lockfile required |
| Lifecycle Discipline | Research vs production separation | governance/FT-GMI_CPMAI_Audit.md | Status = non-production |
| Monitoring & Drift | If applicable: what is monitored & how | (fill) | Can be “not applicable yet” |
| Security & Privacy | Secrets handling & access control | (fill) | .env excluded, key mgmt rules |
| Testing & Validation | Basic correctness, sanity, & regression posture | (fill) | Tests can be lightweight |
| Change Control | Versioning, review gates, approvals | (fill) | PR flow, tags, freeze manifests |

---

## 4) Gaps (Explicit)
List what does NOT exist yet but is planned.

- [ ] Data Source Manifest (FT-GMI)
- [ ] Freeze Manifest for first sponsor review tag
- [x] Minimal tests (smoke) for FT-GMI pipeline
- [ ] Secrets policy (& key injection pattern)

---

## 5) Next Actions (48-hour)
- Build Data Source Manifest
- Add Freeze Manifest & tag a sponsor-safe baseline
- Add 3 smoke tests (load, compute, output schema)

## 6) Lane Closure Evidence (Commit Anchors)

- 2026-01-07 — Lane A closed: CI workflows standardized, duplicate workflows removed, repo hygiene enforced  
  Commit: d9ed234

- 2026-01-07 — Lane B closed: Canonical FT-GMI Pillar 1 scaffold materialized; smoke tests passing; non-predictive  
  Commits: 11a24ce, 7396509
