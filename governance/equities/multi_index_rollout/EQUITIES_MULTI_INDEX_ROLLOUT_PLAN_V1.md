# EQUITIES Multi-Index Rollout Plan V1

## Objective

Extend the governed common-index architecture beyond the completed QQQ
reference slice through explicit, independently authorized phases. This plan
does not authorize acquisition, publication, analytical calculation, or lens
activation.

## Current Architecture Readiness

The shared profile registry, collision-safe paths, canonicalizer, provenance
metadata, transaction-safe canonical and serving publication, governance
contracts, and fail-closed validation tooling are available.

QQQ is the validation reference through SHADOW serving. Its lens eligibility
remains blocked pending human governance decisions. SPY, DIA, IWM, MDY, and
ITOT have governed profiles but no acquisition authorization in their current
profiles.

Runtime remains `VALIDATION_ONLY`; activation remains `PROHIBITED`.

## Index Universe

### SPY

- Identity: SPY / Tiingo `DAILY_EOD_PRICES` / ETF index proxy
- Expected role: US large-cap market proxy
- Dependencies: common profile validation, explicit authorization, isolated
  acquisition, canonical and serving lineage
- Authorization requirement: `AUTHORIZATION_REQUIRED`
- Risk: legacy SPY data or behavior must not be treated as authorization for
  the profile-controlled pipeline

### QQQ

- Identity: QQQ / Tiingo `DAILY_EOD_PRICES` / ETF index proxy
- Expected role: US large-cap growth and concentration proxy
- Dependencies: completed governed vertical slice; pending lens governance
- Authorization requirement: authorized only by
  `QQQ_TIINGO_DAILY_EOD_AUTHORIZATION_V1`
- Risk: serving availability must not be interpreted as lens approval

### DIA

- Identity: DIA / Tiingo `DAILY_EOD_PRICES` / ETF index proxy
- Expected role: US blue-chip price-weighted proxy
- Dependencies: profile validation followed by explicit authorization
- Authorization requirement: `AUTHORIZATION_REQUIRED`
- Risk: price-weighted interpretation and corporate-action lineage must remain
  distinct from other index methodologies

### IWM

- Identity: IWM / Tiingo `DAILY_EOD_PRICES` / ETF index proxy
- Expected role: US small-cap proxy
- Dependencies: profile validation followed by explicit authorization
- Authorization requirement: `AUTHORIZATION_REQUIRED`
- Risk: small-cap coverage cannot be inferred from large-cap reference data

### MDY

- Identity: MDY / Tiingo `DAILY_EOD_PRICES` / ETF index proxy
- Expected role: US mid-cap proxy
- Dependencies: profile validation followed by explicit authorization
- Authorization requirement: `AUTHORIZATION_REQUIRED`
- Risk: mid-cap scope and history must be independently evidenced

### ITOT

- Identity: ITOT / Tiingo `DAILY_EOD_PRICES` / ETF index proxy
- Expected role: total US equity-market proxy
- Dependencies: profile validation followed by explicit authorization
- Authorization requirement: `AUTHORIZATION_REQUIRED`
- Risk: total-market role must not substitute for a governed constituent
  universe or breadth methodology

## Rollout Sequence

1. **Phase 1 — QQQ validation reference:** preserve QQQ as the completed,
   non-production reference implementation.
2. **Phase 2 — DIA/IWM/MDY/ITOT profile validation:** revalidate identities,
   shared implementation references, and collision-safe destinations. This
   phase does not authorize data access. SPY remains separately gated by its
   existing `AUTHORIZATION_REQUIRED` profile state.
3. **Phase 3 — Authorization:** obtain separate, bounded human authorization
   for each intended instrument. No authorization is inherited from QQQ, SPY,
   or another index.
4. **Phase 4 — Acquisition:** execute only the exact approved provider,
   dataset, range, request budget, and retry policy for one instrument at a
   time.
5. **Phase 5 — Canonicalization:** validate raw evidence and publish the
   profile-controlled canonical artifact/metadata pair transactionally.
6. **Phase 6 — Serving:** validate canonical lineage and publish only the
   collision-safe SHADOW serving pair.
7. **Phase 7 — Lens eligibility:** evaluate governance eligibility separately;
   never infer approval or activate a lens.

Every phase fails closed and requires the prior phase to be complete.

<!-- MULTI_INDEX_ROLLOUT_JSON_V1 -->
```json
{
  "plan_id": "EQUITIES_MULTI_INDEX_ROLLOUT_PLAN_V1",
  "plan_version": "1.0.0",
  "runtime_state": "VALIDATION_ONLY",
  "activation_state": "PROHIBITED",
  "index_order": ["SPY", "QQQ", "DIA", "IWM", "MDY", "ITOT"],
  "indexes": [
    {"instrument_id": "SPY", "provider": "TIINGO", "dataset": "DAILY_EOD_PRICES", "expected_role": "US_LARGE_CAP_MARKET_PROXY", "authorization_status": "AUTHORIZATION_REQUIRED", "authorization_reference": null},
    {"instrument_id": "QQQ", "provider": "TIINGO", "dataset": "DAILY_EOD_PRICES", "expected_role": "US_LARGE_CAP_GROWTH_PROXY", "authorization_status": "AUTHORIZED", "authorization_reference": "QQQ_TIINGO_DAILY_EOD_AUTHORIZATION_V1"},
    {"instrument_id": "DIA", "provider": "TIINGO", "dataset": "DAILY_EOD_PRICES", "expected_role": "US_BLUE_CHIP_PRICE_WEIGHTED_PROXY", "authorization_status": "AUTHORIZATION_REQUIRED", "authorization_reference": null},
    {"instrument_id": "IWM", "provider": "TIINGO", "dataset": "DAILY_EOD_PRICES", "expected_role": "US_SMALL_CAP_PROXY", "authorization_status": "AUTHORIZATION_REQUIRED", "authorization_reference": null},
    {"instrument_id": "MDY", "provider": "TIINGO", "dataset": "DAILY_EOD_PRICES", "expected_role": "US_MID_CAP_PROXY", "authorization_status": "AUTHORIZATION_REQUIRED", "authorization_reference": null},
    {"instrument_id": "ITOT", "provider": "TIINGO", "dataset": "DAILY_EOD_PRICES", "expected_role": "US_TOTAL_MARKET_PROXY", "authorization_status": "AUTHORIZATION_REQUIRED", "authorization_reference": null}
  ],
  "phases": [
    {"phase": 1, "name": "QQQ_VALIDATION_REFERENCE", "status": "COMPLETE"},
    {"phase": 2, "name": "DIA_IWM_MDY_ITOT_PROFILE_VALIDATION", "status": "PENDING"},
    {"phase": 3, "name": "AUTHORIZATION", "status": "PENDING"},
    {"phase": 4, "name": "ACQUISITION", "status": "PENDING"},
    {"phase": 5, "name": "CANONICALIZATION", "status": "PENDING"},
    {"phase": 6, "name": "SERVING", "status": "PENDING"},
    {"phase": 7, "name": "LENS_ELIGIBILITY", "status": "PENDING"}
  ]
}
```
