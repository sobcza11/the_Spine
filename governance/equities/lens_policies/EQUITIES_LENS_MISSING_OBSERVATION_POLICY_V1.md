# EQUITIES Lens Missing Observation Policy V1

## Purpose

Establish the governance structure required for deterministic treatment of
incomplete analytical inputs. This document does not select handling rules.

Each lens owner must explicitly govern missing trading days, missing symbols,
partial universe coverage, stale observations, null values, and duplicate
observations. Until then, every category fails closed.

## Enforcement

- Policy versioning is mandatory.
- Handling must be deterministic and evidence-backed.
- Runtime is `VALIDATION_ONLY`.
- Activation is `PROHIBITED`.
- Pending handling cannot be interpreted as imputation, deletion, carry
  forward, or acceptance.

<!-- LENS_POLICY_JSON_V1 -->
```json
{
  "policy_id": "EQUITIES_LENS_MISSING_OBSERVATION_POLICY_V1",
  "policy_version": "1.0.0",
  "policy_status": "POLICY_INCOMPLETE",
  "deterministic_handling_required": true,
  "fail_closed": true,
  "runtime_state": "VALIDATION_ONLY",
  "activation_state": "PROHIBITED",
  "handling": {
    "missing_trading_days": "PENDING",
    "missing_symbols": "PENDING",
    "partial_universe_coverage": "PENDING",
    "stale_observations": "PENDING",
    "null_values": "PENDING",
    "duplicate_observations": "PENDING"
  }
}
```
