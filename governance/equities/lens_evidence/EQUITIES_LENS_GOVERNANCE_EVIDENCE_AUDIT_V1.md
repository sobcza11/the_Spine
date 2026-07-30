# EQUITIES Lens Governance Evidence Audit V1

## Audit purpose

The audit combines each lens input-contract status, governance-policy status,
and evidence-binding status into one deterministic readiness decision for:

- Market Breadth
- Volatility Structure
- Liquidity / Flow

It performs governance validation only. It does not calculate, approve, or
activate a lens.

## Fail-closed behavior

The auditor returns `BLOCKED` if any contract, policy, or evidence binding is
missing, malformed, incomplete, inconsistent with `VALIDATION_ONLY`, or does
not retain `PROHIBITED` activation.

An incomplete component cannot be offset by another complete component.

## No-inference policy

The audit does not infer thresholds, date ranges, frequencies, calendars,
missing-data handling, universe membership, scope, approvals, or evidence from
QQQ availability, SPY, legacy artifacts, or analytical code.

## Future evidence approval workflow

1. The responsible lens methodology owner supplies the required evidence.
2. Evidence is bound to a versioned source and approval reference.
3. Independent governance review validates provenance, scope, effective dates,
   and deterministic handling.
4. The corresponding policy and evidence status may be proposed for `READY`.
5. The auditor is rerun under `VALIDATION_ONLY`.
6. Lens activation requires a separate explicit authorization and is outside
   this audit.

## Current state

All three lenses remain:

```text
contract_status: CONTRACT_INCOMPLETE
policy_status: POLICY_INCOMPLETE
evidence_status: EVIDENCE_INCOMPLETE
overall_status: BLOCKED
runtime_state: VALIDATION_ONLY
activation_state: PROHIBITED
```
