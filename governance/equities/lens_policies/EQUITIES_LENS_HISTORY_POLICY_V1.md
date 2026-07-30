# EQUITIES Lens History Policy V1

## Purpose and ownership

Each EQUITIES lens methodology owner must govern and evidence its own history
requirement before readiness can become complete. Requirements may not be
inferred from available data, inherited from SPY, or inherited from legacy
artifacts.

The policy structure requires a lens identifier, policy version, requirement
status, minimum observations, minimum date range, measurement frequency,
evidence reference, runtime state, and activation state. Unsupported numeric
or temporal thresholds remain unset.

## Enforcement

- Runtime is limited to `VALIDATION_ONLY`.
- Activation remains `PROHIBITED`.
- A missing threshold or evidence reference fails closed.
- Availability of more observations does not establish sufficiency.

<!-- LENS_POLICY_JSON_V1 -->
```json
{
  "policy_id": "EQUITIES_LENS_HISTORY_POLICY_V1",
  "policy_version": "1.0.0",
  "policy_status": "POLICY_INCOMPLETE",
  "runtime_state": "VALIDATION_ONLY",
  "activation_state": "PROHIBITED",
  "lens_policies": [
    {
      "lens_id": "MARKET_BREADTH",
      "policy_version": "1.0.0",
      "history_requirement_status": "PENDING",
      "minimum_observations": null,
      "minimum_date_range": null,
      "measurement_frequency": null,
      "evidence_reference": null,
      "runtime_state": "VALIDATION_ONLY",
      "activation_state": "PROHIBITED"
    },
    {
      "lens_id": "VOLATILITY_STRUCTURE",
      "policy_version": "1.0.0",
      "history_requirement_status": "PENDING",
      "minimum_observations": null,
      "minimum_date_range": null,
      "measurement_frequency": null,
      "evidence_reference": null,
      "runtime_state": "VALIDATION_ONLY",
      "activation_state": "PROHIBITED"
    },
    {
      "lens_id": "LIQUIDITY_FLOWS",
      "policy_version": "1.0.0",
      "history_requirement_status": "PENDING",
      "minimum_observations": null,
      "minimum_date_range": null,
      "measurement_frequency": null,
      "evidence_reference": null,
      "runtime_state": "VALIDATION_ONLY",
      "activation_state": "PROHIBITED"
    }
  ]
}
```
