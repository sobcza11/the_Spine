# EQUITIES Lens Missing Observation Evidence Binding V1

## Purpose

Define the evidence required before missing-observation handling can be
governed. This binding does not choose handling behavior.

Each lens owner must provide an approved trading calendar, missing-session
evidence, stale-data evidence, null-handling evidence, duplicate-handling
evidence, and an approval reference. Pending evidence fails closed.

Runtime state: `VALIDATION_ONLY`

Activation state: `PROHIBITED`

<!-- LENS_EVIDENCE_JSON_V1 -->
```json
{
  "evidence_id": "EQUITIES_LENS_MISSING_OBSERVATION_EVIDENCE_BINDING_V1",
  "evidence_version": "1.0.0",
  "evidence_status": "EVIDENCE_INCOMPLETE",
  "runtime_state": "VALIDATION_ONLY",
  "activation_state": "PROHIBITED",
  "trading_calendar": null,
  "missing_sessions_evidence": null,
  "stale_data_evidence": null,
  "null_handling_evidence": null,
  "duplicate_handling_evidence": null,
  "approval_reference": null,
  "handling_rules": []
}
```
