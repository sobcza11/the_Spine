# EQUITIES Multi-Index Dependency Matrix V1

The matrix records current governed state only. `PENDING` is not authorization,
publication, or activation.

| Index | Profile | Authorization | Raw Data | Canonical | Serving | Lens Dependency | Status |
|---|---|---|---|---|---|---|---|
| SPY | Complete | Required | Pending | Pending | Pending | Governance approval required | `PROFILE_COMPLETE_AUTHORIZATION_REQUIRED` |
| QQQ | Complete | Complete | Complete | Complete | Complete | Governance decisions pending | `SERVING_COMPLETE_LENS_BLOCKED` |
| DIA | Complete | Required | Pending | Pending | Pending | Governance approval required | `PROFILE_COMPLETE_AUTHORIZATION_REQUIRED` |
| IWM | Complete | Required | Pending | Pending | Pending | Governance approval required | `PROFILE_COMPLETE_AUTHORIZATION_REQUIRED` |
| MDY | Complete | Required | Pending | Pending | Pending | Governance approval required | `PROFILE_COMPLETE_AUTHORIZATION_REQUIRED` |
| ITOT | Complete | Required | Pending | Pending | Pending | Governance approval required | `PROFILE_COMPLETE_AUTHORIZATION_REQUIRED` |

All indexes remain `VALIDATION_ONLY`. Lens and production activation are
`PROHIBITED`.

<!-- MULTI_INDEX_MATRIX_JSON_V1 -->
```json
{
  "matrix_id": "EQUITIES_MULTI_INDEX_DEPENDENCY_MATRIX_V1",
  "matrix_version": "1.0.0",
  "runtime_state": "VALIDATION_ONLY",
  "activation_state": "PROHIBITED",
  "rows": [
    {"index": "SPY", "profile": "COMPLETE", "authorization": "AUTHORIZATION_REQUIRED", "raw_data": "PENDING", "canonical": "PENDING", "serving": "PENDING", "lens_dependency": "GOVERNANCE_APPROVAL_REQUIRED", "status": "PROFILE_COMPLETE_AUTHORIZATION_REQUIRED"},
    {"index": "QQQ", "profile": "COMPLETE", "authorization": "COMPLETE", "raw_data": "COMPLETE", "canonical": "COMPLETE", "serving": "COMPLETE", "lens_dependency": "GOVERNANCE_DECISIONS_PENDING", "status": "SERVING_COMPLETE_LENS_BLOCKED"},
    {"index": "DIA", "profile": "COMPLETE", "authorization": "AUTHORIZATION_REQUIRED", "raw_data": "PENDING", "canonical": "PENDING", "serving": "PENDING", "lens_dependency": "GOVERNANCE_APPROVAL_REQUIRED", "status": "PROFILE_COMPLETE_AUTHORIZATION_REQUIRED"},
    {"index": "IWM", "profile": "COMPLETE", "authorization": "AUTHORIZATION_REQUIRED", "raw_data": "PENDING", "canonical": "PENDING", "serving": "PENDING", "lens_dependency": "GOVERNANCE_APPROVAL_REQUIRED", "status": "PROFILE_COMPLETE_AUTHORIZATION_REQUIRED"},
    {"index": "MDY", "profile": "COMPLETE", "authorization": "AUTHORIZATION_REQUIRED", "raw_data": "PENDING", "canonical": "PENDING", "serving": "PENDING", "lens_dependency": "GOVERNANCE_APPROVAL_REQUIRED", "status": "PROFILE_COMPLETE_AUTHORIZATION_REQUIRED"},
    {"index": "ITOT", "profile": "COMPLETE", "authorization": "AUTHORIZATION_REQUIRED", "raw_data": "PENDING", "canonical": "PENDING", "serving": "PENDING", "lens_dependency": "GOVERNANCE_APPROVAL_REQUIRED", "status": "PROFILE_COMPLETE_AUTHORIZATION_REQUIRED"}
  ]
}
```
