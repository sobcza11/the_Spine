# EQUITIES Lens Universe Policy V1

## Purpose and ownership

Define ownership requirements for analytical universes without selecting
symbols or establishing membership.

## Lens requirements

Market Breadth requires a governed cross-sectional universe. Its future owner
must govern constituent or proxy ownership, membership versioning, effective
dates, and rebalance handling.

Volatility Structure requires an explicit universe scope before evaluation.
Liquidity / Flow likewise requires an explicit universe scope. Neither scope
is inferred from the QQQ profile or any legacy artifact.

## Enforcement

Runtime remains `VALIDATION_ONLY`; activation remains `PROHIBITED`. Pending
ownership, scope, membership, or effective-dating evidence fails closed.

<!-- LENS_POLICY_JSON_V1 -->
```json
{
  "policy_id": "EQUITIES_LENS_UNIVERSE_POLICY_V1",
  "policy_version": "1.0.0",
  "policy_status": "POLICY_INCOMPLETE",
  "runtime_state": "VALIDATION_ONLY",
  "activation_state": "PROHIBITED",
  "lens_policies": [
    {
      "lens_id": "MARKET_BREADTH",
      "universe_scope": "PENDING",
      "constituent_or_proxy_owner": "PENDING",
      "membership_versioning": "PENDING",
      "effective_dates": "PENDING",
      "rebalance_handling": "PENDING"
    },
    {
      "lens_id": "VOLATILITY_STRUCTURE",
      "universe_scope": "PENDING"
    },
    {
      "lens_id": "LIQUIDITY_FLOWS",
      "universe_scope": "PENDING"
    }
  ],
  "symbols": []
}
```
