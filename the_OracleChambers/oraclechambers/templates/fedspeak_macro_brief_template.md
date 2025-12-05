# FedSpeak Macro Brief — {{ as_of_date }}

## 1. Policy-Language Regime Snapshot

As of **{{ as_of_date }}**, the FedSpeak engine classifies the policy-language
regime as:

> **{{ fed_regime_label }}**

with an overall Fed sentiment score of:

> **{{ fed_sentiment_score | round(2) }}** (on a scale from -1 to +1)

This places the Federal Reserve’s communication in a **{{ regime_tone }}**
configuration: {{ regime_summary }}.

---

## 2. Inflation vs Growth Emphasis

- **Inflation pressure:** {{ inflation_pressure | round(3) }}
- **Growth concern:**    {{ growth_concern | round(3) }}

Narrative:

> {{ inflation_vs_growth_narrative }}

---

## 3. Uncertainty & Coherence Drift

- **Policy uncertainty level:**  {{ policy_uncertainty | round(3) }}
- **Uncertainty drift:**         {{ uncertainty_drift | round(3) }}
- **Policy coherence level:**    {{ policy_coherence | round(3) }}
- **Coherence drift:**           {{ coherence_drift | round(3) }}

Interpretation:

> {{ drift_narrative }}

---

## 4. Beige Book Tone (If Available)

- **Growth tone:**  {{ beige_growth_tone | default("n/a") }}
- **Price tone:**   {{ beige_price_tone  | default("n/a") }}
- **Wage tone:**    {{ beige_wage_tone   | default("n/a") }}

Summary:

> {{ beige_narrative }}

---

## 5. Implications for the_Spine (p_Sentiment_US)

The current Fed regime and drift profile imply a Fed sentiment
component of **{{ fed_sentiment_score | round(2) }}** feeding into
`p_Sentiment_US`. In practical terms:

> {{ spine_implications_narrative }}

This sentiment will be fused with Econ, Inflation, and WTI pillars to
update the headline macro state in the_Spine.

---
