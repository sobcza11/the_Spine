# <p align="center">*the*_Spine • 🧠 • Data → Diagnostics → Context</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-blue" />
  <img src="https://img.shields.io/badge/MacroFusion-Canonical-blueviolet" />
  <img src="https://img.shields.io/badge/Explainability-Deterministic-success" />
  <img src="https://img.shields.io/badge/TimeSeries-Daily_%26_Monthly-lightgrey" />
  <img src="https://img.shields.io/badge/Governance-CPMAI--Aligned-yellowgreen" />
  <img src="https://img.shields.io/badge/License-MIT-green.svg" />
</p>

<p align="center">
  <img src="_support/assets/ft_mod.png" width="100%"/>
</p>

---

## 🌐 Canonical Macro-Financial Fusion Layer

***the*_Spine** is the **foundational data & fusion layer** for a governed macro-financial research stack.

It **does not forecast, optimize, or recommend actions**.

Instead, it provides:
- Canonical macro-financial inputs,
- Deterministic feature construction,
- Auditable, reproducible fusion outputs,
- A stable substrate for downstream diagnostics & interpretation.

Each domain emits **one locked, interpretable signal**.  
Together, they form the macro-state input used by downstream systems.

---

## 🧠 What the Spine Does (& Does Not Do)

### ✅ Does
- Canonical macro-financial **data fusion & normalization**
- G20-scoped inputs across **rates, FX, inflation, credit, commodities, equities, & real economy metrics**
- Deterministic feature contracts with **locked semantics**
- Versioned parquet outputs with full lineage
- Infrastructure for **reproducibility, auditability, & scale**

### ❌ Does Not
- Generate forecasts
- Train predictive models
- Optimize portfolios
- Produce allocation signals

---

## 🔮 Downstream Layers (Read-Only Consumers) 
**FT-GMI** | [Click Here](https://github.com/sobcza11/FT-GMI)

Regime-aware macro-financial diagnostics.
Consumes Spine outputs read-only.

**VinV** | [Click Here](https://github.com/sobcza11/VinV)

U.S. equity behavior conditioned on FT-GMI regimes.
Descriptive, not predictive.

***the* OracleChambers** | [Click Here](https://github.com/sobcza11/the_Spine/tree/main/the_OracleChambers)

Interpretive narrative & validation layer.
This is where AI/ML lives, operating strictly downstream — “*living off the exhaust*.”

**AI is constrained** to:
- Monitoring
- Explanation
- Validation support
It never alters upstream data or diagnostics.

---

### G20 Global Future Expansion (AE & EM RCpacks)

Based on development status, ***the*_Spine’s** macro segmentation mirrors
frameworks commonly applied by **central banks** and **quantitative macro research teams**.

RCpacks are designed for **diagnostic comparability**, not predictive modeling.

- 🪴 **Advanced Economies (AE-RCpack)**
  - **Australia, Canada, France, Germany, Italy, Japan, Korea, UK, US, EU**
    - Stable macro cycles, higher data frequency, transparent reporting
    - Well-suited for PMI diffusion, inflation decomposition, and yield-curve structure diagnostics

- 🌱 **Emerging Economies (EM-RCpack)**
  - **Argentina, Brazil, China, India, Indonesia, Mexico, Russia, Saudi Arabia, South Africa, Türkiye**
    - Higher volatility, asymmetric shocks, stronger FX & commodity transmission
    - Well-suited for stress propagation, diffusion heatmaps, FX basis, and commodity sensitivity diagnostics
---

## 🔐 **Governance (CPMAI-Aligned)**

- Deterministic pipelines
- Schema validation
- Drift & stability checks
- Locked feature semantics
- No silent retraining
- No feedback loops from outcomes

---

## 📦 **Data Sources**

**Active**
- EIA
- ISM
- BLS
- FRED
- Canonical Fed communications

**Pending**
- TradingEconomics
- WRDS (CRSP / Compustat)

---

## 🧭 **Roadmap (Post-Freeze Only)**

- Cross-regime diagnostics
- Stress-window extensions
- Global macro validation layers
- Documentation hardening







---

## 🌍 G20 Global Expansion (AE & EM RCpacks)
***Extending the Spine to a Globally Balanced Architecture***

The G20 represents **85% of global GDP** & anchors the Spine’s macro scope.
To scale cleanly, the Spine introduces **RCpacks (Regional Canonical Packs)** 
- Governed
- Versioned
- Comparable across regions
- Designed for regime diagnostics, not prediction

---

## 🧩 Architecture (High-Level)

```text
the_Spine/
│
├── MAIN_p/                        # Canonical macro fusion output
│
├── p_FX_Global/                   # FX basis, liquidity, EM stress
├── p_Econ_Global/                 # PMI diffusion, global activity
├── p_Com_Global/                  # Energy & commodity flows
│
├── p_Econ_US/                     # ISM, payrolls, claims
├── p_Com_US/                      # Inventories, flows
├── p_Inflation_US/                # CPI, shelter, supercore
│
├── p_Sentiment_US/                # Fed communications (canonical text → signals)
│
├── p_Glob/
│   ├── AE_RCpack/                 # Advanced Economies
│   ├── EM_RCpack/                 # Emerging Markets
│   └── Glob_fusion/               # AE + EM fusion
│
└── MAIN_fusion/                   # Global + US macro-state
```

---

## 🧠 Fusion Engine (MAIN_p)

All pipes converge into an interpretable macro-state:

```json
{
  "macro_state": "Moderate Slowdown",
  "risk_on_off": "Neutral",
  "confidence": 0.78,
  "drivers": ["WTI_Inventory", "PMI_Diffusion", "FX_Basis"],
  "explainability": {
    "p_Com_US": 0.33,
    "p_Econ_Global": 0.29,
    "p_FX_Global": 0.22,
    "p_Inflation_US": 0.10,
    "p_Sentiment_US": 0.06
  }
}
```

---

## 📜 License
MIT License 

---

## 4️⃣ Final Recommendation

### ✅ Upload This Version
```powershell
git add README.md
git commit -m "Align README with Spine governance & FT-GMI architecture"
git push

