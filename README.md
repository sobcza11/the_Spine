# <p align="center">the_Spine • 🧠 • Signals → Macro</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-blue" />
  <img src="https://img.shields.io/badge/MacroFusion-Enabled-blueviolet" />
  <img src="https://img.shields.io/badge/Explainability-SHAP_%26_Permutation-success" />
  <img src="https://img.shields.io/badge/TimeSeries-Dly, Wkly_%26_Mnthly-lightgrey" />
  <img src="https://img.shields.io/badge/MLOps-Gov._%26_Ver.-yellowgreen" />
  <img src="https://img.shields.io/badge/License-MIT-green.svg" />
</p>


<p align="center">
  <img src="_support/assets/main_photo.png" width="100%"  width="420"/>
</p>

---

## Governed Global
### *A US-Hybrid Macro Intelligence Oracle*  

Bridging micro-signals, PMI, commodities, sentiment, equities/FX, & inflation into a unified, interpretable macro-state.

---

## ⭐ Overview

**the_Spine** is a modular, governed macro-intelligence architecture that fuses **global breadth** with **US micro-depth** to create a stable and interpretable multi-domain macro signal.
Every component (a “pipe”) produces a validated canonical signal, and all pipes flow into **`MAIN_p`**, the unified fusion engine.

The system integrates:

- 🌐 Global FX basis & cross-currency spreads  
- 🌍 Global PMI diffusion & export cycles  
- 🛢️ Commodity flows (Brent/WTI spread, LNG, shipping rates)  
- 🇺🇸 WTI inventories & refinery throughput  
- 🧩 US inflation decomposition (core, supercore, shelter)  
- 🗣️ Fed & macro sentiment signals (Beige Book, FOMC Minutes, SEP, Statements, Speeches)  
- 📊 Equity VinV (“Value in Vogue”) regime model  
- 🔐 Governance, drift detection, schema validation, versioned lineage  
This is the **Hybrid Spine** — the union of *Global context* & *US precision timing*.

---

## 🔮 OracleChambers | Human Interface to the Spine

**the_Spine is the engine; OracleChambers is where humans interpret its signals—reading the tea leaves with structure.**

OracleChambers is the **interpretive layer** — transforming structural signals into human-usable macro narratives, regimes, and explainers.

**Current focus areas**:

- 🗣️ **Fed Speak (OracleChambers | Fed)**  
  NLP-ready sentiment leaves built around FOMC communications:
  - **Beige Book** – district-level tone on business, labor, wages, prices  
  - **FOMC Minutes** – uncertainty, disagreement, inflation vs. growth concern  
  - **FOMC Statement** – paragraph-level hawkish/dovish stance and focus  
  - **Fed SEP (Dot Plot)** – shifts in rate path & neutral rate sentiment  
  - **Fed Speeches** – speaker-level tone, certainty, forward-guidance hints  

  These are wired into canonical parquet leaves under `p_Sentiment_US`, ready for downstream
  modeling (regime flags, risk premia overlays, or macro-state explainers).

- 📊 **VinV (Value in Vogue)**  
  A US equity factor that tracks when **value is “in fashion”** relative to growth/market:
  - Valuation spread (value vs. benchmark)  
  - 12-month relative performance spread  
  - Breadth (% of value names outperforming)  
  - Composite **VinV Score** ∈ [-1, 1] and discrete regimes:
    `out_of_favor → transition → in_vogue`  

  Lives under `p_Equity_US/VinV/` and integrates cleanly into MAIN_p.

- 🧪 **Future Oracles (Planned)**  
  OracleChambers is also the “staging ground” for future interpretive layers, for example:
  - **Contagion analysis** – Fed language shifts → cross-asset response patterns  
  - **WRDS-backed extensions** – CRSP/Compustat earnings & factor overlays (pending access)  
  - **Corporate & earnings sentiment drift** – fraud / overstatement red-flags  
  - **Association Rule Mining** – news / narrative patterns → market co-moves  
  - **Macro regime narratives** – linking Dalio/Gundlach “Illusory Wealth Regime” style views
    to Spine signals  

All of this remains **inside this repo** for now — OracleChambers functions as a documented
sub-system within the_Spine, not as a separate codebase.

---

## 🧩 Architecture (High-Level)

```text
the_Spine/
│
├── MAIN_p/                          # Unified macro fusion engine
│
├── p_FX_Global/                     # FX basis, USD liquidity, EM stress
├── p_Econ_Global/                   # Global PMI diffusion, new orders, exports
├── p_Com_Global/                    # Brent/WTI, LNG, shipping
│
├── p_Econ_US/                       # ISM, NMI, payrolls, claims
├── p_Com_US/                        # WTI inventories, Cushing flows
├── p_Inflation_US/                  # CPI components, supercore, shelter
├── p_Micro_US/                      # MicroLineage-AI (SKU demand signals)
├── p_HealthAI_US/                   # Clinical DriftOps governance models
│
├── p_Sentiment_US/                  # Fed_Sentiment (canonical leaves)
│   ├── BeigeBook/
│   ├── FOMC_Minutes/
│   ├── FOMC_Statement/
│   ├── Fed_SEP/
│   └── Fed_Speeches/
│
├── p_Equity_US/
│   └── VinV/                        # Value-in-Vogue equity factor
│
└── MAIN_fusion/                     # Explainable macro-state
```

All sentiment leaves are now implemented:

| Leaf | Status |
|------|--------|
| **Beige Book** | Complete (district-level sentiment) |
| **FOMC Minutes** | Complete |
| **FOMC Statement** | Complete |
| **Fed SEP (Dot Plot)** | Complete |
| **Fed Speeches** | Complete |

## 🧘 OracleChambers  
A forward-looking space for exploratory sentiment research:
- Contagion analysis between Fed language & market reaction  
- Association Rule Mining (ARM) for news → markets pattern analysis  
- Corporate earnings sentiment drift (fraud/overstatement detection)  
- Cross-market signaling consistency  
- Dalio/Gundlach “Illusory Wealth Regime” integration  
- WRDS data expansion (pending approval)  

---

## 📊 Equity: VinV (Value in Vogue)

A monthly equity factor that measures:

- Value vs. Growth valuation spread  
- 12-month relative performance spread  
- Cohort breadth (percentage of value names outperforming benchmark)  
- Composite VinV Score ∈ [-1, +1]  
- Regime classification: *out_of_favor → transition → in_vogue*

Canonical output:

| as_of_date | vinv_spread_val | vinv_spread_ret_12m | vinv_breadth | vinv_score | vinv_regime |
|------------|------------------|----------------------|--------------|------------|-------------|

---

## 🌍 G20 Global Expansion (AE & EM RCpacks)
***Extending the Spine to a Globally Balanced Architecture***

The G20 cluster represents **85% of world GDP** and is the natural extension of the_Spine’s Global layer.
To scale cleanly, the project introduces **RCpacks (Regional Canonical Packs)** — governed, structured data-packs for each economic block.

----

### G20 Global Future Expansion (AE & EM RCpacks)

Based on development status — `the_Spine`'s primary **macro segmentation  mirrors** the frameworks **applied by central banks** and **quantitative research** teams.

- **Advanced Economies** (***AE-RCpack***)
  - **Australia, Canada, France, Germany, Italy, Japan, Korea, UK, US, EU**
    - *Stable cycles, high-frequency signals, transparent data*
    - *Ideal for PMI, inflation decomposition, yield curve curvature*

- **Emerging Economies** (***EM-RCpack***)
  - **Argentina, Brazil, China, India, Indonesia, Mexico, Russia, Saudi Arabia, South Africa, Türkiye**
    - *Higher volatility, asymmetric shocks, more signal in FX/commodities*
    - *Ideal for diffusion heatmaps, EM FX basis, commodity sensitivity*

---

## Directory Structure

```text
the_Spine/
│
├── p_Glob/
│   ├── AE_RCpack/
│   │    ├── AE_m/         # Canonical AE macro panel
│   │    ├── AE_fx/        # FX basis, carry, liquidity indicators
│   │    ├── AE_pmi/       # Manufacturing + services diffusion
│   │    └── AE_com/       # LNG, Brent, metals
│   │
│   ├── EM_RCpack/
│   │    ├── EM_m/         # Canonical EM macro panel
│   │    ├── EM_fx/        # EM basis, stress spreads
│   │    ├── EM_pmi/       # EM PMI + new orders components
│   │    └── EM_com/       # Commodity-linked EM exposures
│   │
│   └── Glob_fusion/       # AE + EM → unified global macro signal
```

---

## Fusion Logic
**Global_Spine** = *w_AE * AE_fusion  +  w_EM * EM_fusion*


Where:
- w_AE ≈ stability weight
- w_EM ≈ volatility-weighted signal strength

This ***produces*** **the Glob-US Macro State**, the final output for `MAIN_p`.

## 📊 Equity: VinV (Value in Vogue)

A monthly equity factor that measures:
- Valuation spread
- 12-month relative return
- Breadth
- Composite VinV Score ∈ [-1, +1]
- Regime states: out_of_favor → transition → in_vogue

as_of_date	vinv_spread_val	vinv_spread_ret_12m	vinv_breadth	vinv_score	vinv_regime

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

## 🔒 **Governance (CPMAI-Inspired, Responsible Data Science)**

The Spine applies CPMAI-style rigor without claiming formal certification.

## Data Transparency
- All data legally obtained  
- Upstream sources documented  
- No private or login-restricted content  

## Data Preparation
- Schema validation  
- Drift detection (PSI, KS, Z-score)  
- Outlier gates  
- Versioned ETL  

## Modeling
- Each pipe outputs *one* interpretable signal  
- MAIN_p fuses signals with documented, explainable weights  

## Evaluation
- Regime-shift stability  
- Year-over-year consistency  
- Confidence scoring  

## Deployment
- Versioned parquet leaves  
- Logged metadata for audit  
- Drift gates & validation hooks

## 📦 Data Sources (Active + Pending)

### **Active**
- EIA  
- ISM  
- BLS CPI  
- Internal NLP sentiment pipelines  
- MicroLineage-AI datasets  

### **Pending**
- TradingEconomics — global yields API  
- WRDS Approval — CRSP/Compustat, equities, macro datasets  

> These unlock Phase III: global yield curve expansion + equity factor universes.

---

## 🧭 Roadmap
- PINN-based commodity constraints  
- Global yield curve curvature  
- Volatility regime classifier  
- GeoNLP sentiment embeddings  
- Full /docs governance pages  
- Interactive macro dashboard  
- WRDS + TradingEconomics integration

---

## 📜 License
MIT License — open for reuse.

---

## 🚀 Ready to upload?
Once you paste this into `README.md`, run:

```powershell
git add README.md
git commit -m "Full README.md for the_Spine"
git push
