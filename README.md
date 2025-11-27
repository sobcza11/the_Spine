> # <p align="center"> the_Spine | 🧠 </p>

<p align="center">
  <img src="_support/assets/main_photo.png" width="100%"  width="420"/>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/MacroFusion-Enabled-blueviolet" />
  <img src="https://img.shields.io/badge/Explainability-SHAP_%26_Permutation-success" />
  <img src="https://img.shields.io/badge/TimeSeries-Weekly_%26_Monthly-lightgrey" />
  <img src="https://img.shields.io/badge/MLOps-Governed_%26_Versioned-yellowgreen" />
  <img src="https://img.shields.io/badge/Python-3.10-blue" />
  <img src="https://img.shields.io/badge/License-MIT-green.svg" />
</p>

### Governed Global–US Hybrid Macro Intelligence Backbone  
Bridging FX, PMI, Commodities, Inflation, Sentiment, & Micro-economics into a unified, interpretable macro state.

---

## ⭐ Overview

**the_Spine** is a modular, governed macro-intelligence architecture that fuses **global breadth** with **US micro-depth** to create a stable and interpretable multi-domain macro signal.  
Every component (a “pipe”) produces a validated canonical signal, and all pipes flow into **MAIN_p**, the unified fusion engine.

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

## 🧩 Architecture (High-Level)
the_Spine/
│
├── MAIN_p/                         # Unified macro fusion engine
│
├── p_FX_Global/                    # FX basis, USD liquidity, EM stress
├── p_Econ_Global/                  # Global PMI diffusion, new orders, export cycles
├── p_Com_Global/                   # Brent/WTI spread, LNG, shipping rates
│
├── p_Econ_US/                      # ISM, NMI, payrolls, claims
├── p_Com_US/                       # WTI inventories, Cushing flows
├── p_Inflation_US/                 # CPI components, supercore, shelter
├── p_Micro_US/                     # MicroLineage-AI (SKU-level demand)
├── p_HealthAI_US/                  # Clinical DriftOps governance models
│
├── p_Sentiment_US/                 # OracleChambers (Fed sentiment)
│   ├── BeigeBook/                  # District business, labor & price tone
│   ├── FOMC_Minutes/               # Policy disagreement, uncertainty
│   ├── FOMC_Statement/             # Paragraph-level stance (hawkish/dovish)
│   ├── Fed_SEP/                    # Dot-plot shifts, forward guidance
│   └── Fed_Speeches/               # Speaker-level tone & certainty
│
├── p_Equity_US/
│   └── VinV/                       # "Value in Vogue" equity factor
│
└── MAIN_fusion/                    # Explainable macro-state scoring


---

## 🌐 Global Pipes

| Pipe | Description |
|------|-------------|
| **p_FX_Global** | Cross-currency basis, USD funding stress, EM risk-off indicators |
| **p_Econ_Global** | Global PMI diffusion, new orders, export cycles |
| **p_Com_Global** | Brent–WTI spread, LNG shipping rates, upstream supply tension |
| **p_Sent_Global** | (Upcoming) GeoNLP embeddings, geopolitical volatility |

---

## 🇺🇸 US Pipes

| Pipe | Description |
|------|-------------|
| **p_Econ_US** | ISM PMI, ISM NMI, hiring cycle, jobless claims |
| **p_Com_US** | Cushing inventories, refinery flows, internal crude spreads |
| **p_Inflation_US** | CPI decomposition into shelter, goods, services, supercore |
| **p_Micro_US** | MicroLineage-AI SKU-level micro signals |
| **p_HealthAI_US** | Clinical DriftOps fairness & drift monitoring |

---

## 🗣️ Fed & Macro Sentiment Pipes (OracleChambers)

All sentiment leaves are now implemented:

| Leaf | Status |
|------|--------|
| **Beige Book** | Complete (district-level sentiment) |
| **FOMC Minutes** | Complete |
| **FOMC Statement** | Complete |
| **Fed SEP (Dot Plot)** | Complete |
| **Fed Speeches** | Complete |

### 🧘 OracleChambers  
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

as_of_date | vinv_spread_val | vinv_spread_ret_12m | vinv_breadth
| vinv_score | vinv_regime


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
}```


---

🔒 Governance (CPMAI-Inspired, Responsible Data Science)

The Spine applies CPMAI-style rigor without claiming formal certification.

Data Transparency

All data legally obtained

Upstream sources documented

No private or login-restricted content

Data Preparation

Schema validation

Drift detection (PSI, KS, Z-score)

Outlier gates

Versioned ETL

Modeling

Every pipe outputs ONE interpretable signal

MAIN_p fuses signals with documented weights

Evaluation

Regime-shift stability

YoY consistency

Confidence scoring

Deployment

Versioned parquet leaves

Logging metadata

Drift & validation hooks

📦 Data Sources (Active + Pending)
Active

EIA

ISM

BLS CPI

Internal NLP sentiment pipelines

MicroLineage-AI datasets

Pending

TradingEconomics — global yields API

WRDS Approval — CRSP/Compustat, equities, macro data

These unlock Phase III global yield curve expansion + factor universes.

🧭 Roadmap

PINN-based commodity constraints

Global yield curve curvature

Volatility regime classifier

GeoNLP sentiment embeddings

Full /docs governance pages

Interactive macro dashboard

WRDS + TradingEconomics integration

📜 License

MIT License — open for reuse.

---

# 🚀 Ready to upload?

Once you paste this into `README.md`, run:

```powershell
git add README.md
git commit -m "Full README.md for the_Spine"
git push
