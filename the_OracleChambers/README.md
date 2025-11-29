# <p align="center">the_OracleChambers • 🔮 • Macro Narratives → Insight</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/sobcza11/the_Spine/main/_support/assets/main_photo2.png" width="720">
</p>

---

**the_OracleChambers** is a domain-specific Mini AI Lab that transforms economic signals, Fed communication, and cross-market data into structured narratives, macro briefs, scenario paths, and policy-drift insights.

It functions as the **interpretive intelligence layer** on top of:

- **the_Spine** — macro fusion engine  
- **FedSpeak NLP** — policy-sensitive communication analysis  
- **HKNSL scaffolding** — canonical leaf & scenario structure  

Together, these form a governed, explainable system modeled after architectures used in modern macro hedge funds, central banks, and advanced research labs.

**the_OracleChambers behaves like a macro-financial AI Lab** — a CPMAI-aligned approach to governed, structured, and transparent reasoning designed to produce  
**decision-support intelligence**, not just analytics.

---

### 🗣️ CURRENT FOCUS AREA — Fed Speak (OracleChambers | Fed)

NLP-ready sentiment leaves built around major FOMC communication streams:

- **Beige Book** — district-level tone on business, labor, wages, prices  
- **FOMC Minutes** — uncertainty, disagreement, inflation vs. growth concern  
- **FOMC Statement** — paragraph-level hawkish/dovish stance  
- **Fed SEP (Dot Plot)** — rate-path & neutral-rate sentiment  
- **Fed Speeches** — speaker-level tone, certainty, forward-guidance signals  

All outputs integrate into **`p_Sentiment_US`** for consistent macro interpretation inside both  
**the_Spine** and **the_OracleChambers**.

---

## 📊 PLANNED FOCUS AREAS

### **VinV (Value in Vogue)**  
A U.S. equity factor tracking when value is “in fashion”:

- Valuation spread  
- 12-month relative performance  
- Breadth (% of value names outperforming)  
- Composite **VinV Score** ∈ [-1, 1]  
- Regimes: `out_of_favor → transition → in_vogue`

Stored under **`p_Equity_US/VinV/`**, integrated into **MAIN_p**.

---

### 🧪 Future Oracles (Planned)

The_OracleChambers serves as the staging ground for additional interpretive layers:

- **Contagion analysis** — Fed language → cross-asset reactions  
- **WRDS extensions** — CRSP/Compustat factor overlays  
- **Corporate & earnings sentiment drift** — fraud / overstatement signals  
- **Association-Rule mining** — narrative patterns → market co-moves  
- **Macro regime narratives** — Dalio/Gundlach “Illusory Wealth Regime” alignment  

OracleChambers operates as a documented **interpretation subsystem** within the broader `the_Spine` architecture.

---

# 🧠 Purpose & Vision

The aim of the_OracleChambers is straightforward:

> **Explain *why* the economy is shifting — not just *what* is happening.**

It produces both analyst-ready and machine-ready insights through multi-modal reasoning:

### Outputs include:

- Leadership-grade macro briefs  
- Scenario-aware market commentaries  
- Policy-drift & communication-shift insights  
- Multi-lens risk narratives  
- Machine-readable narrative atoms  
- Regime-aware probability adjustments  
- Interpretability across inflation, labor, policy, and global signals  

---

# 🧩 System Architecture

the_OracleChambers sits atop a larger macro-analytic family:

- **the_Spine / Spine-Glob-US** — macro fusion (regimes, probabilities, macro leaves)  
- **FedSpeak / HKNSL** — policy-sensitive NLP & communication leaves  
- **MicroLineage / DriftOps** — micro-demand & governance patterns  

OracleChambers ingests these upstream signals and converts them into  
interpretable outputs: narratives, risk briefs, scenario commentary.

```text
the_OracleChambers/
├─ data/
│  ├─ processed/
│  │  ├─ narrative_snapshots.parquet
│  │  ├─ fedspeak_story_blocks.parquet
│  │  └─ macro_state_briefs.parquet
│  ├─ prompts/
│  │  └─ oracle_prompts.yml
│  └─ vocab/
│     ├─ macro_terms.json
│     ├─ tone_lexicon.json
│     └─ risk_glyphs.json
│
├─ src/
│  └─ oraclechambers/
│     ├─ config.py
│     ├─ registry.py
│     │
│     ├─ inputs/
│     │  ├─ spine_loader.py
│     │  ├─ fedspeak_loader.py
│     │  └─ markets_loader.py
│     │
│     ├─ lenses/
│     │  ├─ inflation_lens.py
│     │  ├─ labor_lens.py
│     │  ├─ stability_lens.py
│     │  └─ global_lens.py
│     │
│     ├─ narratives/
│     │  ├─ macro_state_story.py
│     │  ├─ fedspeak_story.py
│     │  ├─ risk_brief.py
│     │  └─ scenario_commentary.py
│     │
│     ├─ scoring/
│     │  ├─ coherence.py
│     │  ├─ stability.py
│     │  └─ alignment.py
│     │
│     ├─ exporters/
│     │  ├─ to_markdown.py
│     │  ├─ to_json.py
│     │  └─ to_deck_outline.py
│     │
│     └─ utils/
│        ├─ formatting.py
│        └─ time_windows.py
│
└─ notebooks/
   ├─ 01_overview.ipynb
   ├─ 02_macro_narratives.ipynb
   └─ 03_fedspeak_interpretation.ipynb



