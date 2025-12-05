# VinV (Value-in-Vogue) — Tranche 2 Prototype

## 1. What is VinV?

VinV is the_Spine’s **dividend- and valuation-stability layer**.  
It identifies equities whose long-horizon dividend durability and return behavior
exhibit **persistent strength** under certain macro regimes.

In Tranche 2, VinV is positioned as a **quantitative, interpretable factor**:
a governed signal that can be studied by AI & quant-finance groups (MIT, Oxford,
Toronto, Edinburgh) as a candidate for **cycle-aware, regime-sensitive alpha**.

---

## 2. Current Scope: A–LEVI Demo Universe

This prototype operates on a **partial universe**:

- All tickers from `A` through `LEVI` for which we have
  - daily adjusted prices
  - daily cash dividends
  - basic metadata (sector, industry, exchange)
- Plus benchmark tickers such as:
  - `SPY` (broad market)
  - `IWD` (value ETF), if present

This A–LEVI slice is used to validate the VinV logic and analytics
before scaling to the full symbol universe.

---

## 3. Core Components

### 3.1 Monthly Total Returns Panel

**Module:** `src/vinv/vinv_monthly.py`  
**Output:** `vinv_panel_A_LEVI_monthly.parquet`

Transforms daily prices + dividends into **monthly total returns** per ticker:

- `date`
- `ticker`
- `total_return` (price + dividend total return)

This is the canonical input for all VinV analytics.

---

### 3.2 VinV Equal-Weight Basket

**Module:** `src/vinv/vinv_basket.py`

Using a list of **VinV-eligible tickers** (defined by the dividend-stability rules),
we build an **equal-weighted VinV basket**:

- Each month, average the monthly total returns across all eligible tickers.
- Output is a time series: `vinv_ew_ret`.

The same module can align VinV with a benchmark (e.g. `SPY`):

- `vinv_ew_ret`
- `benchmark_ret`

---

### 3.3 Basic Outperformance & Risk Stats

**Module:** `src/vinv/vinv_basic_stats.py`

Given VinV vs benchmark monthly returns, we compute:

- number of months of overlap
- cumulative return (VinV vs benchmark)
- % of months VinV outperforms
- annualized return and volatility
- a simple Sharpe-like ratio for VinV
- mean excess return (VinV − benchmark)
- a basic t-statistic for excess returns

These metrics allow us to answer:

> “How often does VinV beat SPY (or IWD), and is the excess return meaningfully different from zero?”

---

### 3.4 Per-Ticker Outperformance Ranking

**Module:** `src/vinv/vinv_per_ticker_stats.py`

For each VinV-eligible ticker in A–LEVI, we compute:

- cumulative return vs benchmark
- number of months of overlapping data
- % of months outperforming benchmark

Result: a ranking of **candidate “Value in Vogue” equities** in the prototype universe.

---

### 3.5 Crisis-Window Analytics

**Module:** `src/vinv/vinv_crisis_windows.py`

On the VinV basket vs benchmark, we can slice specific stress periods (e.g. COVID):

- cumulative return in the window
- max drawdown in the window
- number of months

This shows how VinV behaves during **regime shocks** and is a key bridge to
adaptive markets and narrative-regime research in Tranche 2.

---

## 4. A–LEVI Demo Driver

**Script:** `scripts/run_vinv_a_levi_demo.py`

End-to-end runner that:

1. Builds the monthly panel from A–LEVI daily data.
2. Constructs the VinV equal-weight basket.
3. Aligns VinV vs `SPY` (or `IWD`).
4. Prints:
   - cumulative performance
   - % of months VinV outperforms
   - basic risk metrics (Sharpe, t-stat)
5. Produces per-ticker rankings.
6. Computes COVID crisis-window stats.

This demo is the current **Tranche 2 VinV prototype** and is designed
to scale to the full ticker universe once all symbols are available.

---

## 5. Next Steps (Beyond A–LEVI)

Once full-universe data is available:

- Re-run the same pipeline on all tickers.
- Extend the stats module with more formal significance testing (bootstrap CIs).
- Add macro-regime tagging to study VinV behavior across inflation/liquidity cycles.
- Package key results as research-grade tables and figures for Tranche 2 partners.
