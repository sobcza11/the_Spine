document.addEventListener("DOMContentLoaded", () => {
  const viewButtons = document.querySelectorAll("[data-view]");
  const contentViews = document.querySelectorAll(".content-view");
  const navParents = document.querySelectorAll(".nav-parent");
  const subnavs = document.querySelectorAll(".subnav");
  const moduleTabs = document.querySelectorAll(".module-tab[data-view]");

  const fxControls = {
    pair: document.getElementById("fx-pair"),
    spreads: document.getElementById("fx-spreads"),
    horizon: document.getElementById("fx-horizon"),
    geoscen: document.getElementById("fx-geoscen-mode")
  };

  const equitiesControls = {
    region: document.getElementById("equities-region"),
    horizon: document.getElementById("equities-horizon"),
    topRightMode: document.getElementById("equities-top-right-mode"),
    geoscen: document.getElementById("equities-geoscen-mode")
  };

  const wtiControls = {
    horizon: document.getElementById("wti-horizon"),
    geoscen: document.getElementById("wti-geoscen-mode"),
    ocOverlay: document.getElementById("wti-oc-overlay")
  };

  const DATA_ENDPOINTS = {
    price: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_price_data.json",
    spreads: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_spreads_data.json",
    sigma: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_sigma_data.json",
    universe: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_universe.json"
  };

  const DEFAULT_FX_UNIVERSE = [
    "AUDUSD",
    "EURUSD",
    "GBPUSD",
    "USDCAD",
    "USDCHF",
    "USDJPY"
  ];

  const SYMBOL_TO_DISPLAY_PAIR = {
    AUDUSD: "AUD/USD",
    EURUSD: "EUR/USD",
    GBPUSD: "GBP/USD",
    USDCAD: "USD/CAD",
    USDCHF: "USD/CHF",
    USDJPY: "USD/JPY"
  };

  const DISPLAY_PAIR_TO_SYMBOL = Object.fromEntries(
    Object.entries(SYMBOL_TO_DISPLAY_PAIR).map(([symbol, display]) => [display, symbol])
  );

  const ACTIVE_REFRESH_MS = 60 * 1000;

  const activeDataStore = {
    price: {},
    spreads: {},
    sigma: [],
    universe: []
  };

  const endpointHealth = {
    price: false,
    spreads: false,
    sigma: false,
    universe: false
  };

  let activeDataLoaded = false;
  let activeDataLoadError = null;
  let activeRefreshHandle = null;

  const HORIZON_LENGTH = {
    "5D": 5,
    "15D": 15,
    "30D": 30,
    "45D": 45
  };

  const SPREAD_HORIZON_LENGTH = {
    "5D": 12,
    "15D": 18,
    "30D": 24,
    "45D": 36
  };

  async function fetchJsonWithBust(url) {
    const bust = `_ts=${Date.now()}`;
    const joiner = url.includes("?") ? "&" : "?";
    const response = await fetch(`${url}${joiner}${bust}`, {
      method: "GET",
      cache: "no-store"
    });

    if (!response.ok) {
      throw new Error(`Fetch failed: ${response.status} ${response.statusText} | ${url}`);
    }

    return response.json();
  }

  function normalizeUniverse(values) {
    if (!Array.isArray(values)) return [];
    return values
      .map((x) => String(x).toUpperCase().trim())
      .filter(Boolean);
  }

  function normalizePricePayload(payload) {
    return payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
  }

  function normalizeSpreadsPayload(payload) {
    return payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
  }

  function normalizeSigmaPayload(payload) {
    return Array.isArray(payload) ? payload : [];
  }

  async function loadActiveData() {
    const results = await Promise.allSettled([
      fetchJsonWithBust(DATA_ENDPOINTS.price),
      fetchJsonWithBust(DATA_ENDPOINTS.spreads),
      fetchJsonWithBust(DATA_ENDPOINTS.sigma),
      fetchJsonWithBust(DATA_ENDPOINTS.universe)
    ]);

    const [priceRes, spreadsRes, sigmaRes, universeRes] = results;

    endpointHealth.price = priceRes.status === "fulfilled";
    endpointHealth.spreads = spreadsRes.status === "fulfilled";
    endpointHealth.sigma = sigmaRes.status === "fulfilled";
    endpointHealth.universe = universeRes.status === "fulfilled";

    activeDataStore.price =
      priceRes.status === "fulfilled"
        ? normalizePricePayload(priceRes.value)
        : {};

    activeDataStore.spreads =
      spreadsRes.status === "fulfilled"
        ? normalizeSpreadsPayload(spreadsRes.value)
        : {};

    activeDataStore.sigma =
      sigmaRes.status === "fulfilled"
        ? normalizeSigmaPayload(sigmaRes.value)
        : [];

    activeDataStore.universe =
      universeRes.status === "fulfilled"
        ? normalizeUniverse(universeRes.value)
        : [...DEFAULT_FX_UNIVERSE];

    activeDataLoaded = true;

    const failed = [];
    if (!endpointHealth.price) failed.push("price");
    if (!endpointHealth.spreads) failed.push("spreads");
    if (!endpointHealth.sigma) failed.push("sigma");
    if (!endpointHealth.universe) failed.push("universe");

    activeDataLoadError = failed.length
      ? new Error(`Failed endpoints: ${failed.join(", ")}`)
      : null;
  }

  async function refreshActiveDataAndRender() {
    try {
      await loadActiveData();
      populateFxPairOptions();

      if (document.body.classList.contains("view-fx")) {
        renderFXDeferred();
      }
    } catch (error) {
      activeDataLoadError = error;
      console.error("Active FX data refresh failed:", error);

      if (document.body.classList.contains("view-fx")) {
        renderFXDeferred();
      }
    }
  }

  function startActiveRefreshLoop() {
    if (activeRefreshHandle) return;
    activeRefreshHandle = window.setInterval(() => {
      refreshActiveDataAndRender();
    }, ACTIVE_REFRESH_MS);
  }

  function clearSidebarState() {
    document.querySelectorAll(".sidebar-nav .nav-item").forEach((item) => {
      item.classList.remove("active");
    });
  }

  function closeAllSubnavs() {
    subnavs.forEach((subnav) => subnav.classList.remove("open"));
  }

  function openSubnav(parentKey) {
    closeAllSubnavs();

    const targetSubnav = document.querySelector(`[data-subnav="${parentKey}"]`);
    if (targetSubnav) {
      targetSubnav.classList.add("open");
    }

    navParents.forEach((parent) => {
      if (parent.dataset.parent === parentKey) {
        parent.classList.add("active");
      }
    });
  }

  function setBodyViewClass(viewName) {
    document.body.classList.remove(
      "view-what-is",
      "view-fx",
      "view-wti",
      "view-equities",
      "about-sidebar-hidden"
    );

    if (viewName === "fx") {
      document.body.classList.add("view-fx", "about-sidebar-hidden");
      return;
    }

    if (viewName === "wti") {
      document.body.classList.add("view-wti", "about-sidebar-hidden");
      return;
    }

    if (viewName === "equities") {
      document.body.classList.add("view-equities", "about-sidebar-hidden");
      return;
    }

    document.body.classList.add("view-what-is");
  }

  function setActiveButtons(viewName) {
    document.querySelectorAll(`[data-view="${viewName}"]`).forEach((button) => {
      button.classList.add("active");
    });
  }

  function syncSidebarHierarchy(viewName) {
    clearSidebarState();

    if (viewName === "what-is" || viewName === "what-is-vector" || viewName === "what-is-iso") {
      openSubnav("what-is");

      const parent = document.querySelector('.nav-parent[data-parent="what-is"]');
      if (parent) parent.classList.add("active");

      if (viewName !== "what-is") {
        const child = document.querySelector(`.nav-child[data-view="${viewName}"]`);
        if (child) child.classList.add("active");
      } else {
        const landing = document.querySelector('.nav-parent[data-view="what-is"]');
        if (landing) landing.classList.add("active");
      }

      return;
    }

    closeAllSubnavs();

    const direct = document.querySelector(`.sidebar-nav .nav-item[data-view="${viewName}"]`);
    if (direct) {
      direct.classList.add("active");
    }
  }

  function showView(viewName) {
    contentViews.forEach((view) => view.classList.remove("active"));
    viewButtons.forEach((button) => button.classList.remove("active"));

    const targetView = document.getElementById(`view-${viewName}`);
    if (targetView) {
      targetView.classList.add("active");
    }

    setActiveButtons(viewName);
    syncSidebarHierarchy(viewName);
    setBodyViewClass(viewName);

    moduleTabs.forEach((tab) => tab.classList.remove("active"));
    document.querySelectorAll(`.module-tab[data-view="${viewName}"]`).forEach((tab) => {
      tab.classList.add("active");
    });

    if (viewName === "fx") {
      populateFxPairOptions();
      renderFXDeferred();
    }

    if (viewName === "wti") {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          void renderWTI();
        });
      });
    }

    if (viewName === "equities") {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          renderEquities();
        });
      });
    }
  }

  function formatNumber(value, digits = 2) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "--";
    return num.toFixed(digits);
  }

  function getPairDigits(pair) {
    return pair === "USD/JPY" ? 2 : 4;
  }

  function formatDateLabel(dateStr) {
    const d = new Date(`${dateStr}T00:00:00`);
    if (Number.isNaN(d.getTime())) return String(dateStr);
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  }

  function formatUTC(dateStr) {
    const d = new Date(`${dateStr}T00:00:00Z`);
    if (Number.isNaN(d.getTime())) return "--";
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")} (UTC)`;
  }

  function reverseSymbol(sym) {
    if (!sym || sym.length !== 6) return sym;
    return `${sym.slice(3)}${sym.slice(0, 3)}`;
  }

  function simpleMovingAverage(values, period) {
    return values.map((_, idx) => {
      if (idx < period - 1) return null;
      const windowVals = values.slice(idx - period + 1, idx + 1);
      const avg = windowVals.reduce((a, b) => a + b, 0) / period;
      return Number(avg.toFixed(6));
    });
  }

  function exponentialMovingAverage(values, period) {
    const k = 2 / (period + 1);
    const ema = new Array(values.length).fill(null);

    if (!values.length) return ema;

    let prev = values[0];
    ema[0] = Number(prev.toFixed(6));

    for (let i = 1; i < values.length; i += 1) {
      prev = (values[i] * k) + (prev * (1 - k));
      ema[i] = Number(prev.toFixed(6));
    }

    return ema;
  }

  function createLinePath(points) {
    if (!points.length) return "";
    return points
      .map((p, i) => `${i === 0 ? "M" : "L"} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`)
      .join(" ");
  }

  function getLockedUniverseSymbols() {
    const fromUniverse = normalizeUniverse(activeDataStore.universe);

    if (fromUniverse.length) return fromUniverse;

    const priceKeys = Object.keys(activeDataStore.price || {}).map((x) => x.toUpperCase());
    const spreadKeys = Object.keys(activeDataStore.spreads || {}).map((x) => x.toUpperCase());
    const sigmaKeys = (activeDataStore.sigma || []).map((row) => String(row.symbol || "").toUpperCase());

    const intersection = priceKeys.filter(
      (sym) => spreadKeys.includes(sym) && sigmaKeys.includes(sym)
    ).sort();

    return intersection.length ? intersection : [...DEFAULT_FX_UNIVERSE];
  }

  function getDisplayPairsFromUniverse() {
    const locked = getLockedUniverseSymbols()
      .map((symbol) => SYMBOL_TO_DISPLAY_PAIR[symbol])
      .filter(Boolean);

    if (locked.length) return locked;

    return [...DEFAULT_FX_UNIVERSE]
      .map((symbol) => SYMBOL_TO_DISPLAY_PAIR[symbol])
      .filter(Boolean);
  }

  function populateFxPairOptions() {
    if (!fxControls.pair) return;

    const displayPairs = getDisplayPairsFromUniverse();
    const currentValue = fxControls.pair.value;

    fxControls.pair.innerHTML = "";

    displayPairs.forEach((pair) => {
      const opt = document.createElement("option");
      opt.value = pair;
      opt.textContent = pair;
      fxControls.pair.appendChild(opt);
    });

    if (displayPairs.includes(currentValue)) {
      fxControls.pair.value = currentValue;
    } else if (displayPairs.length) {
      fxControls.pair.value = displayPairs[0];
    }
  }

  function getHarmonizedSigmaRows() {
    const locked = new Set(getLockedUniverseSymbols());

    return (activeDataStore.sigma || [])
      .filter((row) => locked.has(String(row.symbol || "").toUpperCase()))
      .map((row) => ({
        ...row,
        symbol: String(row.symbol || "").toUpperCase(),
        pair: SYMBOL_TO_DISPLAY_PAIR[String(row.symbol || "").toUpperCase()] || row.pair || row.symbol
      }))
      .sort((a, b) => Number(a.rank) - Number(b.rank));
  }

  function updateGeoScenToolbarLabel() {
    const labelNode = document.querySelector("[data-geoscen-toolbar-label]");
    if (!labelNode || !fxControls.geoscen) return;

    const mode = String(fxControls.geoscen.value || "").toLowerCase();

    labelNode.textContent =
      mode === "systemic"
        ? "GeoScen | OC | Systemic, Groups"
        : "GeoScen | OC | Home, Countries";
  }

  function updateWTIGeoScenToolbarLabel() {
    const labelNode = document.querySelector("[data-wti-geoscen-toolbar-label]");
    if (!labelNode || !wtiControls.geoscen) return;

    const mode = String(wtiControls.geoscen.value || "").toLowerCase();

    labelNode.textContent =
      mode === "systemic"
        ? "GeoScen | OC | Systemic, Groups"
        : "GeoScen | OC | Home, Countries";
  }

  function updateEquitiesGeoScenToolbarLabel() {
    const labelNode = document.querySelector("[data-equities-geoscen-toolbar-label]");
    if (!labelNode || !equitiesControls.geoscen) return;

    const mode = String(equitiesControls.geoscen.value || "").toLowerCase();

    labelNode.textContent =
      mode === "systemic"
        ? "GeoScen | OC | Systemic, Groups"
        : "GeoScen | OC | Home, Countries";
  }

  function renderEquitiesTopRightSkeleton(mode) {
    const titleEl = document.querySelector("#view-equities .equities-top-right-panel .panel-title");
    const subtitleEl = document.getElementById("equities-top-right-subtitle");
    const badgeEl = document.getElementById("equities-top-right-badge");
    const primaryEl = document.getElementById("equities-top-right-primary");
    const secondaryEl = document.getElementById("equities-top-right-secondary");
    const stateEl = document.getElementById("equities-top-right-state");
    const chartEl = document.getElementById("equities-top-right-chart");

    if (!titleEl || !subtitleEl || !badgeEl || !primaryEl || !secondaryEl || !stateEl || !chartEl) {
      return;
    }

    if (mode === "Vol Structure") {
      titleEl.textContent = "Vol Structure";
      subtitleEl.textContent = "Option B skeleton";
      badgeEl.textContent = "Option B";
      primaryEl.textContent = "VIX";
      secondaryEl.textContent = "Term Structure";
      stateEl.textContent = "Skeleton";
      chartEl.innerHTML = `<div class="panel-placeholder">Vol structure skeleton.</div>`;
      return;
    }

    if (mode === "Liquidity / Flows") {
      titleEl.textContent = "Liquidity / Flows";
      subtitleEl.textContent = "Option C skeleton";
      badgeEl.textContent = "Option C";
      primaryEl.textContent = "ETF Flows";
      secondaryEl.textContent = "Positioning";
      stateEl.textContent = "Skeleton";
      chartEl.innerHTML = `<div class="panel-placeholder">Liquidity / flows skeleton.</div>`;
      return;
    }

    titleEl.textContent = "Market Breadth";
    subtitleEl.textContent = "Default entry state";
    badgeEl.textContent = "Option A";
    primaryEl.textContent = "% Above 200D";
    secondaryEl.textContent = "Adv / Dec";
    stateEl.textContent = "Skeleton";
    chartEl.innerHTML = `<div class="panel-placeholder">Market breadth skeleton.</div>`;
  }

  function renderEquities() {
    updateEquitiesGeoScenToolbarLabel();

    const region = equitiesControls.region?.value || "USA / Europe";
    const horizon = equitiesControls.horizon?.value || "30D";
    const topRightMode = equitiesControls.topRightMode?.value || "Market Breadth";

    const indexBadge = document.getElementById("equities-index-badge");
    const indexDate = document.getElementById("equities-index-date");
    const industryDate = document.getElementById("equities-industry-date");
    const topRightDate = document.getElementById("equities-top-right-date");
    const sigmaDate = document.getElementById("equities-sigma-date");

    if (indexBadge) indexBadge.textContent = `${region} • ${horizon}`;
    if (indexDate) indexDate.textContent = "--";
    if (industryDate) industryDate.textContent = "--";
    if (topRightDate) topRightDate.textContent = "--";
    if (sigmaDate) sigmaDate.textContent = "--";

    updateStatValue(document.getElementById("equities-index-focus"), "SPX / NDX / DJI / STOXX");
    updateStatValue(document.getElementById("equities-index-state"), "Skeleton");
    updateStatValue(document.getElementById("equities-index-mode"), "Multi-Index");

    updateStatValue(document.getElementById("equities-industry-pmi"), "Manufacturing / Services");
    updateStatValue(document.getElementById("equities-industry-bias"), "Skeleton");
    updateStatValue(document.getElementById("equities-industry-state"), "Cycle Mapping");

    updateStatValue(document.getElementById("equities-sigma-selected"), "SPX");
    updateStatValue(document.getElementById("equities-sigma-z"), "--");
    updateStatValue(document.getElementById("equities-sigma-rank"), "--");

    setChartPlaceholder("equities-index-chart", "Equity index panel skeleton.");
    setChartPlaceholder("equities-industry-chart", "Industry / PMI alignment skeleton.");
    setChartPlaceholder("equities-sigma-chart", "No sigma data available.");

    renderEquitiesTopRightSkeleton(topRightMode);
  }

  function getEmbeddedPriceRows(pair, horizon) {
    const symbol = DISPLAY_PAIR_TO_SYMBOL[pair];
    const store = activeDataStore.price || {};

    if (!symbol || !store[symbol] || !Array.isArray(store[symbol])) return null;

    const full = store[symbol]
      .map((row) => ({
        date: row.date,
        open: Number(row.open),
        high: Number(row.high),
        low: Number(row.low),
        close: Number(row.close)
      }))
      .filter((row) =>
        row.date &&
        Number.isFinite(row.open) &&
        Number.isFinite(row.high) &&
        Number.isFinite(row.low) &&
        Number.isFinite(row.close)
      )
      .sort((a, b) => a.date.localeCompare(b.date));

    if (!full.length) return null;

    const closes = full.map((d) => d.close);
    const sma150 = simpleMovingAverage(closes, 150);
    const ema20 = exponentialMovingAverage(closes, 20);

    const enriched = full.map((row, idx) => ({
      ...row,
      SMA_150: sma150[idx],
      EMA_20: ema20[idx]
    }));

    const visible = HORIZON_LENGTH[horizon] || 30;
    return enriched.slice(-visible);
  }

  function getEmbeddedSpreadRows(pair, horizon) {
    const store = activeDataStore.spreads || {};
    const direct = DISPLAY_PAIR_TO_SYMBOL[pair];

    if (!direct) return null;

    const reverse = reverseSymbol(direct);

    let rows = null;
    let invert = false;

    if (Array.isArray(store[direct]) && store[direct].length) {
      rows = store[direct];
    } else if (Array.isArray(store[reverse]) && store[reverse].length) {
      rows = store[reverse];
      invert = true;
    } else {
      return null;
    }

    const shaped = rows
      .map((row) => ({
        date: row.as_of_date,
        value: invert ? -Number(row.yld_10y_diff) : Number(row.yld_10y_diff),
        base: invert ? row.quote_ccy : row.base_ccy,
        quote: invert ? row.base_ccy : row.quote_ccy,
        yldBase: invert ? Number(row.yld_10y_quote) : Number(row.yld_10y_base),
        yldQuote: invert ? Number(row.yld_10y_base) : Number(row.yld_10y_quote)
      }))
      .filter((row) => row.date && Number.isFinite(row.value))
      .sort((a, b) => a.date.localeCompare(b.date));

    if (!shaped.length) return null;

    const visible = SPREAD_HORIZON_LENGTH[horizon] || 24;
    return shaped.slice(-visible);
  }

  function getSpreadModeConfig(mode) {
    if (mode === "Bond Spread Lens") {
      return {
        badge: "Bond Spread Lens",
        modeLabel: "Δ 10Y Diff",
        subtitle: "Change in 10Y sovereign differential",
        transform: "delta"
      };
    }

    return {
      badge: "Comparative Spread",
      modeLabel: "Comparative",
      subtitle: "10Y sovereign comparative spread",
      transform: "level"
    };
  }

  function buildSpreadSeries(rows, mode) {
    const config = getSpreadModeConfig(mode);
    if (!rows || !rows.length) return [];

    if (config.transform === "delta") {
      return rows
        .map((row, idx) => ({
          ...row,
          value: idx === 0 ? null : row.value - rows[idx - 1].value
        }))
        .filter((row) => Number.isFinite(row.value));
    }

    return rows.map((row) => ({ ...row }));
  }

  function createSeriesLinePath(values, width, height, padding) {
    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = Math.max(max - min, 1e-9);

    const points = values.map((value, index) => {
      const x = padding.left + (index / Math.max(values.length - 1, 1)) * innerW;
      const y = padding.top + ((max - value) / range) * innerH;
      return { x, y, value };
    });

    const path = points.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(" ");
    return { points, path, min, max };
  }

  function renderPriceChart(container, rows, pair) {
    if (!container || !rows || !rows.length) return;

    const width = Math.max(container.clientWidth, 300);
    const height = Math.max(container.clientHeight, 250);
    const padding = { top: 18, right: 16, bottom: 34, left: 16 };

    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;

    const lows = rows.map((d) => d.low);
    const highs = rows.map((d) => d.high);
    const smaVals = rows.map((d) => d.SMA_150).filter((v) => v !== null);
    const emaVals = rows.map((d) => d.EMA_20).filter((v) => v !== null);

    const allVals = [...lows, ...highs, ...smaVals, ...emaVals];
    const min = Math.min(...allVals);
    const max = Math.max(...allVals);
    const range = Math.max(max - min, 1e-9);

    const priceToY = (value) => padding.top + ((max - value) / range) * innerH;
    const candleStep = innerW / Math.max(rows.length, 1);
    const bodyWidth = Math.max(Math.min(candleStep * 0.58, 14), 5);

    const grid = [max, (max + min) / 2, min]
      .map((v, idx) => {
        const y = padding.top + ((idx / 2) * innerH);
        return `
          <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
          <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(v, getPairDigits(pair))}</text>
        `;
      })
      .join("");

    const candles = rows.map((row, idx) => {
      const x = padding.left + idx * candleStep + candleStep / 2;
      const yHigh = priceToY(row.high);
      const yLow = priceToY(row.low);
      const yOpen = priceToY(row.open);
      const yClose = priceToY(row.close);

      const bodyTop = Math.min(yOpen, yClose);
      const bodyHeight = Math.max(Math.abs(yClose - yOpen), 1.5);
      const up = row.close >= row.open;
      const cls = up ? "fx-candle-up" : "fx-candle-down";

      return `
        <line class="fx-candle-wick ${cls}" x1="${x}" y1="${yHigh}" x2="${x}" y2="${yLow}"></line>
        <rect class="fx-candle-body ${cls}" x="${x - bodyWidth / 2}" y="${bodyTop}" width="${bodyWidth}" height="${bodyHeight}" rx="1.5"></rect>
      `;
    }).join("");

    const smaPoints = rows
      .map((row, idx) => row.SMA_150 === null ? null : ({
        x: padding.left + idx * candleStep + candleStep / 2,
        y: priceToY(row.SMA_150)
      }))
      .filter(Boolean);

    const emaPoints = rows
      .map((row, idx) => row.EMA_20 === null ? null : ({
        x: padding.left + idx * candleStep + candleStep / 2,
        y: priceToY(row.EMA_20)
      }))
      .filter(Boolean);

    const smaPath = createLinePath(smaPoints);
    const emaPath = createLinePath(emaPoints);

    const labelIdx = [0, Math.floor((rows.length - 1) / 2), rows.length - 1];
    const xLabels = [...new Set(labelIdx)]
      .map((idx) => {
        const x = padding.left + idx * candleStep + candleStep / 2;
        return `<text class="fx-axis-label" x="${x}" y="${height - 10}" text-anchor="middle">${formatDateLabel(rows[idx].date)}</text>`;
      })
      .join("");

    container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX Price Candlestick Chart">
        ${grid}
        ${candles}
        ${smaPath ? `<path class="fx-sma-line" d="${smaPath}"></path>` : ""}
        ${emaPath ? `<path class="fx-ema-line" d="${emaPath}"></path>` : ""}
        <text class="fx-overlay-label" x="${padding.left + 8}" y="${padding.top + 12}">SMA_150</text>
        <text class="fx-overlay-label ema" x="${padding.left + 76}" y="${padding.top + 12}">EMA_20</text>
        ${xLabels}
      </svg>
    `;
  }

  function renderSpreadChart(container, rows) {
    if (!container || !rows || !rows.length) return;

    const values = rows.map((r) => r.value);
    const width = Math.max(container.clientWidth, 300);
    const height = Math.max(container.clientHeight, 230);
    const padding = { top: 18, right: 16, bottom: 28, left: 16 };

    const { points, path, min, max } = createSeriesLinePath(values, width, height, padding);
    const zeroValue = 0;
    const valueRange = Math.max(max - min, 1e-9);
    const innerH = height - padding.top - padding.bottom;
    const zeroY = padding.top + ((max - zeroValue) / valueRange) * innerH;
    const lastPoint = points[points.length - 1];

    const labelIdx = [0, Math.floor((rows.length - 1) / 2), rows.length - 1];
    const xLabels = [...new Set(labelIdx)]
      .map((idx) => {
        const x = points[idx]?.x ?? padding.left;
        return `<text class="fx-axis-label" x="${x}" y="${height - 8}" text-anchor="middle">${formatDateLabel(rows[idx].date)}</text>`;
      })
      .join("");

    container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX Spread Chart">
        <line class="fx-zero-line" x1="${padding.left}" y1="${zeroY}" x2="${width - padding.right}" y2="${zeroY}"></line>
        <line class="fx-grid-line" x1="${padding.left}" y1="${padding.top}" x2="${width - padding.right}" y2="${padding.top}"></line>
        <line class="fx-grid-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>
        <path class="fx-line-secondary" d="${path}"></path>
        <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
        <text class="fx-axis-label" x="${width - padding.right}" y="${padding.top - 6}" text-anchor="end">${formatNumber(max, 2)}</text>
        <text class="fx-axis-label" x="${width - padding.right}" y="${zeroY - 6}" text-anchor="end">0.00</text>
        <text class="fx-axis-label" x="${width - padding.right}" y="${height - padding.bottom - 6}" text-anchor="end">${formatNumber(min, 2)}</text>
        ${xLabels}
      </svg>
    `;
  }

  function renderSigmaChart(container, rows, selectedPair) {
    if (!container) return;

    if (!rows.length) {
      container.innerHTML = `<div class="panel-placeholder">No active sigma data.</div>`;
      return;
    }

    const width = Math.max(container.clientWidth, 300);
    const height = Math.max(container.clientHeight, 220);
    const padding = { top: 20, right: 16, bottom: 32, left: 16 };
    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;
    const barGap = 14;
    const barCount = rows.length;
    const barWidth = (innerW - (barGap * Math.max(barCount - 1, 0))) / Math.max(barCount, 1);
    const maxAbs = Math.max(...rows.map((r) => Math.abs(r.z)), 0.1);
    const zeroY = padding.top + innerH / 2;

    const bars = rows.map((row, idx) => {
      const x = padding.left + idx * (barWidth + barGap);
      const scaled = (Math.abs(row.z) / maxAbs) * (innerH / 2 - 12);
      const isPositive = row.z >= 0;
      const y = isPositive ? zeroY - scaled : zeroY;
      const h = Math.max(scaled, 4);
      const selectedClass = row.pair === selectedPair ? "selected" : "";

      return `
        <rect class="fx-bar ${selectedClass}" x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="4"></rect>
        <text class="fx-bar-label" x="${x + (barWidth / 2)}" y="${height - 12}" text-anchor="middle">${row.pair}</text>
        <text class="fx-bar-value" x="${x + (barWidth / 2)}" y="${y - 6}" text-anchor="middle">${row.z.toFixed(2)}</text>
      `;
    }).join("");

    container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX Sigma Rank">
        <line class="fx-zero-line" x1="${padding.left}" y1="${zeroY}" x2="${width - padding.right}" y2="${zeroY}"></line>
        ${bars}
      </svg>
    `;
  }

  function updateStatValue(el, text, positiveNegative = null) {
    if (!el) return;
    el.textContent = text;
    el.classList.remove("positive", "negative");

    if (positiveNegative === "positive") el.classList.add("positive");
    if (positiveNegative === "negative") el.classList.add("negative");
  }

  function setChartPlaceholder(id, message) {
    const node = document.getElementById(id);
    if (node) {
      node.innerHTML = `<div class="panel-placeholder">${message}</div>`;
    }
  }

  function renderFX() {
    updateGeoScenToolbarLabel();

    if (!activeDataLoaded) {
      setChartPlaceholder(
        "fx-price-chart",
        activeDataLoadError ? "FX price fetch failed." : "Loading FX price..."
      );
      setChartPlaceholder(
        "fx-spread-chart",
        activeDataLoadError ? "Spread fetch failed." : "Loading spreads..."
      );
      setChartPlaceholder(
        "fx-sigma-chart",
        activeDataLoadError ? "Sigma fetch failed." : "Loading sigma..."
      );
      return;
    }

    const pair = fxControls.pair?.value || getDisplayPairsFromUniverse()[0] || "AUD/USD";
    const spreadMode = fxControls.spreads?.value || "Comparative Spread";
    const horizon = fxControls.horizon?.value || "30D";

    const priceRows = getEmbeddedPriceRows(pair, horizon) || [];
    const spreadRows = getEmbeddedSpreadRows(pair, horizon) || [];
    const spreadSeries = buildSpreadSeries(spreadRows, spreadMode);
    const spreadConfig = getSpreadModeConfig(spreadMode);
    const sigmaRows = getHarmonizedSigmaRows();
    const sigmaRow = sigmaRows.find((row) => row.pair === pair);

    const priceDateEl = document.getElementById("fx-price-date");
    if (priceDateEl) {
      priceDateEl.textContent = priceRows.length ? formatUTC(priceRows[priceRows.length - 1].date) : "--";
    }

    const spreadDateEl = document.getElementById("fx-spread-date");
    if (spreadDateEl) {
      spreadDateEl.textContent = spreadRows.length ? formatUTC(spreadRows[spreadRows.length - 1].date) : "--";
    }

    const sigmaDateEl = document.getElementById("fx-sigma-date");
    if (sigmaDateEl) {
      sigmaDateEl.textContent = sigmaRow?.as_of_date ? formatUTC(sigmaRow.as_of_date) : "--";
    }

    const priceBadge = document.getElementById("fx-price-badge");
    const spreadBadge = document.getElementById("fx-spread-badge");
    const spreadSubtitle = document.getElementById("fx-spread-subtitle");
    const sigmaBadge = document.getElementById("fx-sigma-badge");

    if (priceBadge) priceBadge.textContent = `${pair} • ${horizon}`;
    if (spreadBadge) spreadBadge.textContent = spreadConfig.badge;
    if (spreadSubtitle) spreadSubtitle.textContent = spreadConfig.subtitle;
    if (sigmaBadge) sigmaBadge.textContent = "Cross-Pair Z Context";

    if (!priceRows.length) {
      updateStatValue(document.getElementById("fx-price-last"), "--");
      updateStatValue(document.getElementById("fx-price-change"), "--");
      updateStatValue(document.getElementById("fx-price-range"), "--");
      setChartPlaceholder(
        "fx-price-chart",
        endpointHealth.price ? `No active FX price data for ${pair}.` : "Price endpoint unavailable."
      );
    } else {
      const priceLast = priceRows[priceRows.length - 1].close;
      const priceFirst = priceRows[0].close;
      const priceChangePct = ((priceLast / priceFirst) - 1) * 100;
      const priceMin = Math.min(...priceRows.map((d) => d.low));
      const priceMax = Math.max(...priceRows.map((d) => d.high));
      const priceDigits = getPairDigits(pair);

      updateStatValue(document.getElementById("fx-price-last"), formatNumber(priceLast, priceDigits));
      updateStatValue(
        document.getElementById("fx-price-change"),
        `${priceChangePct >= 0 ? "+" : ""}${formatNumber(priceChangePct, 2)}%`,
        priceChangePct >= 0 ? "positive" : "negative"
      );
      updateStatValue(
        document.getElementById("fx-price-range"),
        `${formatNumber(priceMin, priceDigits)} - ${formatNumber(priceMax, priceDigits)}`
      );

      renderPriceChart(document.getElementById("fx-price-chart"), priceRows, pair);
    }

    if (!spreadSeries.length) {
      updateStatValue(document.getElementById("fx-spread-mode"), "No Data");
      updateStatValue(document.getElementById("fx-spread-last"), "--");
      updateStatValue(document.getElementById("fx-spread-avg"), "--");
      setChartPlaceholder(
        "fx-spread-chart",
        endpointHealth.spreads ? `No spread data for ${pair}.` : "Spread endpoint unavailable."
      );
    } else {
      const spreadValues = spreadSeries.map((d) => d.value);
      const spreadLast = spreadValues[spreadValues.length - 1];
      const spreadAvg = spreadValues.reduce((a, b) => a + b, 0) / spreadValues.length;

      updateStatValue(document.getElementById("fx-spread-mode"), spreadConfig.modeLabel);
      updateStatValue(
        document.getElementById("fx-spread-last"),
        `${spreadLast >= 0 ? "+" : ""}${formatNumber(spreadLast, 2)}`,
        spreadLast >= 0 ? "positive" : "negative"
      );
      updateStatValue(
        document.getElementById("fx-spread-avg"),
        `${spreadAvg >= 0 ? "+" : ""}${formatNumber(spreadAvg, 2)}`,
        spreadAvg >= 0 ? "positive" : "negative"
      );
      renderSpreadChart(document.getElementById("fx-spread-chart"), spreadSeries);
    }

    updateStatValue(document.getElementById("fx-sigma-selected"), pair);
    updateStatValue(
      document.getElementById("fx-sigma-z"),
      sigmaRow ? `${sigmaRow.z >= 0 ? "+" : ""}${sigmaRow.z.toFixed(2)}` : "--",
      sigmaRow ? (sigmaRow.z >= 0 ? "positive" : "negative") : null
    );
    updateStatValue(
      document.getElementById("fx-sigma-rank"),
      sigmaRow ? `${sigmaRow.rank}/${sigmaRows.length}` : "--"
    );

    renderSigmaChart(document.getElementById("fx-sigma-chart"), sigmaRows, pair);
  }

  async function renderWTI() {
    updateWTIGeoScenToolbarLabel();

    const inventorySourceEl = document.getElementById("wti-inventory-source");
    const inventorySourcePrefixEl = document.getElementById("wti-inventory-source-prefix");
    const inventorySourcePrefix =
      inventorySourceEl?.getAttribute("data-source-prefix") || "Source: EIA | the_Spine | As of ";

    if (inventorySourcePrefixEl) {
      inventorySourcePrefixEl.textContent = inventorySourcePrefix;
    }

    const horizon = wtiControls.horizon?.value || "30D";
    const WTI_PRICE_URL =
      "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/wti/wti_price_data.json";
    const WTI_INVENTORY_PANEL_URL =
      "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/wti/wti_inventory_panel_data.json";

    const HORIZON_LENGTH_WTI = {
      "5D": 5,
      "15D": 15,
      "30D": 30,
      "45D": 45
    };

    const HORIZON_LENGTH_WTI_WEEKLY = {
      "5D": 5,
      "15D": 8,
      "30D": 12,
      "45D": 18
    };

    function ensureWtiInventoryControls() {
      const toolbar = document.querySelector("#view-wti .fx-toolbar");
      if (!toolbar) return;

      let viewGroup = document.getElementById("wti-inventory-view-group");
      if (!viewGroup) {
        viewGroup = document.createElement("div");
        viewGroup.className = "fx-control-group";
        viewGroup.id = "wti-inventory-view-group";
        viewGroup.innerHTML = `
          <label for="wti-inventory-view">View Mode</label>
          <select id="wti-inventory-view">
            <option value="ytd_structure" selected>YTD Structure</option>
            <option value="inventory_pressure">Inventory Pressure</option>
            <option value="seasonal_stress">Seasonal Stress</option>
          </select>
        `;
        toolbar.appendChild(viewGroup);
      }

      let regimeGroup = document.getElementById("wti-inventory-regime-group");
      if (!regimeGroup) {
        regimeGroup = document.createElement("div");
        regimeGroup.className = "fx-control-group";
        regimeGroup.id = "wti-inventory-regime-group";
        regimeGroup.innerHTML = `
          <label for="wti-inventory-regime">Regime Overlay</label>
          <select id="wti-inventory-regime">
            <option value="none" selected>None</option>
            <option value="tight_inventory">Tight Inventory</option>
            <option value="surplus_inventory">Surplus Inventory</option>
            <option value="stress_events">Stress Events</option>
          </select>
        `;
        toolbar.appendChild(regimeGroup);
      }

      return {
        view: document.getElementById("wti-inventory-view"),
        regime: document.getElementById("wti-inventory-regime")
      };
    }

    function renderWTIPriceChart(container, rows) {
      if (!container || !rows || !rows.length) return;

      const width = Math.max(container.clientWidth, 300);
      const height = Math.max(container.clientHeight, 230);
      const padding = { top: 18, right: 16, bottom: 34, left: 16 };

      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const closes = rows.map((r) => r.close);
      const smaVals = rows.map((r) => r.sma_150).filter((v) => v !== null);
      const emaVals = rows.map((r) => r.ema_20).filter((v) => v !== null);

      const allVals = [...closes, ...smaVals, ...emaVals];
      const min = Math.min(...allVals);
      const max = Math.max(...allVals);
      const range = Math.max(max - min, 1e-9);

      const valueToY = (value) => padding.top + ((max - value) / range) * innerH;
      const step = innerW / Math.max(rows.length - 1, 1);

      const pricePoints = rows.map((row, idx) => ({
        x: padding.left + idx * step,
        y: valueToY(row.close)
      }));

      const smaPoints = rows
        .map((row, idx) =>
          row.sma_150 == null
            ? null
            : {
                x: padding.left + idx * step,
                y: valueToY(row.sma_150)
              }
        )
        .filter(Boolean);

      const emaPoints = rows
        .map((row, idx) =>
          row.ema_20 == null
            ? null
            : {
                x: padding.left + idx * step,
                y: valueToY(row.ema_20)
              }
        )
        .filter(Boolean);

      const pricePath = createLinePath(pricePoints);
      const smaPath = createLinePath(smaPoints);
      const emaPath = createLinePath(emaPoints);

      const yTicks = [max, (max + min) / 2, min]
        .map((v, idx) => {
          const y = padding.top + (idx / 2) * innerH;
          return `
            <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
            <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(v, 2)}</text>
          `;
        })
        .join("");

      const labelIdx = [0, Math.floor((rows.length - 1) / 2), rows.length - 1];
      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const x = padding.left + idx * step;
          return `<text class="fx-axis-label" x="${x}" y="${height - 10}" text-anchor="middle">${formatDateLabel(rows[idx].date)}</text>`;
        })
        .join("");

      const lastPoint = pricePoints[pricePoints.length - 1];

      container.innerHTML = `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="WTI Price Chart">
          ${yTicks}
          <path class="fx-line-secondary" d="${pricePath}"></path>
          ${smaPath ? `<path class="fx-sma-line" d="${smaPath}"></path>` : ""}
          ${emaPath ? `<path class="fx-ema-line" d="${emaPath}"></path>` : ""}
          <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
          <text class="fx-overlay-label" x="${padding.left + 8}" y="${padding.top + 12}">WTI</text>
          <text class="fx-overlay-label" x="${padding.left + 48}" y="${padding.top + 12}">SMA_150</text>
          <text class="fx-overlay-label ema" x="${padding.left + 124}" y="${padding.top + 12}">EMA_20</text>
          ${xLabels}
        </svg>
      `;
    }

    function renderBandChart(container, rows, overlaySet) {
      if (!container || !rows?.length) return;

      const filtered = rows.filter((r) => Number.isFinite(r.week_num));
      if (!filtered.length) {
        setChartPlaceholder("wti-inventory-chart", "No YTD structure data available.");
        return;
      }

      const width = Math.max(container.clientWidth, 300);
      const height = Math.max(container.clientHeight, 230);
      const padding = { top: 18, right: 16, bottom: 34, left: 16 };
      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const values = [];
      filtered.forEach((r) => {
        ["historical_min", "historical_avg", "historical_max", "current_value"].forEach((k) => {
          if (Number.isFinite(r[k])) values.push(r[k]);
        });
      });

      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = Math.max(max - min, 1e-9);

      const valueToY = (value) => padding.top + ((max - value) / range) * innerH;
      const step = innerW / Math.max(filtered.length - 1, 1);

      function pointsFor(key) {
        return filtered
          .map((row, idx) => Number.isFinite(row[key]) ? ({
            x: padding.left + idx * step,
            y: valueToY(row[key]),
            date: row.date
          }) : null)
          .filter(Boolean);
      }

      const minPoints = pointsFor("historical_min");
      const avgPoints = pointsFor("historical_avg");
      const maxPoints = pointsFor("historical_max");
      const curPoints = pointsFor("current_value");

      const bandPath = `
        ${createLinePath(maxPoints)}
        L ${[...minPoints].reverse().map((p) => `${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(" L ")}
        Z
      `;

      const overlayMarks = filtered
        .filter((r) => overlaySet.has(r.date))
        .map((row, idx) => {
          const x = padding.left + idx * step;
          return `<line x1="${x}" y1="${padding.top}" x2="${x}" y2="${height - padding.bottom}" stroke="rgba(213,179,124,0.25)" stroke-width="2"></line>`;
        })
        .join("");

      const yTicks = [max, (max + min) / 2, min]
        .map((v, idx) => {
          const y = padding.top + (idx / 2) * innerH;
          return `
            <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
            <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(v, 2)}</text>
          `;
        })
        .join("");

      const labelIdx = [0, Math.floor((filtered.length - 1) / 2), filtered.length - 1];
      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const x = padding.left + idx * step;
          const label = `W${filtered[idx].week_num}`;
          return `<text class="fx-axis-label" x="${x}" y="${height - 10}" text-anchor="middle">${label}</text>`;
        })
        .join("");

      const lastPoint = curPoints[curPoints.length - 1];

      container.innerHTML = `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="WTI Inventory YTD Structure">
          ${yTicks}
          ${overlayMarks}
          <path d="${bandPath}" fill="rgba(126, 153, 196, 0.14)" stroke="none"></path>
          <path class="fx-line-secondary" d="${createLinePath(avgPoints)}"></path>
          <path d="${createLinePath(curPoints)}" fill="none" stroke="#f2d8a7" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"></path>
          <path d="${createLinePath(minPoints)}" fill="none" stroke="rgba(126, 153, 196, 0.55)" stroke-width="1.2" stroke-dasharray="4 4"></path>
          <path d="${createLinePath(maxPoints)}" fill="none" stroke="rgba(126, 153, 196, 0.55)" stroke-width="1.2" stroke-dasharray="4 4"></path>
          ${lastPoint ? `<circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>` : ""}
          <text class="fx-overlay-label" x="${padding.left + 8}" y="${padding.top + 12}">Current</text>
          <text class="fx-overlay-label" x="${padding.left + 58}" y="${padding.top + 12}">Hist Avg</text>
          <text class="fx-overlay-label ema" x="${padding.left + 122}" y="${padding.top + 12}">Hist Band</text>
          ${xLabels}
        </svg>
      `;
    }

    function renderStateChart(container, rows, primaryKey, secondaryKey, overlaySet, primaryLabel, secondaryLabel = null) {
      if (!container || !rows?.length) return;

      const filtered = rows.filter((r) => Number.isFinite(r[primaryKey]));
      if (!filtered.length) {
        setChartPlaceholder("wti-inventory-chart", "No inventory state data available.");
        return;
      }

      const width = Math.max(container.clientWidth, 300);
      const height = Math.max(container.clientHeight, 230);
      const padding = { top: 18, right: 16, bottom: 34, left: 16 };
      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const values = [];
      filtered.forEach((r) => {
        if (Number.isFinite(r[primaryKey])) values.push(r[primaryKey]);
        if (secondaryKey && Number.isFinite(r[secondaryKey])) values.push(r[secondaryKey]);
      });

      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = Math.max(max - min, 1e-9);

      const valueToY = (value) => padding.top + ((max - value) / range) * innerH;
      const step = innerW / Math.max(filtered.length - 1, 1);

      function pointsFor(key) {
        return filtered
          .map((row, idx) => Number.isFinite(row[key]) ? ({
            x: padding.left + idx * step,
            y: valueToY(row[key]),
            date: row.date
          }) : null)
          .filter(Boolean);
      }

      const primaryPoints = pointsFor(primaryKey);
      const secondaryPoints = secondaryKey ? pointsFor(secondaryKey) : [];
      const overlayMarks = filtered
        .filter((r) => overlaySet.has(r.date))
        .map((row, idx) => {
          const x = padding.left + idx * step;
          return `<line x1="${x}" y1="${padding.top}" x2="${x}" y2="${height - padding.bottom}" stroke="rgba(213,179,124,0.25)" stroke-width="2"></line>`;
        })
        .join("");

      const zeroY = (min <= 0 && max >= 0) ? valueToY(0) : null;

      const yTicks = [max, (max + min) / 2, min]
        .map((v, idx) => {
          const y = padding.top + (idx / 2) * innerH;
          return `
            <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
            <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(v, 2)}</text>
          `;
        })
        .join("");

      const labelIdx = [0, Math.floor((filtered.length - 1) / 2), filtered.length - 1];
      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const x = padding.left + idx * step;
          return `<text class="fx-axis-label" x="${x}" y="${height - 10}" text-anchor="middle">${formatDateLabel(filtered[idx].date)}</text>`;
        })
        .join("");

      const lastPoint = primaryPoints[primaryPoints.length - 1];

      container.innerHTML = `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="WTI Inventory State Chart">
          ${yTicks}
          ${overlayMarks}
          ${zeroY !== null ? `<line class="fx-zero-line" x1="${padding.left}" y1="${zeroY}" x2="${width - padding.right}" y2="${zeroY}"></line>` : ""}
          <path d="${createLinePath(primaryPoints)}" fill="none" stroke="#f2d8a7" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"></path>
          ${secondaryPoints.length ? `<path class="fx-ema-line" d="${createLinePath(secondaryPoints)}"></path>` : ""}
          ${lastPoint ? `<circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>` : ""}
          <text class="fx-overlay-label" x="${padding.left + 8}" y="${padding.top + 12}">${primaryLabel}</text>
          ${secondaryLabel ? `<text class="fx-overlay-label ema" x="${padding.left + 118}" y="${padding.top + 12}">${secondaryLabel}</text>` : ""}
          ${xLabels}
        </svg>
      `;
    }

    const inventoryDate = document.getElementById("wti-inventory-date");
    const priceDate = document.getElementById("wti-price-date");
    const macroDate = document.getElementById("wti-macro-date");
    const sigmaDate = document.getElementById("wti-sigma-date");

    if (macroDate) macroDate.textContent = "--";
    if (sigmaDate) sigmaDate.textContent = "--";

    const inventoryBadge = document.getElementById("wti-inventory-badge");
    const priceBadge = document.getElementById("wti-price-badge");
    const macroBadge = document.getElementById("wti-macro-badge");
    const sigmaBadge = document.getElementById("wti-sigma-badge");

    if (inventoryBadge) inventoryBadge.textContent = horizon;
    if (priceBadge) priceBadge.textContent = `WTI • ${horizon}`;
    if (macroBadge) macroBadge.textContent = "CPI / Macro";
    if (sigmaBadge) sigmaBadge.textContent = "Cross-Series Z Context";

    updateStatValue(document.getElementById("wti-macro-cpi"), "--");
    updateStatValue(document.getElementById("wti-macro-trend"), "--");
    updateStatValue(document.getElementById("wti-macro-state"), "--");

    updateStatValue(document.getElementById("wti-sigma-selected"), "WTI");
    updateStatValue(document.getElementById("wti-sigma-z"), "--");
    updateStatValue(document.getElementById("wti-sigma-rank"), "--");

    setChartPlaceholder("wti-macro-chart", "No inflation data available.");
    setChartPlaceholder("wti-sigma-chart", "No sigma data available.");

    const inventoryControls = ensureWtiInventoryControls();
    const selectedView = inventoryControls?.view?.value || "ytd_structure";
    const selectedRegime = inventoryControls?.regime?.value || "none";
    const selectedOcOverlay = wtiControls.ocOverlay?.value || "off";
    const ocOverlayOn = selectedOcOverlay === "on";

    const inventoryPanelEl = document.querySelector(".wti-inventory-panel");

    if (inventoryPanelEl) {
      inventoryPanelEl.classList.toggle("wti-oc-active", ocOverlayOn);
    }

    if (inventorySourceEl) {
      inventorySourceEl.setAttribute(
        "data-source-prefix",
        ocOverlayOn
          ? "Source: EIA | the_Spine (+ OC Overlay) | As of "
          : "Source: EIA | the_Spine | As of "
      );
    }

    try {
      const inventoryPayload = await fetchJsonWithBust(WTI_INVENTORY_PANEL_URL);

      const summary = inventoryPayload?.summary || {};
      const viewData = inventoryPayload?.view_data || {};
      const overlayData = inventoryPayload?.regime_overlay_data || {};

      const selectedOverlayRows =
        ocOverlayOn && selectedRegime !== "none" && overlayData[selectedRegime]?.rows
          ? overlayData[selectedRegime].rows
          : [];

      const overlaySet = new Set(selectedOverlayRows.map((r) => r.date));

      if (selectedView === "ytd_structure") {
        const allYtdRows = (viewData.ytd_structure?.rows || [])
          .filter((row) => row?.date)
          .map((row) => {
            const d = new Date(`${row.date}T00:00:00`);
            return Number.isNaN(d.getTime())
              ? null
              : {
                  ...row,
                  __dt: d
                };
          })
          .filter(Boolean)
          .sort((a, b) => a.__dt - b.__dt);

        const latestRow = allYtdRows.length ? allYtdRows[allYtdRows.length - 1] : null;
        const latestDataYear = latestRow ? latestRow.__dt.getFullYear() : null;

        const rows = latestDataYear == null
          ? []
          : allYtdRows.filter((row) => row.__dt.getFullYear() === latestDataYear);

        const ytdSummary = summary.ytd_structure || {};

        updateStatValue(
          document.getElementById("wti-inventory-last"),
          ytdSummary.current_value != null ? formatNumber(ytdSummary.current_value, 2) : "--"
        );

        updateStatValue(
          document.getElementById("wti-inventory-change"),
          ytdSummary.position_vs_avg_pct != null
            ? `${ytdSummary.position_vs_avg_pct >= 0 ? "+" : ""}${formatNumber(ytdSummary.position_vs_avg_pct, 2)}%`
            : "--",
          ytdSummary.position_vs_avg_pct != null
            ? (ytdSummary.position_vs_avg_pct >= 0 ? "positive" : "negative")
            : null
        );

        updateStatValue(
          document.getElementById("wti-inventory-range"),
          ytdSummary.historical_min != null && ytdSummary.historical_max != null
            ? `${formatNumber(ytdSummary.historical_min, 2)} - ${formatNumber(ytdSummary.historical_max, 2)}`
            : "--"
        );

        if (inventoryDate) {
          inventoryDate.textContent =
            rows.length ? formatUTC(rows[rows.length - 1].date) : "--";
        }

        if (!rows.length) {
          setChartPlaceholder("wti-inventory-chart", "No YTD structure data available.");
        } else {
          renderBandChart(document.getElementById("wti-inventory-chart"), rows, overlaySet);
        }
      } else if (selectedView === "inventory_pressure") {
        const rows = (viewData.inventory_pressure?.rows || []).slice(
          -(HORIZON_LENGTH_WTI_WEEKLY[horizon] || 12)
        );
        const pSummary = summary.inventory_pressure || {};

        updateStatValue(
          document.getElementById("wti-inventory-last"),
          pSummary.stor_sprd_idx != null ? formatNumber(pSummary.stor_sprd_idx, 2) : "--"
        );
        updateStatValue(
          document.getElementById("wti-inventory-change"),
          pSummary.inv_std_dev_position != null
            ? `${pSummary.inv_std_dev_position >= 0 ? "+" : ""}${formatNumber(pSummary.inv_std_dev_position, 2)}`
            : "--",
          pSummary.inv_std_dev_position != null
            ? (pSummary.inv_std_dev_position >= 0 ? "positive" : "negative")
            : null
        );
        updateStatValue(
          document.getElementById("wti-inventory-range"),
          pSummary.stress_flag || "--"
        );

        if (inventoryDate) {
          inventoryDate.textContent = rows.length ? formatUTC(rows[rows.length - 1].date) : "--";
        }

        renderStateChart(
          document.getElementById("wti-inventory-chart"),
          rows,
          "stor_sprd_idx",
          "inv_std_dev_position",
          overlaySet,
          "Storage Spread",
          "Std Dev"
        );
      } else {
        const rows = (viewData.seasonal_stress?.rows || []).slice(
          -(HORIZON_LENGTH_WTI_WEEKLY[horizon] || 12)
        );
        const sSummary = summary.seasonal_stress || {};

        updateStatValue(
          document.getElementById("wti-inventory-last"),
          sSummary.seas_inv_idx != null ? formatNumber(sSummary.seas_inv_idx, 3) : "--"
        );
        updateStatValue(
          document.getElementById("wti-inventory-change"),
          sSummary.stress_flag || "--",
          sSummary.stress_flag === "tight" ? "negative" : null
        );
        updateStatValue(
          document.getElementById("wti-inventory-range"),
          selectedRegime === "none" ? "Seasonal" : selectedRegime.replaceAll("_", " ")
        );

        if (inventoryDate) {
          inventoryDate.textContent = rows.length ? formatUTC(rows[rows.length - 1].date) : "--";
        }

        renderStateChart(
          document.getElementById("wti-inventory-chart"),
          rows,
          "seas_inv_idx",
          null,
          overlaySet,
          "Seasonal Index"
        );
      }
    } catch (error) {
      console.error("WTI inventory panel fetch failed:", error);
      updateStatValue(document.getElementById("wti-inventory-last"), "--");
      updateStatValue(document.getElementById("wti-inventory-change"), "--");
      updateStatValue(document.getElementById("wti-inventory-range"), "--");
      setChartPlaceholder("wti-inventory-chart", "WTI inventory panel fetch failed.");
      if (inventoryDate) inventoryDate.textContent = "--";
    }

    try {
      const payload = await fetchJsonWithBust(WTI_PRICE_URL);
      const series = Array.isArray(payload?.series) ? payload.series : [];

      const normalized = series
        .map((row) => ({
          date: row.date,
          close: Number(row.close)
        }))
        .filter((row) => row.date && Number.isFinite(row.close))
        .sort((a, b) => a.date.localeCompare(b.date));

      if (!normalized.length) {
        updateStatValue(document.getElementById("wti-price-last"), "--");
        updateStatValue(document.getElementById("wti-price-change"), "--");
        updateStatValue(document.getElementById("wti-price-range"), "--");
        setChartPlaceholder("wti-price-chart", "No active WTI price data.");
        if (priceDate) priceDate.textContent = "--";
        return;
      }

      const closes = normalized.map((r) => r.close);
      const sma150 = simpleMovingAverage(closes, 150);
      const ema20 = exponentialMovingAverage(closes, 20);

      const enriched = normalized.map((row, idx) => ({
        ...row,
        sma_150: sma150[idx],
        ema_20: ema20[idx]
      }));

      const visible = HORIZON_LENGTH_WTI[horizon] || 30;
      const rows = enriched.slice(-visible);

      const last = rows[rows.length - 1].close;
      const first = rows[0].close;
      const changePct = ((last / first) - 1) * 100;
      const min = Math.min(...rows.map((r) => r.close));
      const max = Math.max(...rows.map((r) => r.close));

      updateStatValue(document.getElementById("wti-price-last"), formatNumber(last, 2));
      updateStatValue(
        document.getElementById("wti-price-change"),
        `${changePct >= 0 ? "+" : ""}${formatNumber(changePct, 2)}%`,
        changePct >= 0 ? "positive" : "negative"
      );
      updateStatValue(
        document.getElementById("wti-price-range"),
        `${formatNumber(min, 2)} - ${formatNumber(max, 2)}`
      );

      if (priceDate) {
        priceDate.textContent = formatUTC(rows[rows.length - 1].date);
      }

      renderWTIPriceChart(document.getElementById("wti-price-chart"), rows);
    } catch (error) {
      console.error("WTI price fetch failed:", error);
      updateStatValue(document.getElementById("wti-price-last"), "--");
      updateStatValue(document.getElementById("wti-price-change"), "--");
      updateStatValue(document.getElementById("wti-price-range"), "--");
      setChartPlaceholder("wti-price-chart", "WTI price fetch failed.");
      if (priceDate) priceDate.textContent = "--";
    }
  }

  function renderFXDeferred() {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        renderFX();
      });
    });
  }

  navParents.forEach((parent) => {
    parent.addEventListener("click", () => {
      if (parent.dataset.locked === "true") return;

      const viewName = parent.dataset.view;
      const parentKey = parent.dataset.parent;

      if (parentKey) {
        openSubnav(parentKey);
      }

      if (viewName) {
        showView(viewName);
      }
    });
  });

  viewButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (button.disabled) return;

      const viewName = button.dataset.view;
      if (!viewName) return;

      showView(viewName);
    });
  });

  Object.values(fxControls).forEach((el) => {
    if (el) {
      el.addEventListener("change", () => {
        updateGeoScenToolbarLabel();

        if (document.body.classList.contains("view-fx")) {
          renderFXDeferred();
        }
      });
    }
  });

  Object.values(wtiControls).forEach((el) => {
    if (el) {
      el.addEventListener("change", () => {
        updateWTIGeoScenToolbarLabel();

        if (document.body.classList.contains("view-wti")) {
          window.requestAnimationFrame(() => {
            window.requestAnimationFrame(() => {
              void renderWTI();
            });
          });
        }
      });
    }
  });

  Object.values(equitiesControls).forEach((el) => {
    if (el) {
      el.addEventListener("change", () => {
        updateEquitiesGeoScenToolbarLabel();

        if (document.body.classList.contains("view-equities")) {
          renderEquities();
        }
      });
    }
  });

  document.addEventListener("change", (event) => {
    const target = event.target;
    if (!target) return;

    if (target.id === "wti-inventory-view" || target.id === "wti-inventory-regime") {
      if (document.body.classList.contains("view-wti")) {
        void renderWTI();
      }
    }
  });

  window.addEventListener("resize", () => {
    if (document.body.classList.contains("view-fx")) {
      renderFXDeferred();
    }

    if (document.body.classList.contains("view-wti")) {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          void renderWTI();
        });
      });
    }

    if (document.body.classList.contains("view-equities")) {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          renderEquities();
        });
      });
    }
  });

  (async function boot() {
    try {
      await loadActiveData();
      populateFxPairOptions();
    } catch (error) {
      activeDataLoadError = error;
      console.error("Initial active FX data load failed:", error);
    }

    startActiveRefreshLoop();
    showView("what-is");
  })();
});

