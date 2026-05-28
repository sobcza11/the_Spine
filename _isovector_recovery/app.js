document.addEventListener("DOMContentLoaded", () => {
  const viewButtons = document.querySelectorAll("[data-view]");
  const contentViews = document.querySelectorAll(".content-view");
  const navParents = document.querySelectorAll(".nav-parent");
  const subnavs = document.querySelectorAll(".subnav");
  const moduleTabs = document.querySelectorAll(".module-tab[data-view], .top-nav-item[data-view]");

  const fxControls = {
    pair: document.getElementById("fx-pair"),
    spreads: document.getElementById("fx-spreads"),
    horizon: document.getElementById("fx-horizon"),
    geoscen: document.getElementById("fx-geoscen-mode")
  };

  const equitiesControls = {
    region: document.getElementById("equities-region"),
    etfFocus: document.getElementById("equities-etf-focus"),
    horizon: document.getElementById("equities-horizon"),
    topRightMode: document.getElementById("equities-top-right-mode"),
    geoscen: document.getElementById("equities-geoscen-mode")
  };

  const wtiControls = {
    horizon: document.getElementById("wti-horizon"),
    geoscen: document.getElementById("wti-geoscen-mode"),
    ocOverlay: document.getElementById("wti-oc-overlay")
  };

  const macroControls = {
    region: document.getElementById("macro-region"),
    horizon: document.getElementById("macro-horizon"),
    regimeFilter: document.getElementById("macro-regime-filter"),
    dataMode: document.getElementById("macro-data-mode")
  };

const WTI_PANEL_URL = "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/wti/wti_panel.json";

const DATA_ENDPOINTS = {
  price: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_price_data.json",
  spreads: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_spreads_data.json",
  sigma: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_sigma_data.json",
  universe: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_universe.json",

  equitiesIndex: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/us_equity_index_data.json",
  equitiesBreadth: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/etf_pmi_breadth_by_etf.json",
  equitiesSigma: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/equities_sigma_rank.json",
  equitiesIndustryPanel: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/industry_panel_serving.json",
  
  wtiPrice: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/wti/wti_price_data.json"
};

const EQUITIES_MARKET_INDEXES = ["SPY", "QQQ", "DIA", "ITOT", "MDY", "IWM"];
const EQUITIES_SECTOR_ETFS = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"];

const EQUITIES_LENS_CONFIG = {
  SPY: {
    universe: "Market Indexes",
    lens_primary: "Broad Market",
    lens_secondary: "Blend",
    growth_defensive: "Balanced",
    cyclical_defensive: "Balanced",
    size_profile: "Large Cap",
    notes: ["Core U.S. beta", "Broad macro read", "Blended exposure"]
  },
  QQQ: {
    universe: "Market Indexes",
    lens_primary: "Growth",
    lens_secondary: "Innovation",
    growth_defensive: "Growth",
    cyclical_defensive: "Cyclical",
    size_profile: "Large Cap",
    notes: ["Tech concentration", "Low defensive mix", "Duration-sensitive"]
  },
  DIA: {
    universe: "Market Indexes",
    lens_primary: "Legacy Quality",
    lens_secondary: "Industrial / Value Tilt",
    growth_defensive: "Balanced",
    cyclical_defensive: "Balanced",
    size_profile: "Large Cap",
    notes: ["Old-economy tilt", "Quality cyclicals", "Industrial sensitivity"]
  },
  ITOT: {
    universe: "Market Indexes",
    lens_primary: "Total Market",
    lens_secondary: "Blend",
    growth_defensive: "Balanced",
    cyclical_defensive: "Balanced",
    size_profile: "All Cap",
    notes: ["Full U.S. market", "Benchmark baseline", "Broad participation"]
  },
  MDY: {
    universe: "Market Indexes",
    lens_primary: "Mid Cap",
    lens_secondary: "Domestic Cyclicality",
    growth_defensive: "Balanced",
    cyclical_defensive: "Cyclical",
    size_profile: "Mid Cap",
    notes: ["Economic sensitivity", "Mid-cap breadth", "Domestic exposure"]
  },
  IWM: {
    universe: "Market Indexes",
    lens_primary: "Small Cap",
    lens_secondary: "Domestic Sensitivity",
    growth_defensive: "Balanced",
    cyclical_defensive: "Cyclical",
    size_profile: "Small Cap",
    notes: ["Higher beta", "Domestic cyclicality", "Financial conditions sensitive"]
  }
};

window.addEventListener("error", function (e) {
  console.error("GLOBAL ERROR CAUGHT:", e.error);
});

function isSectorEtf(symbol) {
  return EQUITIES_SECTOR_ETFS.includes(String(symbol || "").toUpperCase());
}

function isMarketIndexEtf(symbol) {
  return EQUITIES_MARKET_INDEXES.includes(String(symbol || "").toUpperCase());
}

function normalizeIndustryPanelPayload(payload) {
  if (payload && typeof payload === "object" && Array.isArray(payload.rows)) return payload.rows;
  if (Array.isArray(payload)) return payload;
  return [];
}

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

function renderEquitiesLensCard(container, symbol) {
  if (!container) return;

  const cfg = EQUITIES_LENS_CONFIG[String(symbol || "").toUpperCase()];
  if (!cfg) {
    container.innerHTML = `<div class="panel-placeholder">No lens profile available.</div>`;
    return;
  }

  const notes = (cfg.notes || [])
    .map((note) => `<li>${note}</li>`)
    .join("");

  container.innerHTML = `
    <div class="equities-lens-grid">
      <div class="equities-lens-tile">
        <div class="label">Lens</div>
        <div class="value">${cfg.lens_primary}</div>
      </div>
      <div class="equities-lens-tile">
        <div class="label">Sub-Lens</div>
        <div class="value">${cfg.lens_secondary}</div>
      </div>
      <div class="equities-lens-tile">
        <div class="label">Growth / Defensive</div>
        <div class="value">${cfg.growth_defensive}</div>
      </div>
      <div class="equities-lens-tile">
        <div class="label">Cyclical / Defensive</div>
        <div class="value">${cfg.cyclical_defensive}</div>
      </div>
      <div class="equities-lens-tile">
        <div class="label">Size</div>
        <div class="value">${cfg.size_profile}</div>
      </div>
      <div class="equities-lens-tile">
        <div class="label">Universe</div>
        <div class="value">${cfg.universe}</div>
      </div>
    </div>
    <div class="equities-lens-notes">
      <div class="equities-industry-section-title">Context</div>
      <ul>${notes}</ul>
    </div>
  `;
}

console.log({
  region: document.getElementById("equities-region"),
  etf: document.getElementById("equities-etf-focus"),
  horizon: document.getElementById("equities-horizon"),
  topRight: document.getElementById("equities-top-right-mode"),
  geoscen: document.getElementById("equities-geoscen-mode")
});

function normalizeEquitiesSigmaPayload(payload) {
  return Array.isArray(payload) ? payload : [];
}
  
function getEquitiesIndexUniverse() {
  return ["SPY", "QQQ", "DIA", "ITOT", "MDY", "IWM"];
}

async function loadWtiPanel() {
  return fetchJsonWithBust(WTI_PANEL_URL);
}

async function loadWtiPriceData() {
  const response = await fetchJsonWithBust(DATA_ENDPOINTS.wtiPrice);

  if (Array.isArray(response)) {
    return {
      rows: response,
      meta: {}
    };
  }

  if (response && typeof response === "object" && Array.isArray(response.series)) {
    return {
      rows: response.series,
      meta: response.meta || {}
    };
  }

  return {
    rows: [],
    meta: {}
  };
}

function normalizeEquitiesIndexPayload(payload) {
  return payload && typeof payload === "object" ? payload : {};
}

function normalizeEquitiesPmiSeriesPayload(payload) {
  return Array.isArray(payload) ? payload : [];
}

async function loadEquitiesData() {
  const results = await Promise.allSettled([
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesIndex),
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesIndustryPanel),
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesBreadth),
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesSigma)
  ]);

  const [indexRes, panelRes, breadthRes, sigmaRes] = results;

  const indexOk = indexRes.status === "fulfilled";
  const panelOk = panelRes.status === "fulfilled";
  const breadthOk = breadthRes.status === "fulfilled";
  const sigmaOk = sigmaRes.status === "fulfilled";

  return {
    index: indexOk
      ? normalizeEquitiesIndexPayload(indexRes.value)
      : {},

    industryPanel: panelOk
      ? normalizeIndustryPanelPayload(panelRes.value)
      : [],

    breadth: breadthOk
      ? normalizeEquitiesPmiSeriesPayload(breadthRes.value)
      : [],

    sigma: sigmaOk
      ? normalizeEquitiesSigmaPayload(sigmaRes.value)
      : [],

    health: {
      index: indexOk,
      industryPanel: panelOk,
      breadth: breadthOk,
      sigma: sigmaOk
    }
  };
}

function renderIndustryPanelTable(container, rows, etfFocus) {
  if (!container) return;

  const etfRows = rows.filter((r) => String(r.etf || "").toUpperCase() === etfFocus);

  if (!etfRows.length) {
    container.innerHTML = `<div class="panel-placeholder">No industry panel data available for ${etfFocus}.</div>`;
    return;
  }

  const sections = ["manu", "serv"]
    .map((bucket) => {
      const bucketRows = etfRows
        .filter((r) => String(r.pmi_type || "").toLowerCase() === bucket)
        .sort((a, b) => Math.abs(Number(b.Sig || 0)) - Math.abs(Number(a.Sig || 0)));

      if (!bucketRows.length) return "";

      const label = bucket === "manu" ? "Manufacturing" : "Services";

      const body = bucketRows.map((row) => `
        <tr>
          <td>${row.industry}</td>
          <td class="num pmi-col">${formatInt(row.pmi_Idx)}</td>
          <td class="num pmi-col">${formatSignedInt(row["pmi_3M_D"])}</td>
          <td class="num pmi-col">${formatSignedInt(row["pmi_6M_D"])}</td>

          <td class="num group-sep-left orders-col">${formatInt(row.no_Idx)}</td>
          <td class="num orders-col">${formatSignedInt(row["no_3M_D"])}</td>
          <td class="num orders-col">${formatSignedInt(row["no_6M_D"])}</td>

          <td class="num group-sep-left">${formatSignedInt(row.Sig)}</td>
          <td class="num">${formatNumber(row.Wt, 1)}</td>
        </tr>
      `).join("");

      return `
        <div class="equities-industry-section">
          <div class="equities-industry-section-title">${label}</div>
          <table class="equities-industry-table equities-industry-table-combined">
            <thead>
              <tr>
                <th rowspan="2">Industry</th>
                <th colspan="3" class="group-head">PMI</th>
                <th colspan="3" class="group-head group-sep-left">New Orders</th>
                <th colspan="2" class="group-head group-sep-left"></th>
              </tr>
              <tr>
                <th>Idx</th>
                <th>3M Δ</th>
                <th>6M Δ</th>
                <th class="group-sep-left">Idx</th>
                <th>3M Δ</th>
                <th>6M Δ</th>
                <th class="group-sep-left">Sig</th>
                <th>Wt</th>
              </tr>
            </thead>
            <tbody>${body}</tbody>
          </table>
        </div>
      `;
    })
    .join("");

  container.innerHTML = `<div class="equities-industry-table-wrap">${sections}</div>`;
}

function renderEquitiesIndexPlaceholder(containerId, title) {
  const node = document.getElementById(containerId);
  if (!node) return;
  node.innerHTML = `<div class="panel-placeholder">${title}</div>`;
}





function coerceEquitiesIndexSeries(payload) {
  const allowed = new Set(getEquitiesIndexUniverse());

  if (Array.isArray(payload)) {
    const grouped = {};
    payload.forEach((row) => {
      const symbol = String(
        row.symbol ?? row.ticker ?? row.etf ?? row.index ?? ""
      ).toUpperCase().trim();

      const date = row.date ?? row.as_of_date ?? row.timestamp ?? null;
      const closeRaw =
        row.close ?? row.price ?? row.last ?? row.value ?? row.adjClose ?? null;

      const close = Number(closeRaw);

      if (!allowed.has(symbol) || !date || !Number.isFinite(close)) return;

      if (!grouped[symbol]) grouped[symbol] = [];
      grouped[symbol].push({ date: String(date), close });
    });

    Object.values(grouped).forEach((rows) => {
      rows.sort((a, b) => String(a.date).localeCompare(String(b.date)));
    });

    return grouped;
  }

  if (payload && typeof payload === "object") {
    const grouped = {};

    Object.entries(payload).forEach(([symbolRaw, rows]) => {
      const symbol = String(symbolRaw).toUpperCase().trim();
      if (!allowed.has(symbol) || !Array.isArray(rows)) return;

      grouped[symbol] = rows
        .map((row) => ({
          date: String(row.date ?? row.as_of_date ?? row.timestamp ?? ""),
          close: Number(row.close ?? row.price ?? row.last ?? row.value ?? row.adjClose)
        }))
        .filter((row) => row.date && Number.isFinite(row.close))
        .sort((a, b) => String(a.date).localeCompare(String(b.date)));
    });

    return grouped;
  }

  return {};
}

function coerceEquitiesPmiSeries(rows) {
  if (!Array.isArray(rows)) return [];

  return rows
    .map((row) => {
      const date = row.date ?? row.as_of_date ?? row.month ?? null;

      const composite =
        row.composite_signal ??
        row.pmi_composite_signal ??
        row.signal ??
        row.value ??
        row.composite ??
        null;

      return {
        date: date ? String(date) : null,
        etf: row.etf ?? "",
        composite: Number(composite),
        bias: row.bias ?? row.direction ?? row.composite_bias ?? "--",
        state: row.state ?? row.regime ?? row.composite_state ?? "Live"
      };
    })
    .filter((row) => row.date && Number.isFinite(row.composite))
    .sort((a, b) => String(a.date).localeCompare(String(b.date)));
}


function coerceEquitiesSigmaRows(rows) {
  if (!Array.isArray(rows)) return [];

  return rows
    .map((row) => ({
      symbol: String(row.symbol ?? row.pair ?? "").toUpperCase().trim(),
      pair: String(row.symbol ?? row.pair ?? "").toUpperCase().trim(),
      z: Number(row.z ?? row.sigma_value ?? row.value),
      rank: Number(row.rank),
      pct: Number(row.pct),
      state: row.state ?? "--",
      as_of_date: row.as_of_date ?? row.date ?? null
    }))
    .filter((row) => row.symbol && Number.isFinite(row.z) && Number.isFinite(row.rank))
    .sort((a, b) => Number(a.rank) - Number(b.rank));
} 


function sliceEquitiesRowsByHorizon(rows, horizon) {
  const visible = HORIZON_LENGTH[horizon] || 30;
  return rows.slice(-visible);
}

function formatCompactDateLabel(dateStr) {
  const d = new Date(`${dateStr}T00:00:00`);
  if (Number.isNaN(d.getTime())) return String(dateStr);
  return d.toLocaleDateString("en-US", { month: "short", year: "2-digit" });
}

function renderEquitiesIndexChart(container, groupedSeries, horizon, selectedSymbol = null) {
  if (!container) return;

  const lineClasses = [
      "equities-line-1",
      "equities-line-2",
      "equities-line-3",
      "equities-line-4",
      "equities-line-5",
      "equities-line-6"
    ];

  const symbols = getEquitiesIndexUniverse().filter(
    (symbol) => Array.isArray(groupedSeries[symbol]) && groupedSeries[symbol].length
  );

  if (!symbols.length) {
    container.innerHTML = `<div class="panel-placeholder">No equity index series available.</div>`;
    return;
  }

  const sliced = {};
  symbols.forEach((symbol) => {
    sliced[symbol] = sliceEquitiesRowsByHorizon(groupedSeries[symbol], horizon);
  });

  const baseSeries = symbols
    .map((symbol) => {
      const rows = sliced[symbol];
      if (!rows.length) return null;
      const base = rows[0].close;
      if (!Number.isFinite(base) || base === 0) return null;

      return {
        symbol,
        rows: rows.map((row) => ({
          date: row.date,
          level: ((row.close / base) - 1) * 100
        }))
      };
    })
    .filter(Boolean);

  if (!baseSeries.length) {
    container.innerHTML = `<div class="panel-placeholder">No normalized equity index series available.</div>`;
    return;
  }

  const width = Math.max(container.clientWidth, 320);
  const height = Math.max(container.clientHeight, 240);
  const padding = { top: 22, right: 18, bottom: 34, left: 18 };

  const allValues = baseSeries.flatMap((series) => series.rows.map((r) => r.level));
  const min = Math.min(...allValues, 0);
  const max = Math.max(...allValues, 0);
  const range = Math.max(max - min, 1e-9);
  const innerW = width - padding.left - padding.right;
  const innerH = height - padding.top - padding.bottom;

  const valueToY = (value) => padding.top + ((max - value) / range) * innerH;

  const longest = Math.max(...baseSeries.map((s) => s.rows.length), 1);

const paths = baseSeries
  .map((series, idx) => {
    const points = series.rows.map((row, pointIdx) => ({
      x: padding.left + (pointIdx / Math.max(series.rows.length - 1, 1)) * innerW,
      y: valueToY(row.level)
    }));

    const last = points[points.length - 1];
    const isSelected = selectedSymbol && series.symbol === selectedSymbol;
    const opacity = selectedSymbol ? (isSelected ? 1 : 0.22) : 1;
    const strokeWidth = selectedSymbol ? (isSelected ? 3.2 : 1.5) : 2;

    return `
      <path
        class="${lineClasses[idx % lineClasses.length]}"
        d="${createLinePath(points)}"
        style="opacity:${opacity};stroke-width:${strokeWidth}"
      ></path>
      <circle
        class="fx-point-last"
        cx="${last.x}"
        cy="${last.y}"
        r="${isSelected ? 4.6 : 3.2}"
        style="opacity:${opacity}"
      ></circle>
    `;
  })
  .join("");


  const zeroY = valueToY(0);

  const labelIdx = [0, Math.floor((longest - 1) / 2), longest - 1];
  const referenceRows = baseSeries.reduce(
  (a, b) => (b.rows.length > a.rows.length ? b : a)
).rows;

  const xLabels = [...new Set(labelIdx)]
    .map((idx) => {
      const safeIdx = Math.min(idx, referenceRows.length - 1);
      const x = padding.left + (safeIdx / Math.max(referenceRows.length - 1, 1)) * innerW;
      return `<text class="fx-axis-label" x="${x}" y="${height - 10}" text-anchor="middle">${formatCompactDateLabel(referenceRows[safeIdx].date)}</text>`;
    })
    .join("");


const legend = baseSeries
  .map((series, idx) => {
    const step = innerW / Math.max(baseSeries.length, 1);
    const x = padding.left + idx * step;
    const isSelected = selectedSymbol && series.symbol === selectedSymbol;
    const opacity = selectedSymbol ? (isSelected ? 1 : 0.32) : 1;
    const fontWeight = isSelected ? 700 : 500;

    return `
      <text
        class="${lineClasses[idx % lineClasses.length]} equities-legend-label"
        x="${x}"
        y="${padding.top - 6}"
        style="opacity:${opacity};font-weight:${fontWeight}"
      >${series.symbol}</text>
    `;
  })
  .join("");

  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Equities index comparison chart">
      <line class="fx-grid-line" x1="${padding.left}" y1="${padding.top}" x2="${width - padding.right}" y2="${padding.top}"></line>
      <line class="fx-zero-line" x1="${padding.left}" y1="${zeroY}" x2="${width - padding.right}" y2="${zeroY}"></line>
      <line class="fx-grid-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>

      <text class="fx-axis-label" x="${width - padding.right}" y="${padding.top - 6}" text-anchor="end">${formatNumber(max, 2)}%</text>
      <text class="fx-axis-label" x="${width - padding.right}" y="${zeroY - 6}" text-anchor="end">0.00%</text>
      <text class="fx-axis-label" x="${width - padding.right}" y="${height - padding.bottom - 6}" text-anchor="end">${formatNumber(min, 2)}%</text>

      ${paths}
      ${legend}
      ${xLabels}
    </svg>
  `;
}

function renderEquitiesPmiChart(container, rows, horizon) {
  if (!container) return;

  const visibleRows = sliceEquitiesRowsByHorizon(rows, horizon);

  if (!visibleRows.length) {
    container.innerHTML = `<div class="panel-placeholder">No PMI Composite Signal history available.</div>`;
    return;
  }

  const width = Math.max(container.clientWidth, 320);
  const height = Math.max(container.clientHeight, 240);
  const padding = { top: 22, right: 18, bottom: 34, left: 18 };
  const values = visibleRows.map((r) => r.composite);

  const min = Math.min(...values, 0);
  const max = Math.max(...values, 0);
  const range = Math.max(max - min, 1e-9);
  const innerW = width - padding.left - padding.right;
  const innerH = height - padding.top - padding.bottom;

  const valueToY = (value) => padding.top + ((max - value) / range) * innerH;

  const points = visibleRows.map((row, idx) => ({
    x: padding.left + (idx / Math.max(visibleRows.length - 1, 1)) * innerW,
    y: valueToY(row.composite)
  }));

  const last = points[points.length - 1];
  const zeroY = valueToY(0);

  const bars = visibleRows
    .map((row, idx) => {
      const xCenter = padding.left + (idx / Math.max(visibleRows.length - 1, 1)) * innerW;
      const barWidth = Math.max(innerW / Math.max(visibleRows.length * 1.8, 10), 4);
      const y = valueToY(row.composite);
      const baseY = zeroY;
      const barY = Math.min(y, baseY);
      const barH = Math.max(Math.abs(baseY - y), 2);
      const cls = row.composite >= 0 ? "equities-bar-positive" : "equities-bar-negative";

      return `<rect class="${cls}" x="${xCenter - barWidth / 2}" y="${barY}" width="${barWidth}" height="${barH}" rx="2"></rect>`;
    })
    .join("");

  const labelIdx = [0, Math.floor((visibleRows.length - 1) / 2), visibleRows.length - 1];
  const xLabels = [...new Set(labelIdx)]
    .map((idx) => {
      const x = padding.left + (idx / Math.max(visibleRows.length - 1, 1)) * innerW;
      return `<text class="fx-axis-label" x="${x}" y="${height - 10}" text-anchor="middle">${formatCompactDateLabel(visibleRows[idx].date)}</text>`;
    })
    .join("");

  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Equities PMI composite chart">
      <line class="fx-grid-line" x1="${padding.left}" y1="${padding.top}" x2="${width - padding.right}" y2="${padding.top}"></line>
      <line class="fx-zero-line" x1="${padding.left}" y1="${zeroY}" x2="${width - padding.right}" y2="${zeroY}"></line>
      <line class="fx-grid-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>

      <text class="fx-axis-label" x="${width - padding.right}" y="${padding.top - 6}" text-anchor="end">${formatNumber(max, 2)}</text>
      <text class="fx-axis-label" x="${width - padding.right}" y="${zeroY - 6}" text-anchor="end">0.00</text>
      <text class="fx-axis-label" x="${width - padding.right}" y="${height - padding.bottom - 6}" text-anchor="end">${formatNumber(min, 2)}</text>

      ${bars}
      <path class="equities-line-1" d="${createLinePath(points)}"></path>
      <circle class="fx-point-last" cx="${last.x}" cy="${last.y}" r="4"></circle>
      ${xLabels}
    </svg>
  `;
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

  function renderMacro() {
    const macroView = document.getElementById("view-macro");
    if (!macroView) return;

    const region = macroControls.region?.value || "USA";
    const horizon = macroControls.horizon?.value || "30D";
    const regimeFilter = macroControls.regimeFilter?.value || "Skeleton";
    const dataMode = macroControls.dataMode?.value || "Skeleton";

    const driversPanel = macroView.querySelector(".macro-drivers-panel");
    const transmissionPanel = macroView.querySelector(".macro-transmission-panel");
    const lagPanel = macroView.querySelector(".macro-lag-panel");
    const exposurePanel = macroView.querySelector(".macro-exposure-panel");
    const impactPanel = macroView.querySelector(".macro-impact-panel");
    const riskValue = macroView.querySelector(".macro-riskstack-panel strong");

    if (driversPanel) {
      driversPanel.innerHTML = `
        <div class="panel-title">Macro Drivers</div>
        <div class="panel-placeholder">${region} • ${horizon}</div>
      `;
    }

    if (transmissionPanel) {
      transmissionPanel.innerHTML = `
        <div class="panel-title">Transmission</div>
        <div class="panel-placeholder">Rates → CPI → USD → Oil</div>
      `;
    }

    if (lagPanel) {
      lagPanel.innerHTML = `
        <div class="panel-title">Lag Structure</div>
        <div class="panel-placeholder">${regimeFilter}</div>
      `;
    }

    if (exposurePanel) {
      exposurePanel.innerHTML = `
        <div class="panel-title">Exposure</div>
        <div class="panel-placeholder">Static sector map</div>
      `;
    }

    if (impactPanel) {
      impactPanel.innerHTML = `
        <div class="panel-title">Impact</div>
        <div class="panel-placeholder">${dataMode}</div>
      `;
    }

    if (riskValue) {
      riskValue.textContent = "--";
    }
  }


function setBodyViewClass(viewName) {
  document.body.classList.remove(
    "view-what-is",
    "view-fx",
    "view-wti",
    "view-equities",
    "view-macro",
    "view-oc",
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

  if (viewName === "macro") {
    document.body.classList.add("view-macro", "about-sidebar-hidden");
    return;
  }

  if (viewName === "oc") {
    document.body.classList.add("view-oc", "about-sidebar-hidden");
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
document.querySelectorAll(
  `.module-tab[data-view="${viewName}"], .top-nav-item[data-view="${viewName}"]`
).forEach((tab) => {
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
          void renderEquities();
        });
      });
    }

    if (viewName === "macro") {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          renderMacro();
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

  function formatMonthYearLabel(dateStr) {
    if (!dateStr) return "--";
    const d = new Date(`${dateStr}T00:00:00`);
    if (Number.isNaN(d.getTime())) return String(dateStr);
    return d.toLocaleDateString("en-US", { month: "long", year: "numeric" });
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

async function renderEquities() {
  updateEquitiesGeoScenToolbarLabel();

  const region = equitiesControls.region?.value || "USA";
  const horizon = equitiesControls.horizon?.value || "30D";
  const topRightMode = equitiesControls.topRightMode?.value || "Market Breadth";
  const etfFocus = String(equitiesControls.etfFocus?.value || "SPY").toUpperCase();

  const indexBadge = document.getElementById("equities-index-badge");
  const indexDate = document.getElementById("equities-index-date");
  const industryDate = document.getElementById("equities-industry-date");
  const topRightDate = document.getElementById("equities-top-right-date");
  const sigmaDate = document.getElementById("equities-sigma-date");

if (indexBadge) indexBadge.textContent = `${region} • ${horizon}`;

const regionIsLive = region === "USA";

if (!regionIsLive) {
  renderEquitiesIndexPlaceholder("equities-index-chart", `${region} is not live yet.`);
  renderEquitiesIndexPlaceholder("equities-industry-chart", `${region} PMI composite is not live yet.`);
  updateStatValue(document.getElementById("equities-index-state"), "Coming Soon");
  updateStatValue(document.getElementById("equities-industry-state"), "Coming Soon");
  return;
}

  try {
    const equitiesData = await loadEquitiesData();

    if (!equitiesData.health.index) {
      renderEquitiesIndexPlaceholder("equities-index-chart", "Equity index endpoint unavailable.");
      updateStatValue(document.getElementById("equities-index-state"), "Endpoint Unavailable");
    } else {
      const groupedSeries = coerceEquitiesIndexSeries(equitiesData.index);
      const availableSymbols = getEquitiesIndexUniverse().filter(
        (symbol) => Array.isArray(groupedSeries[symbol]) && groupedSeries[symbol].length
      );

      const selectedSymbol = availableSymbols.includes(etfFocus) ? etfFocus : null;

      if (!availableSymbols.length) {
        renderEquitiesIndexPlaceholder("equities-index-chart", "No equity index series available.");
        updateStatValue(document.getElementById("equities-index-state"), "No Data");
      } else {
        renderEquitiesIndexChart(
          document.getElementById("equities-index-chart"),
          groupedSeries,
          horizon,
          selectedSymbol
        );

        updateStatValue(document.getElementById("equities-index-state"), "Live");
        updateStatValue(
          document.getElementById("equities-index-focus"),
          selectedSymbol || availableSymbols.join(" / ")
        );
        updateStatValue(
          document.getElementById("equities-index-mode"),
          selectedSymbol ? "Focused vs Universe" : "USA Multi-Index"
        );

        const lastDates = availableSymbols
          .map((symbol) => groupedSeries[symbol][groupedSeries[symbol].length - 1]?.date)
          .filter(Boolean)
          .sort();

        if (indexDate) {
          indexDate.textContent = lastDates.length ? lastDates[lastDates.length - 1] : "--";
        }
      }
    }

    if (!equitiesData.health.industryPanel) {
      renderEquitiesIndexPlaceholder("equities-industry-chart", "Industry panel endpoint unavailable.");
      updateStatValue(document.getElementById("equities-industry-state"), "Endpoint Unavailable");
    } else {
      const industrySubtitle = document.getElementById("equities-industry-subtitle");

      if (isSectorEtf(etfFocus)) {
        const panelRows = Array.isArray(equitiesData.industryPanel) ? equitiesData.industryPanel : [];
        const etfRows = panelRows.filter(
          (r) => String(r.etf || "").toUpperCase() === etfFocus
        );

        if (industrySubtitle) {
          industrySubtitle.textContent = `PMI + New Orders Composite — ${etfFocus} | ${getEquitiesTickerLabel(etfFocus)}`;
        }

        if (!etfRows.length) {
          renderEquitiesIndexPlaceholder(
            "equities-industry-chart",
            `No industry panel available for ${etfFocus}.`
          );
          updateStatValue(document.getElementById("equities-industry-pmi"), "--");
          updateStatValue(document.getElementById("equities-industry-bias"), "--");
          updateStatValue(document.getElementById("equities-industry-state"), "No Data");
        } else {
          const latestDate = etfRows
            .map((x) => x.date)
            .filter(Boolean)
            .sort()
            .slice(-1)[0];

          const latestEtfRows = etfRows.filter((r) => r.date === latestDate);
          const sigValues = latestEtfRows
            .map((r) => Number(r.Sig))
            .filter(Number.isFinite);

          const avgSig = sigValues.length
            ? sigValues.reduce((a, b) => a + b, 0) / sigValues.length
            : null;

          updateStatValue(
            document.getElementById("equities-industry-pmi"),
            avgSig == null ? "--" : `${avgSig >= 0 ? "+" : ""}${formatNumber(avgSig, 2)}`,
            avgSig == null ? null : (avgSig >= 0 ? "positive" : "negative")
          );

          updateStatValue(
            document.getElementById("equities-industry-bias"),
            "Panel"
          );

          updateStatValue(
            document.getElementById("equities-industry-state"),
            "Live"
          );

          renderIndustryPanelTable(
            document.getElementById("equities-industry-chart"),
            panelRows,
            etfFocus
          );

          if (industryDate) {
            industryDate.textContent = latestDate || "--";
          }

          const industrySource = document.getElementById("equities-industry-source");
          if (industrySource) {
            industrySource.innerHTML = `Source: industry_panel_serving.json | the_Spine | Showing ${formatMonthYearLabel(latestDate)}`;
          }
        }
      } else if (isMarketIndexEtf(etfFocus)) {
        const cfg = EQUITIES_LENS_CONFIG[etfFocus];

        if (industrySubtitle) {
          industrySubtitle.textContent = `Lens Structure — ${etfFocus} | ${getEquitiesTickerLabel(etfFocus)}`;
        }

        updateStatValue(
          document.getElementById("equities-industry-pmi"),
          cfg?.lens_primary || "--"
        );
        updateStatValue(
          document.getElementById("equities-industry-bias"),
          cfg?.lens_secondary || "--"
        );
        updateStatValue(
          document.getElementById("equities-industry-state"),
          cfg?.cyclical_defensive || "--"
        );

        renderEquitiesLensCard(
          document.getElementById("equities-industry-chart"),
          etfFocus
        );

        if (industryDate) {
          industryDate.textContent = "Lens";
        }
      } else {
        renderEquitiesIndexPlaceholder(
          "equities-industry-chart",
          `No industry/lens definition available for ${etfFocus}.`
        );
        updateStatValue(document.getElementById("equities-industry-pmi"), "--");
        updateStatValue(document.getElementById("equities-industry-bias"), "--");
        updateStatValue(document.getElementById("equities-industry-state"), "No Data");
      }
    }


    if (topRightMode === "Market Breadth") {
      const titleEl = document.querySelector("#view-equities .equities-top-right-panel .panel-title");
      const subtitleEl = document.getElementById("equities-top-right-subtitle");

      if (titleEl) titleEl.textContent = "Market Breadth";
      if (subtitleEl) subtitleEl.textContent = `${etfFocus} Breadth Signal`;

      if (!equitiesData.health.breadth) {
        renderEquitiesIndexPlaceholder("equities-top-right-chart", "Breadth endpoint unavailable.");
        updateStatValue(document.getElementById("equities-top-right-primary"), "--");
        updateStatValue(document.getElementById("equities-top-right-secondary"), "--");
        updateStatValue(document.getElementById("equities-top-right-state"), "Endpoint Unavailable");
      } else {
        const breadthRows = coerceEquitiesPmiSeries(
          equitiesData.breadth.filter(
            (row) => String(row.etf || "").toUpperCase() === etfFocus
          )
        );

        const latestBreadth = breadthRows.length ? breadthRows[breadthRows.length - 1] : null;

        if (latestBreadth) {
          updateStatValue(
            document.getElementById("equities-top-right-primary"),
            formatNumber(latestBreadth.composite, 2),
            latestBreadth.composite >= 0 ? "positive" : "negative"
          );
          updateStatValue(
            document.getElementById("equities-top-right-secondary"),
            String(latestBreadth.bias)
          );
          updateStatValue(
            document.getElementById("equities-top-right-state"),
            String(latestBreadth.state)
          );

          renderEquitiesPmiChart(
            document.getElementById("equities-top-right-chart"),
            breadthRows,
            horizon
          );

          if (topRightDate) {
            topRightDate.textContent = latestBreadth.date || "--";
          }
        } else {
          renderEquitiesIndexPlaceholder("equities-top-right-chart", `No breadth data available for ${etfFocus}.`);
          updateStatValue(document.getElementById("equities-top-right-primary"), "--");
          updateStatValue(document.getElementById("equities-top-right-secondary"), "--");
          updateStatValue(document.getElementById("equities-top-right-state"), "No Data");
        }
      }
    }

    if (!equitiesData.health.sigma) {
      setChartPlaceholder("equities-sigma-chart", "Sigma endpoint unavailable.");
    } else {
      const sigmaRows = coerceEquitiesSigmaRows(equitiesData.sigma);
      const sigmaRow = sigmaRows.find((row) => row.symbol === etfFocus);

      if (sigmaRow) {
        updateStatValue(document.getElementById("equities-sigma-selected"), sigmaRow.symbol);
        updateStatValue(
          document.getElementById("equities-sigma-z"),
          `${sigmaRow.z >= 0 ? "+" : ""}${formatNumber(sigmaRow.z, 2)}`,
          sigmaRow.z >= 0 ? "positive" : "negative"
        );
        updateStatValue(
          document.getElementById("equities-sigma-rank"),
          `${sigmaRow.rank}/${sigmaRows.length}`
        );

        renderAssetSigmaChart(
          document.getElementById("equities-sigma-chart"),
          sigmaRows,
          sigmaRow.symbol
        );

        if (sigmaDate) {
          sigmaDate.textContent = sigmaRow.as_of_date || "--";
        }
      } else {
        setChartPlaceholder("equities-sigma-chart", `No sigma data available for ${etfFocus}.`);
      }
    }
  } catch (error) {
    console.error("Equities panel load failed:", error);
    renderEquitiesIndexPlaceholder("equities-index-chart", "Equities index load failed.");
    renderEquitiesIndexPlaceholder("equities-industry-chart", "PMI Composite load failed.");
    renderEquitiesIndexPlaceholder("equities-top-right-chart", "Breadth load failed.");
    setChartPlaceholder("equities-sigma-chart", "Sigma load failed.");
    updateStatValue(document.getElementById("equities-index-state"), "Load Failed");
    updateStatValue(document.getElementById("equities-industry-state"), "Load Failed");
    updateStatValue(document.getElementById("equities-top-right-state"), "Load Failed");
  }
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


const EQUITIES_TICKER_LABELS = {
  SPY: "S&P 500",
  QQQ: "Nasdaq 100 / Growth-Tech Tilt",
  DIA: "Dow Jones Industrial Average",
  ITOT: "Total U.S. Stock Market",
  MDY: "S&P MidCap 400",
  IWM: "Russell 2000 Small Cap",

  XLB: "Materials",
  XLC: "Communication Services",
  XLE: "Energy",
  XLF: "Financials",
  XLI: "Industrials",
  XLK: "Technology",
  XLP: "Consumer Staples",
  XLRE: "Real Estate",
  XLU: "Utilities",
  XLV: "Health Care",
  XLY: "Consumer Discretionary"
};

function getEquitiesTickerLabel(symbol) {
  return EQUITIES_TICKER_LABELS[String(symbol || "").toUpperCase()] || String(symbol || "").toUpperCase();
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


function renderAssetSigmaChart(container, rows, selectedKey) {
  if (!container) return;

  if (!rows.length) {
    container.innerHTML = `<div class="panel-placeholder">No sigma data available.</div>`;
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
    const selectedClass = row.pair === selectedKey ? "selected" : "";

    return `
      <rect class="fx-bar ${selectedClass}" x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="4"></rect>
      <text class="fx-bar-label" x="${x + (barWidth / 2)}" y="${height - 12}" text-anchor="middle">${row.pair}</text>
      <text class="fx-bar-value" x="${x + (barWidth / 2)}" y="${y - 6}" text-anchor="middle">${row.z.toFixed(2)}</text>
    `;
  }).join("");

  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Sigma Rank">
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

    const HORIZON_LENGTH_WTI = {
      "5D": 5,
      "15D": 15,
      "30D": 30,
      "45D": 45
    };

    const horizon = wtiControls.horizon?.value || "30D";

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
            <option value="standard" selected>Standard</option>
          </select>
        `;
        toolbar.insertBefore(viewGroup, toolbar.firstChild);
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
          </select>
        `;
        toolbar.insertBefore(regimeGroup, toolbar.firstChild.nextSibling);
      }

      return {
        view: document.getElementById("wti-inventory-view"),
        regime: document.getElementById("wti-inventory-regime")
      };
    }

    function renderWTILineChart(container, rows, labelText) {
      if (!container || !rows || !rows.length) return;

      const width = Math.max(container.clientWidth, 300);
      const height = Math.max(container.clientHeight, 230);
      const padding = { top: 18, right: 16, bottom: 34, left: 16 };

      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const values = rows.map((r) => r.close).filter((v) => Number.isFinite(v));
      if (!values.length) {
        container.innerHTML = `<div class="panel-placeholder">No data available.</div>`;
        return;
      }

      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = Math.max(max - min, 1e-9);

      const valueToY = (value) => padding.top + ((max - value) / range) * innerH;
      const step = innerW / Math.max(rows.length - 1, 1);

      const points = rows.map((row, idx) => ({
        x: padding.left + idx * step,
        y: valueToY(row.close)
      }));

      const path = createLinePath(points);

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

      const lastPoint = points[points.length - 1];

      container.innerHTML = `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${labelText}">
          ${yTicks}
          <path class="fx-line-secondary" d="${path}"></path>
          <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
          <text class="fx-overlay-label" x="${padding.left + 8}" y="${padding.top + 12}">${labelText}</text>
          ${xLabels}
        </svg>
      `;
    }

    const inventorySourceEl = document.getElementById("wti-inventory-source");
    const inventorySourcePrefixEl = document.getElementById("wti-inventory-source-prefix");
    const inventorySourcePrefix =
      inventorySourceEl?.getAttribute("data-source-prefix") || "Source: EIA | the_Spine | As of ";

    if (inventorySourcePrefixEl) {
      inventorySourcePrefixEl.textContent = inventorySourcePrefix;
    }

    const inventoryDate = document.getElementById("wti-inventory-date");
    const priceDate = document.getElementById("wti-price-date");
    const macroDate = document.getElementById("wti-macro-date");
    const sigmaDate = document.getElementById("wti-sigma-date");

    const inventoryBadge = document.getElementById("wti-inventory-badge");
    const priceBadge = document.getElementById("wti-price-badge");
    const macroBadge = document.getElementById("wti-macro-badge");
    const sigmaBadge = document.getElementById("wti-sigma-badge");

if (inventoryBadge) inventoryBadge.textContent = horizon;
if (priceBadge) priceBadge.textContent = `WTI • ${horizon}`;
if (macroBadge) macroBadge.textContent = "Pressure / Macro";
if (sigmaBadge) sigmaBadge.textContent = "Cross-Series Z Context";

    ensureWtiInventoryControls();

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
      const panel = await loadWtiPanel();
      const rows = Array.isArray(panel.history) ? panel.history : [];
      const summary = panel.summary || {};

      const inventorySummary = summary.inventory || {};
      const inflationSummary = summary.inflation_pressure || {};
      const sigmaSummary = summary.sigma_rank || {};

      const visible = rows.slice(-(HORIZON_LENGTH_WTI[horizon] || 30));

updateStatValue(
  document.getElementById("wti-inventory-last"),
  formatNumber(inventorySummary.last_inventory_mmbbl, 2)
);

const inventoryChange =
  rows.length >= 2 &&
  Number.isFinite(Number(rows[rows.length - 1]?.inventory_mmbbl)) &&
  Number.isFinite(Number(rows[rows.length - 2]?.inventory_mmbbl))
    ? Number(rows[rows.length - 1].inventory_mmbbl) - Number(rows[rows.length - 2].inventory_mmbbl)
    : null;

updateStatValue(
  document.getElementById("wti-inventory-change"),
  inventoryChange == null
    ? "--"
    : `${inventoryChange >= 0 ? "+" : ""}${formatNumber(inventoryChange, 2)}`,
  inventoryChange == null ? null : (inventoryChange >= 0 ? "positive" : "negative")
);

updateStatValue(
  document.getElementById("wti-inventory-range"),
  inventorySummary.min_inventory_mmbbl != null && inventorySummary.max_inventory_mmbbl != null
    ? `${formatNumber(inventorySummary.min_inventory_mmbbl, 2)} - ${formatNumber(inventorySummary.max_inventory_mmbbl, 2)}`
    : "--"
);

      updateStatValue(
        document.getElementById("wti-macro-cpi"),
        formatNumber(inflationSummary.last_inflation_pressure_z, 2),
        Number(inflationSummary.last_inflation_pressure_z) >= 0 ? "positive" : "negative"
      );
      updateStatValue(
        document.getElementById("wti-macro-trend"),
        inflationSummary.components_used || "--"
      );
      updateStatValue(
        document.getElementById("wti-macro-state"),
        inflationSummary.last_state || "--"
      );

      updateStatValue(document.getElementById("wti-sigma-selected"), "WTI");
      updateStatValue(
        document.getElementById("wti-sigma-z"),
        formatNumber(sigmaSummary.last_sigma_value, 2),
        Number(sigmaSummary.last_sigma_value) >= 0 ? "positive" : "negative"
      );
      updateStatValue(
        document.getElementById("wti-sigma-rank"),
        sigmaSummary.last_sigma_rank != null
          ? String(sigmaSummary.last_sigma_rank)
          : "--"
      );

      if (inventoryDate) {
        inventoryDate.textContent = inventorySummary.history_end ? formatUTC(inventorySummary.history_end) : "--";
      }
      if (macroDate) {
        macroDate.textContent = inflationSummary.history_end ? formatUTC(inflationSummary.history_end) : "--";
      }
      if (sigmaDate) {
        sigmaDate.textContent = sigmaSummary.history_end ? formatUTC(sigmaSummary.history_end) : "--";
      }
      if (priceDate) {
        priceDate.textContent = visible.length ? formatUTC(visible[visible.length - 1].date) : "--";
      }

      const inventoryRows = visible
        .map((row) => ({
          date: row.date,
          close: Number(row.inventory_mmbbl)
        }))
        .filter((row) => row.date && Number.isFinite(row.close));

      if (inventoryRows.length) {
        renderWTILineChart(
          document.getElementById("wti-inventory-chart"),
          inventoryRows,
          "Inventory Index"
        );
      } else {
        setChartPlaceholder("wti-inventory-chart", "No inventory data available.");
      }


      const macroRows = visible
        .map((row) => ({
          date: row.date,
          close: Number(row.inflation_pressure_z)
        }))
        .filter((row) => row.date && Number.isFinite(row.close));

      const wtiSigmaRows = [];
      const lastSigmaValue = Number(sigmaSummary.last_sigma_value);
      const lastSigmaRank = Number(sigmaSummary.last_sigma_rank);

      if (Number.isFinite(lastSigmaValue)) {
        wtiSigmaRows.push({
          pair: "WTI",
          z: lastSigmaValue,
          rank: Number.isFinite(lastSigmaRank) ? lastSigmaRank : 1,
          pct: Number(sigmaSummary.last_sigma_pct),
          state: sigmaSummary.last_state ?? "--"
        });
      }

      if (macroRows.length) {
        renderWTILineChart(
          document.getElementById("wti-macro-chart"),
          macroRows,
          "Inflation Pressure"
        );
      } else {
        setChartPlaceholder("wti-macro-chart", "No inflation data available.");
      }

      if (wtiSigmaRows.length) {
        renderAssetSigmaChart(
          document.getElementById("wti-sigma-chart"),
          wtiSigmaRows,
          "WTI"
        );
      } else {
        setChartPlaceholder("wti-sigma-chart", "No sigma data available.");
      }

      const wtiPricePayload = await loadWtiPriceData();
      const wtiPriceRows = wtiPricePayload.rows
        .map((row) => ({
          date: row.date ?? row.as_of_date ?? row.timestamp ?? null,
          close: Number(row.close ?? row.price ?? row.last ?? row.value),
          high: Number(row.high ?? row.close ?? row.price ?? row.last ?? row.value),
          low: Number(row.low ?? row.close ?? row.price ?? row.last ?? row.value)
        }))
        .filter((row) => row.date && Number.isFinite(row.close))
        .sort((a, b) => String(a.date).localeCompare(String(b.date)))
        .slice(-(HORIZON_LENGTH_WTI[horizon] || 30));

      if (wtiPriceRows.length) {
        const wtiLast = wtiPriceRows[wtiPriceRows.length - 1].close;
        const wtiFirst = wtiPriceRows[0].close;
        const wtiChangePct = ((wtiLast / wtiFirst) - 1) * 100;
        const wtiMin = Math.min(...wtiPriceRows.map((d) => d.low));
        const wtiMax = Math.max(...wtiPriceRows.map((d) => d.high));

        updateStatValue(document.getElementById("wti-price-last"), formatNumber(wtiLast, 2));
        updateStatValue(
          document.getElementById("wti-price-change"),
          `${wtiChangePct >= 0 ? "+" : ""}${formatNumber(wtiChangePct, 2)}%`,
          wtiChangePct >= 0 ? "positive" : "negative"
        );
        updateStatValue(
          document.getElementById("wti-price-range"),
          `${formatNumber(wtiMin, 2)} - ${formatNumber(wtiMax, 2)}`
        );

        renderWTILineChart(
          document.getElementById("wti-price-chart"),
          wtiPriceRows.map((row) => ({
            date: row.date,
            close: row.close
          })),
          "WTI Price"
        );

        if (priceDate) {
          priceDate.textContent = formatUTC(
            wtiPricePayload.meta?.latest_date || wtiPriceRows[wtiPriceRows.length - 1].date
          );
        }
      } else {
        updateStatValue(document.getElementById("wti-price-last"), "--");
        updateStatValue(document.getElementById("wti-price-change"), "--");
        updateStatValue(document.getElementById("wti-price-range"), "--");
        setChartPlaceholder("wti-price-chart", "No WTI price data available.");
      }


    } catch (error) {
      console.error("WTI panel fetch failed:", error);

      updateStatValue(document.getElementById("wti-inventory-last"), "--");
      updateStatValue(document.getElementById("wti-inventory-change"), "--");
      updateStatValue(document.getElementById("wti-inventory-range"), "--");
      updateStatValue(document.getElementById("wti-macro-cpi"), "--");
      updateStatValue(document.getElementById("wti-macro-trend"), "--");
      updateStatValue(document.getElementById("wti-macro-state"), "--");
      updateStatValue(document.getElementById("wti-sigma-selected"), "WTI");
      updateStatValue(document.getElementById("wti-sigma-z"), "--");
      updateStatValue(document.getElementById("wti-sigma-rank"), "--");
      updateStatValue(document.getElementById("wti-price-last"), "--");
      updateStatValue(document.getElementById("wti-price-change"), "--");
      updateStatValue(document.getElementById("wti-price-range"), "--");

      setChartPlaceholder("wti-inventory-chart", "WTI panel fetch failed.");
      setChartPlaceholder("wti-macro-chart", "WTI panel fetch failed.");
      setChartPlaceholder("wti-sigma-chart", "WTI panel fetch failed.");
      setChartPlaceholder("wti-price-chart", "WTI price fetch failed.");

      if (inventoryDate) inventoryDate.textContent = "--";
      if (priceDate) priceDate.textContent = "--";
      if (macroDate) macroDate.textContent = "--";
      if (sigmaDate) sigmaDate.textContent = "--";
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

  Object.values(macroControls).forEach((el) => {
    if (el) {
      el.addEventListener("change", () => {
        if (document.body.classList.contains("view-macro")) {
          renderMacro();
        }
      });
    }
  });

Object.values(equitiesControls).forEach((el) => {
  if (el) {
    el.addEventListener("change", () => {
      updateEquitiesGeoScenToolbarLabel();

      if (document.body.classList.contains("view-equities")) {
        void renderEquities();
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
          void renderEquities();
        });
      });
    }

    if (document.body.classList.contains("view-macro")) {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          renderMacro();
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


