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
  equitiesPmiComposite: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/etf_pmi_composite.json",
  equitiesNewOrdersComposite: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/etf_no_composite.json",
  equitiesBreadth: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/etf_pmi_breadth_by_etf.json",
  equitiesSigma: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/equities_sigma_rank.json",

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


function normalizeEquitiesIndexPayload(payload) {
  if (Array.isArray(payload)) return payload;
  return payload && typeof payload === "object" ? payload : {};
}

function normalizeEquitiesPmiPayload(payload) {
  if (payload && typeof payload === "object" && payload.etfs) return payload;
  return { etfs: {} };
}

function normalizeEquitiesNoPayload(payload) {
  if (payload && typeof payload === "object" && payload.etfs) return payload;
  return { etfs: {} };
}

function normalizeEquitiesPmiSeriesPayload(payload) {
  if (Array.isArray(payload)) return payload;
  if (payload && typeof payload === "object" && Array.isArray(payload.value)) return payload.value;
  if (payload && typeof payload === "object" && Array.isArray(payload.rows)) return payload.rows;
  return [];
}

function getEquitiesPmiPackageForEtf(payload, etfFocus) {
  if (!payload || !payload.etfs) return null;
  return payload.etfs[String(etfFocus || "").toUpperCase()] || null;
}


function computeDelta(series, monthsBack) {
  if (!Array.isArray(series) || !series.length) return null;

  const currentIndex = series.length - 1;
  const pastIndex = currentIndex - (monthsBack - 1);

  if (pastIndex < 0) return null;

  const current = Number(series[currentIndex]);
  const past = Number(series[pastIndex]);

  if (!Number.isFinite(current) || !Number.isFinite(past)) return null;

  return current - past;
}


function getPmiValue(row) {
  const num = Number(row?.value ?? row?.pmi ?? row?.signal);
  return Number.isFinite(num) ? num : null;
}

function getOrdersValue(row) {
  const num = Number(row?.new_orders ?? row?.value ?? row?.orders);
  return Number.isFinite(num) ? num : null;
}

function normalizeIndustryText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeIndustryBucket(value) {
  const v = String(value || "").toLowerCase().trim();

  if (v === "nonmanu" || v === "non-manufacturing" || v === "services" || v === "service") {
    return "services";
  }

  if (v === "manu" || v === "manufacturing" || v === "mfg") {
    return "manufacturing";
  }

  return v;
}

function buildIndustryFullKey(row) {
  return `${normalizeIndustryBucket(row?.pmi_type)}||${normalizeIndustryText(row?.industry)}`;
}

function buildIndustryFallbackKey(row) {
  return normalizeIndustryText(row?.industry);
}

function buildIndustryDeltaLookup(etfPkg) {
  const lookup = {};

  if (!etfPkg) return lookup;

  const latestRows = Array.isArray(etfPkg.industries_latest) ? etfPkg.industries_latest : [];
  const historyRows = Array.isArray(etfPkg.industries_history) ? etfPkg.industries_history : [];

  if (!latestRows.length && !historyRows.length) return lookup;

  const grouped = {};

historyRows.forEach((row) => {
  const fullKey = buildIndustryFullKey(row);

  const value =
    etfPkg.meta?.metric === "new_orders"
      ? getOrdersValue(row)
      : getPmiValue(row);

  if (!Number.isFinite(value)) return;

  if (!grouped[fullKey]) grouped[fullKey] = [];
  grouped[fullKey].push({
    date: String(row.date || row.as_of_date || ""),
    value
  });
});

  Object.entries(grouped).forEach(([key, rows]) => {
    rows.sort(
      (a, b) =>
        new Date(String(a.date || "")).getTime() -
        new Date(String(b.date || "")).getTime()
    );

    const series = rows.map((x) => x.value);

    lookup[key] = {
      delta3m:
        series.length >= 3 &&
        Number.isFinite(Number(series[series.length - 1])) &&
        Number.isFinite(Number(series[series.length - 3]))
          ? Number(series[series.length - 1]) - Number(series[series.length - 3])
          : null,
      delta6m: computeDelta(series, 6)
    };
  });

latestRows.forEach((row) => {
  const fullKey = buildIndustryFullKey(row);

  if (!lookup[fullKey]) {
    lookup[fullKey] = { delta3m: null, delta6m: null };
  }
});

  return lookup;
}

function formatDelta(val) {
  if (val == null || !Number.isFinite(Number(val))) return "--";
  const num = Number(val);
  return `${num >= 0 ? "+" : ""}${num.toFixed(1)}`;
}

function formatSignedInt(val) {
  if (val == null || !Number.isFinite(Number(val))) return "--";
  const num = Math.round(Number(val));
  return `${num >= 0 ? "+" : ""}${num}`;
}

function formatInt(val) {
  if (val == null || !Number.isFinite(Number(val))) return "--";
  return String(Math.round(Number(val)));
}


function buildRankBuckets(items, fieldName) {
  const valid = items
    .filter((x) => Number.isFinite(Number(x[fieldName])))
    .slice()
    .sort((a, b) => Number(b[fieldName]) - Number(a[fieldName]));

  const top = new Set(valid.slice(0, 5).map((x) => `${x.pmi_type}||${x.industry}`));
  const bottom = new Set(valid.slice(-5).map((x) => `${x.pmi_type}||${x.industry}`));

  return { top, bottom };
}


function buildIndustryLatestMap(etfPkg, metricType) {
  const latestMap = {};

  const latestRows = Array.isArray(etfPkg?.industries_latest)
    ? etfPkg.industries_latest
    : [];

  const historyRows = Array.isArray(etfPkg?.industries_history)
    ? etfPkg.industries_history
    : [];

  latestRows.forEach((row) => {
    const fullKey = buildIndustryFullKey(row);
    latestMap[fullKey] = row;
  });

  const groupedHistory = {};

  historyRows.forEach((row) => {
    const fullKey = buildIndustryFullKey(row);
    if (!groupedHistory[fullKey]) groupedHistory[fullKey] = [];
    groupedHistory[fullKey].push(row);
  });

  Object.entries(groupedHistory).forEach(([key, rows]) => {
    if (latestMap[key]) return;

    rows.sort(
      (a, b) =>
        new Date(String(a.date || a.as_of_date || "")).getTime() -
        new Date(String(b.date || b.as_of_date || "")).getTime()
    );

    const lastRow = rows[rows.length - 1];

    const value =
      metricType === "new_orders"
        ? Number(lastRow?.new_orders ?? lastRow?.value ?? lastRow?.orders)
        : Number(lastRow?.pmi ?? lastRow?.value ?? lastRow?.signal);

    latestMap[key] = {
      ...lastRow,
      value: Number.isFinite(value) ? value : null
    };
  });

  return latestMap;
}

function buildCombinedIndustryRows(pmiPkg, ordersPkg) {
  const pmiLatest = Array.isArray(pmiPkg?.industries_latest) ? pmiPkg.industries_latest : [];
  const ordersLatest = Array.isArray(ordersPkg?.industries_latest) ? ordersPkg.industries_latest : [];

  const pmiDeltaLookup = buildIndustryDeltaLookup(pmiPkg);
  const ordersDeltaLookup = buildIndustryDeltaLookup(ordersPkg);

  const pmiMap = buildIndustryLatestMap(pmiPkg, "pmi");
  const ordersMap = buildIndustryLatestMap(ordersPkg, "new_orders");

  const seedMap = new Map();

  [...pmiLatest, ...ordersLatest].forEach((row) => {
    const fullKey = buildIndustryFullKey(row);
    if (!seedMap.has(fullKey)) seedMap.set(fullKey, row);
  });

  return Array.from(seedMap.values()).map((seedRow) => {
    const fullKey = buildIndustryFullKey(seedRow);

    const pmiRow = pmiMap[fullKey] || null;
    const ordersRow = ordersMap[fullKey] || null;

    return {
      pmi_type: seedRow.pmi_type,
      industry: seedRow.industry,

      pmi_level: getPmiValue(pmiRow),
      pmi_delta3m: pmiDeltaLookup[fullKey]?.delta3m ?? null,
      pmi_delta6m: pmiDeltaLookup[fullKey]?.delta6m ?? null,

      orders_level: getOrdersValue(ordersRow),
      orders_delta3m: ordersDeltaLookup[fullKey]?.delta3m ?? null,
      orders_delta6m: ordersDeltaLookup[fullKey]?.delta6m ?? null,

      signal: Number(seedRow.signal ?? pmiRow?.signal ?? 0),
      weight: Number(seedRow.weight ?? pmiRow?.weight ?? ordersRow?.weight ?? 0)
    };
  });
}

function getSignedValueClass(value) {
  return "";  // 🔥 kill all color logic
}

function getRankedDeltaClass(key, value, topSet, bottomSet) {
  return "";
}

function renderEquitiesIndustryTable(container, pmiPkg, ordersPkg) {
  if (!container) return;

  if (
    !pmiPkg ||
    !ordersPkg ||
    !Array.isArray(pmiPkg.industries_latest) ||
    !pmiPkg.industries_latest.length ||
    !Array.isArray(ordersPkg.industries_latest) ||
    !ordersPkg.industries_latest.length
  ) {
    container.innerHTML = `<div class="panel-placeholder">No mapped PMI / Orders industry detail available.</div>`;
    return;
  }

  const enrichedItems = buildCombinedIndustryRows(pmiPkg, ordersPkg);

  const sections = ["manu", "nonmanu"]
    .map((pmiType) => {
      const rows = enrichedItems
        .filter((r) => r.pmi_type === pmiType)
        .sort((a, b) => Math.abs(b.signal) - Math.abs(a.signal));

      if (!rows.length) return "";

      const label = pmiType === "manu" ? "Manufacturing" : "Services";

      const body = rows.map((row) => {
        return `
          <tr>
            <td>${row.industry}</td>
            <td class="num pmi-col">${formatInt(row.pmi_level)}</td>
            <td class="num pmi-col">${formatSignedInt(row.pmi_delta3m)}</td>
            <td class="num pmi-col">${formatSignedInt(row.pmi_delta6m)}</td>

            <td class="num group-sep-left orders-col">${formatInt(row.orders_level)}</td>
            <td class="num orders-col">${formatSignedInt(row.orders_delta3m)}</td>
            <td class="num orders-col">${formatSignedInt(row.orders_delta6m)}</td>

            <td class="num group-sep-left">${formatSignedInt(row.signal)}</td>
            <td class="num">${formatNumber(row.weight, 1)}</td>
          </tr>
        `;
      }).join("");

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
            <tbody>
              ${body}
            </tbody>
          </table>
        </div>
      `;
    })
    .join("");

  const overall = pmiPkg.latest?.overall || {};
  const manu = pmiPkg.latest?.manu || {};
  const nonmanu = pmiPkg.latest?.nonmanu || {};

  container.innerHTML = `
    <div class="equities-pmi-summary-grid">
      <div class="equities-pmi-summary-card">
        <div class="label">Overall</div>
        <div class="value">${overall.signal == null ? "--" : `${overall.signal >= 0 ? "+" : ""}${Number(overall.signal).toFixed(1)}`}</div>
      </div>
      <div class="equities-pmi-summary-card">
        <div class="label">Manu</div>
        <div class="value">${manu.signal == null ? "--" : `${manu.signal >= 0 ? "+" : ""}${Number(manu.signal).toFixed(1)}`}</div>
      </div>
      <div class="equities-pmi-summary-card">
        <div class="label">Services</div>
        <div class="value">${nonmanu.signal == null ? "--" : `${nonmanu.signal >= 0 ? "+" : ""}${Number(nonmanu.signal).toFixed(1)}`}</div>
      </div>
    </div>
    <div class="equities-industry-table-wrap">
      ${sections}
    </div>
  `;
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

async function loadEquitiesData() {
  const results = await Promise.allSettled([
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesIndex),
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesPmiComposite),
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesNewOrdersComposite),
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesBreadth),
    fetchJsonWithBust(DATA_ENDPOINTS.equitiesSigma)
  ]);

  const [indexRes, pmiRes, noRes, breadthRes, sigmaRes] = results;

  return {
    index:
      indexRes.status === "fulfilled"
        ? normalizeEquitiesIndexPayload(indexRes.value)
        : {},
    pmi:
      pmiRes.status === "fulfilled"
        ? normalizeEquitiesPmiPayload(pmiRes.value)
        : { etfs: {} },
    no:
      noRes.status === "fulfilled"
        ? normalizeEquitiesNoPayload(noRes.value)
        : { etfs: {} },
    breadth:
      breadthRes.status === "fulfilled"
        ? normalizeEquitiesPmiSeriesPayload(breadthRes.value)
        : [],
    sigma:
      sigmaRes.status === "fulfilled"
        ? normalizeEquitiesSigmaPayload(sigmaRes.value)
        : [],
    health: {
      index: indexRes.status === "fulfilled",
      pmi: pmiRes.status === "fulfilled",
      no: noRes.status === "fulfilled",
      breadth: breadthRes.status === "fulfilled",
      sigma: sigmaRes.status === "fulfilled"
    }
  };
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

    if (!equitiesData.health.pmi) {
      renderEquitiesIndexPlaceholder("equities-industry-chart", "PMI Composite endpoint unavailable.");
      updateStatValue(document.getElementById("equities-industry-state"), "Endpoint Unavailable");
    } else {
      const industrySubtitle = document.getElementById("equities-industry-subtitle");

            if (isSectorEtf(etfFocus)) {
        const pmiPkg = equitiesData.health.pmi
          ? getEquitiesPmiPackageForEtf(equitiesData.pmi, etfFocus)
          : null;
        const noPkg = equitiesData.health.no
          ? getEquitiesPmiPackageForEtf(equitiesData.no, etfFocus)
          : null;
        const overall = pmiPkg?.latest?.overall || null;

        if (industrySubtitle) {
          industrySubtitle.textContent = `PMI + New Orders Composite — ${etfFocus} | ${getEquitiesTickerLabel(etfFocus)}`;
        }

        if (!equitiesData.health.pmi || !equitiesData.health.no) {
          renderEquitiesIndexPlaceholder(
            "equities-industry-chart",
            "PMI / Orders endpoint unavailable."
          );
          updateStatValue(document.getElementById("equities-industry-pmi"), "--");
          updateStatValue(document.getElementById("equities-industry-bias"), "--");
          updateStatValue(document.getElementById("equities-industry-state"), "Endpoint Unavailable");
        } else if (!pmiPkg || !noPkg || !overall) {
          renderEquitiesIndexPlaceholder(
            "equities-industry-chart",
            `No PMI / Orders composite available for ${etfFocus}.`
          );
          updateStatValue(document.getElementById("equities-industry-pmi"), "--");
          updateStatValue(document.getElementById("equities-industry-bias"), "--");
          updateStatValue(document.getElementById("equities-industry-state"), "No Data");
        } else {
          updateStatValue(
            document.getElementById("equities-industry-pmi"),
            `${overall.signal >= 0 ? "+" : ""}${formatNumber(overall.signal, 2)}`,
            overall.signal >= 0 ? "positive" : "negative"
          );
const biasText = String(overall.bias ?? "").toLowerCase();

updateStatValue(
  document.getElementById("equities-industry-bias"),
  overall.bias ?? "--",
  biasText === "positive" ? "positive" : biasText === "negative" ? "negative" : null
);
          updateStatValue(
            document.getElementById("equities-industry-state"),
            String(overall.state ?? "--")
          );

          renderEquitiesIndustryTable(
            document.getElementById("equities-industry-chart"),
            pmiPkg,
            noPkg
          );

          const industrySource = document.getElementById("equities-industry-source");
          const latestDate =
            pmiPkg.meta?.latest_date ||
            noPkg.meta?.latest_date ||
            pmiPkg.industries_latest?.[0]?.date ||
            noPkg.industries_latest?.[0]?.date ||
            "--";

          if (industryDate) {
            industryDate.textContent = latestDate;
          }

          if (industrySource) {
            industrySource.innerHTML = `Source: Monthly ISM PMI + New Orders + ETF Proxy Mapping | the_Spine | Showing ${formatMonthYearLabel(latestDate)}`;
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
        renderEquitiesIndexPlaceholder("equities-industry-chart", `No industry/lens definition available for ${etfFocus}.`);
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


