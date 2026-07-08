  document.addEventListener("DOMContentLoaded", async () => {
    console.log("APP BOOT OK - WTI SAFE BUILD");

    window.__WTI_SAFE_BUILD__ = true;

    const viewButtons = document.querySelectorAll("[data-view]");
    const contentViews = document.querySelectorAll(".content-view");
    const navParents = document.querySelectorAll(".nav-parent");
    const subnavs = document.querySelectorAll(".subnav");
    const moduleTabs = document.querySelectorAll(
      ".module-tab[data-view], .top-nav-item[data-view]",
    );

    const fxControls = {
      pair: document.getElementById("fx-pair"),
      spreads: document.getElementById("fx-spreads"),
      horizon: document.getElementById("fx-horizon"),
      geoscen: document.getElementById("fx-geoscen-mode"),
      viewMode: document.getElementById("fx-view-mode"),
    };

    fxControls.pair?.addEventListener("change", renderFX);

    const equitiesControls = {
      region: document.getElementById("equities-region"),
      industryMetric: document.getElementById("equities-industry-metric"),
      industrySort: document.getElementById("equities-industry-sort"),
      etfFocus: document.getElementById("equities-etf-focus"),
      horizon: document.getElementById("equities-horizon"),
      topRightMode: document.getElementById("equities-top-right-mode"),
      geoscen: document.getElementById("equities-geoscen-mode"),
    };

    const wtiControls = {
      horizon: document.getElementById("wti-horizon"),
      geoscen: document.getElementById("wti-geoscen-mode"),
      ocOverlay: document.getElementById("wti-oc-overlay"),
    };

    const macroControls = {
      region: document.getElementById("macro-region"),
      horizon: document.getElementById("macro-horizon"),
      regimeFilter: document.getElementById("macro-regime-filter"),
      dataMode: document.getElementById("macro-data-mode"),
    };

    const ratesControls = {
      region: document.getElementById("rates-region"),
      country: document.getElementById("rates-country"),
      horizon: document.getElementById("rates-horizon"),
      metric: document.getElementById("rates-metric"),
      geoscen: document.getElementById("rates-geoscen-mode"),
    };

    const cflowControls = {
      domain: document.getElementById("cflow-domain"),
      subsystem: document.getElementById("cflow-subsystem"),
      metric: document.getElementById("cflow-metric"),
      lens: document.getElementById("cflow-lens"),
      horizon: document.getElementById("cflow-horizon"),
      geoscen: document.getElementById("cflow-geoscen-mode"),
    };

    ratesControls.metric
      ?.querySelector('option[value="duration-term-premium"]')
      ?.removeAttribute("disabled");
    ratesControls.metric
      ?.querySelector('option[value="global-rates-dispersion"]')
      ?.removeAttribute("disabled");



    const FX_DEPTH_DESCRIPTIONS = {
      "AUD/USD": "DEPTHS BETWEEN DOWN & ABOVE ?|? Oz / US",

      "EUR/USD": "DEPTHS ACROSS THE ATLANTIC ?|? EU / US",

      "GBP/USD": "DEPTHS BETWEEN THE CITY ?|? BRITAIN / US",

      "USD/CAD": "DEPTHS BETWEEN NORTH AMERICA ?|? CANADA / US",

      "USD/CHF": "DEPTHS BETWEEN THE HAVEN ?|? SWITZERLAND / US",

      "USD/JPY": "DEPTHS BETWEEN CARRY ?|? JAPAN / US",
    };

    function normalizeMetricName(x) {
      return String(x || "")
        .toLowerCase()
        .replace(/&/g, "and")
        .replace(/\s+/g, " ")
        .replace(/[./-]/g, "")
        .trim();
    }

    let globalEquityRegionPanelData = [];
    let globalEquityRegionLatestData = null;

    function getFXDepthMetricPayload(pair, metricName) {
      const root = fxDepthData?.pairs || fxDepthData || {};
      const pairPayload = root[pair];

      if (!pairPayload || typeof pairPayload !== "object") return null;

      const metrics = pairPayload.metrics || {};
      if (metrics[metricName]?.rows?.length) return metrics[metricName];

      if (pairPayload.metric === metricName && Array.isArray(pairPayload.rows)) {
        return pairPayload;
      }

      return null;
    }

    /* =========================
    FINSTATE CONTROLS
    ========================= */

    const FINSTATE_SIGMA_METRICS_V1 = [
      {
        key: "debt-coverage",
        label: "Debt Coverage Sigma",
        direction: "higher_is_better",
        displayInvert: false,
      },
      {
        key: "fcf-quality",
        label: "FCF Quality Sigma",
        direction: "higher_is_better",
        displayInvert: false,
      },
      {
        key: "margin-stability",
        label: "Margin Stability Sigma",
        direction: "higher_is_better",
        displayInvert: false,
      },
      {
        key: "roic",
        label: "ROIC Sigma",
        direction: "higher_is_better",
        displayInvert: false,
      },
      {
        key: "credit-stress",
        label: "Credit Stress Sigma",
        direction: "higher_raw_is_worse",
        displayInvert: true,
      },
    ];

  const FINSTATE_COUNTRIES_BY_REGION = {
    "north-america": [
      { value: "all", label: "All", disabled: false },
      { value: "usa", label: "USA", disabled: false },
      { value: "canada", label: "Canada (TBD)", disabled: true },
    ],

    "europe-plus": [
      { value: "all", label: "All", disabled: false },
      { value: "france", label: "France", disabled: false },
      { value: "germany", label: "Germany", disabled: false },
      { value: "switzerland", label: "Switzerland", disabled: false },
      { value: "uk", label: "UK", disabled: false },
    ],

    "asia-pacific": [
      { value: "all", label: "All", disabled: false },
      { value: "japan", label: "Japan", disabled: false },
      { value: "australia", label: "Australia", disabled: false },
      { value: "hong-kong", label: "Hong Kong (No Pipe)", disabled: true },
    ],

    global: [
      { value: "all", label: "Global-lite", disabled: false },
    ],
  };

  const FINSTATE_INDUSTRIES_BY_REGION_COUNTRY = {
    "north-america": {
      all: [
        "all",
        "industrials",
        "financials",
        "technology",
        "healthcare",
        "energy",
        "consumer-discretionary",
        "consumer-staples",
        "materials",
        "utilities",
        "communication-services",
      ],
      usa: [
        "all",
        "industrials",
        "financials",
        "technology",
        "healthcare",
        "energy",
        "consumer-discretionary",
        "consumer-staples",
        "materials",
        "utilities",
        "communication-services",
      ],
    },

    "europe-plus": {
      all: ["all", "energy", "industrials", "insurance", "luxury", "pharma"],
      france: ["all", "energy", "industrials", "luxury"],
      germany: ["all", "insurance"],
      switzerland: ["all", "pharma"],
      uk: ["all", "energy"],
    },

    "asia-pacific": {
      all: ["all", "autos", "energy", "mining"],
      japan: ["all", "autos"],
      australia: ["all", "energy", "mining"],
    },

    global: {
      all: ["all", "autos", "energy", "industrials", "insurance", "luxury", "mining", "pharma"],
    },
  };

  const FINSTATE_INDUSTRY_LABELS = {
    all: "All",
    autos: "Autos",
    energy: "Energy",
    financials: "Financials",
    healthcare: "Healthcare",
    industrials: "Industrials",
    insurance: "Insurance",
    luxury: "Luxury",
    materials: "Materials",
    mining: "Mining",
    pharma: "Pharma",
    technology: "Technology",
    utilities: "Utilities",
    "consumer-discretionary": "Consumer Discretionary",
    "consumer-staples": "Consumer Staples",
    "communication-services": "Communication Services",
  };

    const FINSTATE_BASKET_V0 = {
      "communication-services": [
        {
          ticker: "GOOGL",
          company: "Alphabet",
          sector: "Communication Services",
          weight: "--",
        },
        {
          ticker: "META",
          company: "Meta",
          sector: "Communication Services",
          weight: "--",
        },
        {
          ticker: "NFLX",
          company: "Netflix",
          sector: "Communication Services",
          weight: "--",
        },
        {
          ticker: "DIS",
          company: "Disney",
          sector: "Communication Services",
          weight: "--",
        },
        {
          ticker: "TMUS",
          company: "T-Mobile",
          sector: "Communication Services",
          weight: "--",
        },
        {
          ticker: "VZ",
          company: "Verizon",
          sector: "Communication Services",
          weight: "--",
        },
        {
          ticker: "CMCSA",
          company: "Comcast",
          sector: "Communication Services",
          weight: "--",
        },
        {
          ticker: "CHTR",
          company: "Charter Communications",
          sector: "Communication Services",
          weight: "--",
        },
      ],

      "consumer-discretionary": [
        {
          ticker: "AMZN",
          company: "Amazon",
          sector: "Consumer Discretionary",
          weight: "--",
        },
        {
          ticker: "HD",
          company: "Home Depot",
          sector: "Consumer Discretionary",
          weight: "--",
        },
        {
          ticker: "MCD",
          company: "McDonald's",
          sector: "Consumer Discretionary",
          weight: "--",
        },
        {
          ticker: "NKE",
          company: "Nike",
          sector: "Consumer Discretionary",
          weight: "--",
        },
        {
          ticker: "TSLA",
          company: "Tesla",
          sector: "Consumer Discretionary",
          weight: "--",
        },
        {
          ticker: "LOW",
          company: "Lowe's",
          sector: "Consumer Discretionary",
          weight: "--",
        },
        {
          ticker: "BKNG",
          company: "Booking Holdings",
          sector: "Consumer Discretionary",
          weight: "--",
        },
        {
          ticker: "SBUX",
          company: "Starbucks",
          sector: "Consumer Discretionary",
          weight: "--",
        },
      ],

      "consumer-staples": [
        {
          ticker: "PG",
          company: "Procter & Gamble",
          sector: "Consumer Staples",
          weight: "--",
        },
        {
          ticker: "KO",
          company: "Coca-Cola",
          sector: "Consumer Staples",
          weight: "--",
        },
        {
          ticker: "PEP",
          company: "PepsiCo",
          sector: "Consumer Staples",
          weight: "--",
        },
        {
          ticker: "WMT",
          company: "Walmart",
          sector: "Consumer Staples",
          weight: "--",
        },
        {
          ticker: "COST",
          company: "Costco",
          sector: "Consumer Staples",
          weight: "--",
        },
        {
          ticker: "MDLZ",
          company: "Mondelez",
          sector: "Consumer Staples",
          weight: "--",
        },
        {
          ticker: "CL",
          company: "Colgate-Palmolive",
          sector: "Consumer Staples",
          weight: "--",
        },
        {
          ticker: "KMB",
          company: "Kimberly-Clark",
          sector: "Consumer Staples",
          weight: "--",
        },
      ],

      energy: [
        { ticker: "XOM", company: "Exxon Mobil", sector: "Energy", weight: "--" },
        { ticker: "CVX", company: "Chevron", sector: "Energy", weight: "--" },
        {
          ticker: "COP",
          company: "ConocoPhillips",
          sector: "Energy",
          weight: "--",
        },
        { ticker: "SLB", company: "SLB", sector: "Energy", weight: "--" },
        {
          ticker: "EOG",
          company: "EOG Resources",
          sector: "Energy",
          weight: "--",
        },
        {
          ticker: "OXY",
          company: "Occidental Petroleum",
          sector: "Energy",
          weight: "--",
        },
        {
          ticker: "MPC",
          company: "Marathon Petroleum",
          sector: "Energy",
          weight: "--",
        },
        { ticker: "PSX", company: "Phillips 66", sector: "Energy", weight: "--" },
      ],

      financials: [
        {
          ticker: "JPM",
          company: "JPMorgan Chase",
          sector: "Financials",
          weight: "--",
        },
        {
          ticker: "GS",
          company: "Goldman Sachs",
          sector: "Financials",
          weight: "--",
        },
        {
          ticker: "BLK",
          company: "BlackRock",
          sector: "Financials",
          weight: "--",
        },
        {
          ticker: "BAC",
          company: "Bank of America",
          sector: "Financials",
          weight: "--",
        },
        {
          ticker: "MS",
          company: "Morgan Stanley",
          sector: "Financials",
          weight: "--",
        },
        { ticker: "C", company: "Citigroup", sector: "Financials", weight: "--" },
        {
          ticker: "SPGI",
          company: "S&P Global",
          sector: "Financials",
          weight: "--",
        },
        { ticker: "V", company: "Visa", sector: "Financials", weight: "--" },
      ],

      healthcare: [
        {
          ticker: "LLY",
          company: "Eli Lilly",
          sector: "Healthcare",
          weight: "--",
        },
        {
          ticker: "JNJ",
          company: "Johnson & Johnson",
          sector: "Healthcare",
          weight: "--",
        },
        {
          ticker: "UNH",
          company: "UnitedHealth",
          sector: "Healthcare",
          weight: "--",
        },
        { ticker: "PFE", company: "Pfizer", sector: "Healthcare", weight: "--" },
        { ticker: "ABBV", company: "AbbVie", sector: "Healthcare", weight: "--" },
        { ticker: "MRK", company: "Merck", sector: "Healthcare", weight: "--" },
        {
          ticker: "TMO",
          company: "Thermo Fisher",
          sector: "Healthcare",
          weight: "--",
        },
        {
          ticker: "ISRG",
          company: "Intuitive Surgical",
          sector: "Healthcare",
          weight: "--",
        },
      ],

      industrials: [
        {
          ticker: "CAT",
          company: "Caterpillar",
          sector: "Industrials",
          weight: "--",
        },
        {
          ticker: "GE",
          company: "GE Aerospace",
          sector: "Industrials",
          weight: "--",
        },
        {
          ticker: "DE",
          company: "Deere & Co.",
          sector: "Industrials",
          weight: "--",
        },
        { ticker: "ETN", company: "Eaton", sector: "Industrials", weight: "--" },
        {
          ticker: "HON",
          company: "Honeywell",
          sector: "Industrials",
          weight: "--",
        },
        { ticker: "UPS", company: "UPS", sector: "Industrials", weight: "--" },
        {
          ticker: "LMT",
          company: "Lockheed Martin",
          sector: "Industrials",
          weight: "--",
        },
        {
          ticker: "WM",
          company: "Waste Management",
          sector: "Industrials",
          weight: "--",
        },
      ],

      materials: [
        { ticker: "LIN", company: "Linde", sector: "Materials", weight: "--" },
        {
          ticker: "APD",
          company: "Air Products",
          sector: "Materials",
          weight: "--",
        },
        { ticker: "NEM", company: "Newmont", sector: "Materials", weight: "--" },
        {
          ticker: "FCX",
          company: "Freeport-McMoRan",
          sector: "Materials",
          weight: "--",
        },
        {
          ticker: "SHW",
          company: "Sherwin-Williams",
          sector: "Materials",
          weight: "--",
        },
        { ticker: "ECL", company: "Ecolab", sector: "Materials", weight: "--" },
        { ticker: "DD", company: "DuPont", sector: "Materials", weight: "--" },
        {
          ticker: "MLM",
          company: "Martin Marietta",
          sector: "Materials",
          weight: "--",
        },
      ],

      technology: [
        {
          ticker: "MSFT",
          company: "Microsoft",
          sector: "Technology",
          weight: "--",
        },
        { ticker: "NVDA", company: "NVIDIA", sector: "Technology", weight: "--" },
        { ticker: "AAPL", company: "Apple", sector: "Technology", weight: "--" },
        {
          ticker: "GOOGL",
          company: "Alphabet",
          sector: "Technology",
          weight: "--",
        },
        {
          ticker: "AVGO",
          company: "Broadcom",
          sector: "Technology",
          weight: "--",
        },
        { ticker: "AMD", company: "AMD", sector: "Technology", weight: "--" },
        {
          ticker: "CRM",
          company: "Salesforce",
          sector: "Technology",
          weight: "--",
        },
        { ticker: "ORCL", company: "Oracle", sector: "Technology", weight: "--" },
      ],

      utilities: [
        {
          ticker: "D",
          company: "Dominion Energy",
          sector: "Utilities",
          weight: "--",
        },
        {
          ticker: "NEE",
          company: "NextEra Energy",
          sector: "Utilities",
          weight: "--",
        },
        {
          ticker: "DUK",
          company: "Duke Energy",
          sector: "Utilities",
          weight: "--",
        },
        {
          ticker: "SO",
          company: "Southern Company",
          sector: "Utilities",
          weight: "--",
        },
        {
          ticker: "AEP",
          company: "American Electric Power",
          sector: "Utilities",
          weight: "--",
        },
        { ticker: "EXC", company: "Exelon", sector: "Utilities", weight: "--" },
        {
          ticker: "XEL",
          company: "Xcel Energy",
          sector: "Utilities",
          weight: "--",
        },
        {
          ticker: "PEG",
          company: "Public Service Enterprise",
          sector: "Utilities",
          weight: "--",
        },
      ],
    };

  const FINSTATE_GLOBAL_LITE_BASKET = [
    { ticker: "7203", source_ticker: "TM", company: "Toyota Motor", country: "Japan", region: "asia-pacific", sector: "Autos", finstate_sector: "Autos", weight: "--" },
    { ticker: "7267", source_ticker: "HMC", company: "Honda Motor", country: "Japan", region: "asia-pacific", sector: "Autos", finstate_sector: "Autos", weight: "--" },

    { ticker: "RIO", source_ticker: "RIO", company: "Rio Tinto", country: "Australia", region: "asia-pacific", sector: "Mining", finstate_sector: "Mining", weight: "--" },
    { ticker: "WDS", source_ticker: "WDS", company: "Woodside Energy", country: "Australia", region: "asia-pacific", sector: "Energy", finstate_sector: "Energy", weight: "--" },

    { ticker: "BP", source_ticker: "BP", company: "BP", country: "UK", region: "europe-plus", sector: "Energy", finstate_sector: "Energy", weight: "--" },
    { ticker: "TTE", source_ticker: "TTE", company: "TotalEnergies", country: "France", region: "europe-plus", sector: "Energy", finstate_sector: "Energy", weight: "--" },
    { ticker: "SU", source_ticker: "SU", company: "Schneider Electric", country: "France", region: "europe-plus", sector: "Industrials", finstate_sector: "Industrials", weight: "--" },
    { ticker: "MC", source_ticker: "MC", company: "LVMH", country: "France", region: "europe-plus", sector: "Luxury", finstate_sector: "Luxury", weight: "--" },
    { ticker: "ALV", source_ticker: "ALV", company: "Allianz", country: "Germany", region: "europe-plus", sector: "Insurance", finstate_sector: "Insurance", weight: "--" },
    { ticker: "ROG", source_ticker: "ROG", company: "Roche", country: "Switzerland", region: "europe-plus", sector: "Pharma", finstate_sector: "Pharma", weight: "--" },
  ];


  function slugifyFinState(value) {
    return String(value || "")
      .toLowerCase()
      .replace(/\+/g, "plus")
      .replace(/&/g, "and")
      .replace(/\s+/g, "-")
      .replace(/[^a-z0-9-]/g, "")
      .trim();
  }

  function getFinStateBasketRows(industry = "all") {
    const region = document.getElementById("finstate-region")?.value || "north-america";
    const country = document.getElementById("finstate-country")?.value || "all";

    const liveRows = Array.isArray(finstateUniverseData)
      ? finstateUniverseData.filter((row) => row && typeof row === "object")
      : [];

    const globalLiteRows = FINSTATE_GLOBAL_LITE_BASKET.filter((row) => {
      const rowRegion = slugifyFinState(row.region);
      const rowCountry = slugifyFinState(row.country);
      const rowSector = slugifyFinState(row.finstate_sector || row.sector);

      const regionOk = region === "global" || rowRegion === region;
      const countryOk = country === "all" || rowCountry === country;
      const industryOk = industry === "all" || rowSector === industry;

      return regionOk && countryOk && industryOk;
    });

    if (region !== "north-america") {
      return globalLiteRows.slice(0, 8);
    }

    const fallbackRows =
      industry === "all"
        ? Object.values(FINSTATE_BASKET_V0).flat()
        : FINSTATE_BASKET_V0[industry] || [];

    const scopedLive =
      industry === "all"
        ? liveRows
        : liveRows.filter((row) => {
            const sector = slugifyFinState(row.finstate_sector || row.sector);
            return row.finstate_sector_key === industry || row.sector_key === industry || sector === industry;
          });

    const rows = scopedLive.length ? scopedLive : fallbackRows;

    return rows
      .filter((row) => row && typeof row === "object")
      .slice(0, 8);
  }

    const FINSTATE_QUADRANT_MODES = {
      "financial-quality": {
        x: "capital_efficiency_score",
        y: "survivability_score",
        q1: "Durable Compounders",
        q2: "Stable but Inefficient",
        q3: "Structurally Weak",
        q4: "Efficient but Fragile",
      },

      "macro-regime": {
        x: "capital_efficiency_score",
        y: "fragility_inverse",
        q1: "Disinflationary Boom",
        q2: "Defensive Slowdown",
        q3: "Deflationary Bust",
        q4: "Inflationary Boom",
      },

      "stress-regime": {
        x: "credit_stress_score",
        y: "fragility_score",
        q1: "Contained Stress",
        q2: "Balance Sheet Strain",
        q3: "Critical Stress",
        q4: "Operational Fragility",
      },
    };

    function fmtFinState(value, digits = 2) {
      if (
        value === null ||
        value === undefined ||
        value === "" ||
        value === "null" ||
        value === "NaN"
      ) {
        return "--";
      }

      const num = Number(value);

      if (!Number.isFinite(num)) {
        return "--";
      }

      return num.toFixed(digits);
    }

    function updateFinStateUniverseDate(rows = []) {
      const dateNode = document.getElementById("finstate-sigma-date");
      if (!dateNode) return;

      const dates = rows
        .map(
          (row) =>
            row.as_of_date || row.report_date || row.period_end_date || row.date,
        )
        .filter(Boolean)
        .sort();

      dateNode.textContent = dates.length ? dates[dates.length - 1] : "--";
    }

    function getFinStateLensStory(lens, industry = "all") {
      const stories = {
        "capital-efficiency": "Measuring ? Productive Durability",
        fragility: "Measuring ? Structural Fragility",
        survivability: "Measuring ? Stress Survivability",
        "credit-stress": "Measuring ? Refinancing Pressure",
        "balance-sheet-quality": "Measuring ? Balance-Sheet Durability",
        "i2-vinv-divergence": "Measuring ? Fundamental Divergence",
      };

      const base = stories[lens] || "Measuring ? Financial State";

      if (!industry || industry === "all") {
        return base;
      }

      const industryLabel = industry.replaceAll("-", " ").toUpperCase();

      return `${base} ? ${industryLabel}`;
    }

    function renderFinStateLensVisual(lens) {
      const container = document.getElementById("finstate-lens-visual");
      if (!container) return;

      const context = getFinStateVisualContext();

      const renderers = {
        "capital-efficiency": renderCapitalEfficiencyRadar,
        fragility: renderFragilityGauge,
        survivability: renderSurvivabilityRadar,
        "credit-stress": renderCreditStressGauge,
        "balance-sheet-quality": renderBalanceSheetQualityRadar,
      };

      const renderer = renderers[lens];

      if (renderer) {
        renderer(container, context);
        return;
      }

      container.innerHTML = `
      <div class="finstate-lens-placeholder">
        Visual engine reserved for ${formatFinStateLensLabel(lens)}.
      </div>
    `;
    }

    function formatFinStateLensLabel(lens) {
      return String(lens || "")
        .split("-")
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(" ");
    }

    function getFinStateVisualContext() {
      const lens =
        document.getElementById("finstate-analytic-lens")?.value ||
        "capital-efficiency";

      const industry =
        document.getElementById("finstate-industry")?.value || "all";

      const quadrantMode =
        document.getElementById("finstate-quadrant-mode")?.value ||
        "financial-quality";

      const modeLabels = {
        "financial-quality": "Financial Quality",
        "macro-regime": "Macro Regime",
        "stress-regime": "Stress Regime",
      };

      const periodLabels = {
        quarterly: "QRT",
        ttm: "TTM",
        annual: "Annual",
      };

      const modeAdjustment = {
        "financial-quality": 8,
        "macro-regime": -10,
        "stress-regime": -22,
      };

      const periodAdjustment = {
        quarterly: -8,
        ttm: 6,
        annual: 14,
      };

      const period =
        document.getElementById("finstate-period")?.value || "quarterly";

      const industryAdjustmentMap = {
        all: 0,
        technology: 10,
        financials: -8,
        energy: -12,
        healthcare: 6,
        industrials: -4,
        materials: -7,
        utilities: 4,
        "consumer-discretionary": -6,
        "consumer-staples": 5,
        "communication-services": 2,
      };

      return {
        lens,
        industry,
        quadrantMode,
        period,
        modeLabel: modeLabels[quadrantMode] || "Financial Quality",
        periodLabel: periodLabels[period] || "QRT",
        industryLabel:
          industry === "all"
            ? "All Industries"
            : industry.replaceAll("-", " ").toUpperCase(),
        adjustment:
          (modeAdjustment[quadrantMode] || 0) +
          (periodAdjustment[period] || 0) +
          (industryAdjustmentMap[industry] || 0),
      };
    }

    function adjustFinStateMetricValue(value, metricKey, context) {
      const modeProfiles = {
        "financial-quality": {
          roic: 10,
          margin: 8,
          fcf: 7,
          assets: 5,
          leverage: -4,
          durability: 9,
          liquidity: 7,
          coverage: 8,
          debt: -5,
          cash: 6,
          stability: 8,
        },
        "macro-regime": {
          roic: -5,
          margin: -7,
          fcf: -8,
          assets: -3,
          leverage: -10,
          durability: -6,
          liquidity: -8,
          coverage: -9,
          debt: -11,
          cash: 4,
          stability: -6,
        },
        "stress-regime": {
          roic: -10,
          margin: -12,
          fcf: -15,
          assets: -8,
          leverage: -18,
          durability: -13,
          liquidity: -16,
          coverage: -18,
          debt: -20,
          cash: -6,
          stability: -14,
        },
      };

      const periodProfiles = {
        quarterly: {
          roic: -4,
          margin: -3,
          fcf: -6,
          assets: -2,
          leverage: -4,
          durability: -3,
          liquidity: -5,
          coverage: -4,
          debt: -3,
          cash: -2,
          stability: -3,
        },
        ttm: {
          roic: 4,
          margin: 3,
          fcf: 5,
          assets: 2,
          leverage: 2,
          durability: 4,
          liquidity: 3,
          coverage: 4,
          debt: 2,
          cash: 2,
          stability: 4,
        },
        annual: {
          roic: 8,
          margin: 6,
          fcf: 7,
          assets: 5,
          leverage: 4,
          durability: 9,
          liquidity: 5,
          coverage: 6,
          debt: 3,
          cash: 4,
          stability: 8,
        },
      };

      const industryProfiles = {
        technology: {
          roic: 10,
          margin: 8,
          fcf: 6,
          assets: -3,
          leverage: 5,
          durability: 8,
        },
        financials: {
          roic: -5,
          margin: 2,
          fcf: -4,
          assets: 6,
          leverage: -10,
          durability: -3,
        },
        energy: {
          roic: -6,
          margin: -8,
          fcf: 4,
          assets: 7,
          leverage: -6,
          durability: -8,
        },
        healthcare: {
          roic: 6,
          margin: 7,
          fcf: 5,
          assets: 2,
          leverage: 2,
          durability: 9,
        },
        industrials: {
          roic: 1,
          margin: -2,
          fcf: 2,
          assets: 5,
          leverage: -3,
          durability: 1,
        },
        materials: {
          roic: -4,
          margin: -6,
          fcf: -2,
          assets: 4,
          leverage: -5,
          durability: -5,
        },
        utilities: {
          roic: -3,
          margin: 2,
          fcf: 1,
          assets: 6,
          leverage: -8,
          durability: 5,
        },
        "consumer-discretionary": {
          roic: 2,
          margin: -4,
          fcf: -5,
          assets: 1,
          leverage: -4,
          durability: -6,
        },
        "consumer-staples": {
          roic: 5,
          margin: 4,
          fcf: 4,
          assets: 2,
          leverage: 1,
          durability: 8,
        },
        "communication-services": {
          roic: 3,
          margin: 5,
          fcf: 3,
          assets: 1,
          leverage: -2,
          durability: 4,
        },
      };

      const modeAdj = modeProfiles[context.quadrantMode]?.[metricKey] || 0;
      const periodAdj = periodProfiles[context.period]?.[metricKey] || 0;
      const industryAdj = industryProfiles[context.industry]?.[metricKey] || 0;

      const adjusted = Number(value) + modeAdj + periodAdj + industryAdj;

      return Math.max(5, Math.min(95, adjusted));
    }

    function getFinStateModeCaption(baseCaption, context) {
      return baseCaption;
    }

    function renderCapitalEfficiencyRadar(container, context) {
      const metrics = [
        { label: "ROIC", value: adjustFinStateMetricValue(82, "roic", context) },
        {
          label: "Margin",
          value: adjustFinStateMetricValue(74, "margin", context),
        },
        { label: "FCF", value: adjustFinStateMetricValue(68, "fcf", context) },
        {
          label: "Assets",
          value: adjustFinStateMetricValue(71, "assets", context),
        },
        {
          label: "Leverage",
          value: adjustFinStateMetricValue(63, "leverage", context),
        },
        {
          label: "Durability",
          value: adjustFinStateMetricValue(79, "durability", context),
        },
      ];

      renderFinStateRadarTemplate(
        container,
        "Capital Efficiency Radar",
        metrics,
        getFinStateModeCaption(
          "ROIC, Margin & FCF quality, Asset Efficiency, Operating Leverage & Return Durability.",
          context,
        ),
        context,
      );
    }

    function renderFragilityGauge(container, context) {
      const fragilityScore = adjustFinStateMetricValue(64, "leverage", context);

      renderFinStateGaugeTemplate(
        container,
        "Fragility Gauge",
        fragilityScore,
        getFinStateModeCaption(
          "Measures leverage stress, liquidity weakness, FCF compression & credit deterioration.",
          context,
        ),
        context,
      );
    }

    function renderSurvivabilityRadar(container, context) {
      const metrics = [
        {
          label: "Liquidity",
          value: adjustFinStateMetricValue(76, "liquidity", context),
        },
        {
          label: "Coverage",
          value: adjustFinStateMetricValue(69, "coverage", context),
        },
        { label: "FCF", value: adjustFinStateMetricValue(72, "fcf", context) },
        {
          label: "Margins",
          value: adjustFinStateMetricValue(66, "margin", context),
        },
        {
          label: "Leverage",
          value: adjustFinStateMetricValue(61, "leverage", context),
        },
        {
          label: "Durability",
          value: adjustFinStateMetricValue(78, "durability", context),
        },
      ];

      renderFinStateRadarTemplate(
        container,
        "Survivability Radar",
        metrics,
        getFinStateModeCaption(
          "Cash flow stability, liquidity strength, coverage quality & resilience persistence.",
          context,
        ),
        context,
      );
    }

    function renderCreditStressGauge(container, context) {
      const creditStressScore = adjustFinStateMetricValue(58, "debt", context);

      renderFinStateGaugeTemplate(
        container,
        "Credit Stress Gauge",
        creditStressScore,
        getFinStateModeCaption(
          "Measures debt burden, coverage weakness, refinancing risk & balance-sheet pressure.",
          context,
        ),
        context,
      );
    }

    function renderBalanceSheetQualityRadar(container, context) {
      const metrics = [
        { label: "Cash", value: adjustFinStateMetricValue(70, "cash", context) },
        { label: "Debt", value: adjustFinStateMetricValue(62, "debt", context) },
        {
          label: "Coverage",
          value: adjustFinStateMetricValue(68, "coverage", context),
        },
        {
          label: "Assets",
          value: adjustFinStateMetricValue(73, "assets", context),
        },
        {
          label: "Working Cap",
          value: adjustFinStateMetricValue(64, "liquidity", context),
        },
        {
          label: "Stability",
          value: adjustFinStateMetricValue(71, "stability", context),
        },
      ];

      renderFinStateRadarTemplate(
        container,
        "Balance Sheet Quality Radar",
        metrics,
        getFinStateModeCaption(
          "Liquidity, leverage quality, asset structure, working capital & structural stability.",
          context,
        ),
        context,
      );
    }

    function renderFinStateRadarTemplate(
      container,
      title,
      metrics,
      caption,
      context,
    ) {
      const cx = 150;
      const cy = 150;
      const radius = 100;

      const points = metrics
        .map((m, i) => {
          const angle = ((-90 + (i * 360) / metrics.length) * Math.PI) / 180;
          const r = radius * (m.value / 100);
          return `${cx + r * Math.cos(angle)},${cy + r * Math.sin(angle)}`;
        })
        .join(" ");

      const axisLines = metrics
        .map((m, i) => {
          const angle = ((-90 + (i * 360) / metrics.length) * Math.PI) / 180;
          const x = cx + radius * Math.cos(angle);
          const y = cy + radius * Math.sin(angle);
          const lx = cx + (radius + 24) * Math.cos(angle);
          const ly = cy + (radius + 24) * Math.sin(angle);

          return `
        <line x1="${cx}" y1="${cy}" x2="${x}" y2="${y}" stroke="rgba(255,255,255,.16)" />
        <text x="${lx}" y="${ly}" text-anchor="middle" dominant-baseline="middle"
          fill="rgba(255,255,255,.62)" font-size="10">${m.label}</text>
      `;
        })
        .join("");

      container.innerHTML = `
      <div class="finstate-radar-wrap">

        <svg class="finstate-radar-svg" viewBox="0 0 300 300">
          <circle cx="${cx}" cy="${cy}" r="100" fill="none" stroke="rgba(255,255,255,.10)" />
          <circle cx="${cx}" cy="${cy}" r="70" fill="none" stroke="rgba(255,255,255,.08)" />
          <circle cx="${cx}" cy="${cy}" r="40" fill="none" stroke="rgba(255,255,255,.06)" />
          ${axisLines}
          <polygon points="${points}" fill="rgba(213,179,124,.22)" stroke="#d5b37c" stroke-width="2" />
        </svg>
        <div class="finstate-visual-caption">
    ${caption}
  </div>
      </div>
    `;
    }

    function renderFinStateGaugeTemplate(
      container,
      title,
      score,
      caption,
      context,
    ) {
      const rotation = -90 + (score / 100) * 180;

      container.innerHTML = `
      <div class="finstate-gauge-wrap">
      
        <svg class="finstate-gauge-svg" viewBox="0 0 300 180">
          <path d="M 50 150 A 100 100 0 0 1 250 150"
            fill="none" stroke="rgba(255,255,255,.12)" stroke-width="20" stroke-linecap="round" />
          <path d="M 50 150 A 100 100 0 0 1 250 150"
            fill="none" stroke="#d5b37c" stroke-width="20" stroke-linecap="round"
            stroke-dasharray="${score * 3.14} 314" />
          <line x1="150" y1="150" x2="150" y2="70"
            stroke="rgba(255,255,255,.85)" stroke-width="3"
            transform="rotate(${rotation} 150 150)" />
          <circle cx="150" cy="150" r="6" fill="rgba(255,255,255,.85)" />
          <text x="150" y="132" text-anchor="middle" fill="rgba(255,255,255,.9)" font-size="26">
            ${score}
          </text>
          <text x="150" y="166" text-anchor="middle" fill="rgba(255,255,255,.55)" font-size="11">
            PRESSURE SCORE
          </text>
        </svg>
        <div class="finstate-visual-caption">
          ${caption}
        </div>
      </div>
    `;
    }

    function getFinStatePeriodProfile(period) {
      const profiles = {
        quarterly: {
          label: "Quarterly",
          suffix: "QRT",
          fragMultiplier: 1.18,
          fcfMultiplier: 0.92,
          debtMultiplier: 1.08,
        },
        ttm: {
          label: "TTM",
          suffix: "TTM",
          fragMultiplier: 1.0,
          fcfMultiplier: 1.0,
          debtMultiplier: 1.0,
        },
        annual: {
          label: "Annual",
          suffix: "ANN",
          fragMultiplier: 0.86,
          fcfMultiplier: 1.12,
          debtMultiplier: 0.94,
        },
      };

      return profiles[period] || profiles.quarterly;
    }

    function getFinStateNumber(row, keys, fallback = null) {
      for (const key of keys) {
        const value = row?.[key];
        const num = parseFloat(value);

        if (Number.isFinite(num)) {
          return num;
        }
      }

      return fallback;
    }

  function remapFinStateRowByPeriod(row, period) {
    const profile = getFinStatePeriodProfile(period);

    const tickerKey = String(row.ticker || row.symbol || "").toUpperCase();

    const globalMetric = finstateGlobalLiteMetricsData.find(
      (m) => String(m.ticker || "").toUpperCase() === tickerKey,
    );

    const frag = getFinStateNumber(
      globalMetric || row,
      ["fragility_pct_qrt", "fragility", "fragility_score", "fragility_pct", "frag_pct"],
      0,
    );

    const fcf = getFinStateNumber(
      globalMetric || row,
      ["fcf_b_qrt", "fcfy", "fcf", "fcf_b", "free_cash_flow", "free_cash_flow_b"],
      0,
    );

    const debt = getFinStateNumber(
      globalMetric || row,
      ["debt_eq_qrt", "debtEq", "debt_equity", "debt_to_equity", "debt_eq"],
      0,
    );

    return {
      ...row,
      ticker: row.ticker || row.symbol || "--",
      company: row.company || row.name || row.company_name || "--",
      fragility_period: Number(frag) * profile.fragMultiplier,
      fcfy_period: Number(fcf),
      debtEq_period: Number(debt),
      iv_score: row.iv_score ?? row.iv_t ?? row.iv ?? null,
      sigma_delta: row.sigma_delta ?? row.sigmaDelta ?? row.sigma_change ?? null,
    };
  }

    document
      .getElementById("finstate-period")
      ?.addEventListener("change", renderFinState);

    function getFinStateRegionUniverseTitle(region, industry) {
      const regionLabels = {
        "north-america": "US",
        "europe-plus": "EU+",
        "asia-pacific": "APAC",
        global: "GLOBAL",
      };

      const regionLabel = regionLabels[region] || "US";
      const industryLabel =
        industry && industry !== "all"
          ? ` ? ${industry.replaceAll("-", " ").toUpperCase()}`
          : "";

      return `FIN ? STATE | ${regionLabel} ? UNIVERSE${industryLabel}`;
    }

    function renderFinStateConstituents() {
      const industry =
        document.getElementById("finstate-industry")?.value || "all";

      const region =
        document.getElementById("finstate-region")?.value || "north-america";

      const period =
        document.getElementById("finstate-period")?.value || "quarterly";

      const periodProfile = getFinStatePeriodProfile(period);

      const container = document.getElementById("finstate-constituents-table");
      if (!container) return;

      const title = document.getElementById("finstate-universe-title");

      if (title) {
        title.textContent = getFinStateRegionUniverseTitle(region, industry);
      }

      container.classList.remove("panel-placeholder");

      const rows = getFinStateBasketRows(industry).map((row) =>
        remapFinStateRowByPeriod(row, period),
      );

      if (!rows.length) {
        container.innerHTML = `
        <div class="panel-placeholder">
          No FINSTATE constituents available.
        </div>
      `;
        return;
      }

      container.innerHTML = `
      <div class="finstate-constituents-wrap">
        <table class="finstate-constituents-table">
          <thead>
            <tr class="finstate-group-row">
              <th colspan="5">FinState Core</th>
              <th colspan="3">IsoVector Engine</th>
            </tr>
            <tr>
              <th class="finstate-col-ticker">Ticker</th>
              <th class="finstate-col-company">Company</th>
              <th class="finstate-col-core">Frag% ${periodProfile.suffix}</th>
              <th class="finstate-col-core">FCF(B) ${periodProfile.suffix}</th>
              <th class="finstate-col-core">Debt/Eq ${periodProfile.suffix}</th>
              <th class="finstate-col-engine">IV[t]</th>
              <th class="finstate-col-engine">Weight</th>
              <th class="finstate-col-engine">SigmaÎ”</th>
            </tr>
          </thead>
          <tbody>
            ${rows
              .map(
                (row) => `
              <tr>
                <td>${row.ticker || "--"}</td>
                <td>${row.company || row.name || "--"}</td>
                <td>${fmtFinState(row.fragility_period)}</td>
                <td>${fmtFinState(row.fcfy_period)}</td>
                <td>${fmtFinState(row.debtEq_period)}</td>
                <td>${fmtFinState(row.iv_score)}</td>
                <td>${row.weight || "--"}</td>
                <td>${fmtFinState(row.sigma_delta)}</td>
              </tr>
            `,
              )
              .join("")}
          </tbody>
        </table>
      </div>
    `;
    }

    const CFLOW_VECTOR_SKELETON = [
      {
        key: "P",
        name: "Pressure",
        layer: "System Pressure",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "F",
        name: "Fragility",
        layer: "Structural Fragility",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "L",
        name: "Liquidity",
        layer: "Funding Liquidity",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "D",
        name: "Dispersion",
        layer: "Structural Fragility",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "M",
        name: "Momentum",
        layer: "Economic Momentum",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "X",
        name: "Cross-Market Stress",
        layer: "Cross-Asset Transmission",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "C",
        name: "Coherence",
        layer: "Cross-Asset Transmission",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "S",
        name: "Systemicity",
        layer: "System Outcome",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
    ];

  const CFLOW_MENU = {
    physical: {
      label: "Econ",
      subsystems: {
        activity: {
          label: "Activity",
          metrics: [
            { value: "weekly-economic-index", label: "Redbook" },
            { value: "industrial-production", label: "Industrial Production" },
            { value: "real-personal-income", label: "Real Personal Income" },
            { value: "retail-sales", label: "Retail Sales" },
            { value: "building-permits", label: "Building Permits" },
            { value: "capacity-utilization", label: "Capacity Utilization" },
            { value: "consumer-sentiment", label: "Consumer Sentiment" },
          ],
        },

        labor: {
          label: "Labor",
          metrics: [
            { value: "labor-composite", label: "Labor Composite" },
            { value: "jolts-openings", label: "JOLTS Openings" },
            { value: "initial-jobless-claims", label: "Initial Claims" },
            { value: "weekly-hours-worked", label: "Weekly Hours Worked" },
          ],
        },

        inflation: {
          label: "Inflation",
          metrics: [
            { value: "inflation-composite", label: "Inflation Composite" },
            { value: "core-pce", label: "Core PCE" },
            { value: "core-cpi", label: "Core CPI" },
            { value: "ppi-finished-goods", label: "PPI Finished Goods" },
          ],
        },

        energy: {
          label: "Energy",
          metrics: [
            { value: "energy-composite", label: "Energy Composite" },
            { value: "diesel-demand", label: "Diesel Demand" },
            { value: "distillate-inventories", label: "Distillate Inventories" },
          ],
        },

        logistics: {
          label: "Logistics",
          metrics: [
            { value: "transport-transmission-composite", label: "Transport Composite" },
            { value: "cass-freight-shipments", label: "Cass Freight Shipments" },
            { value: "freight-transportation-services", label: "Freight Transportation Services" },
            { value: "rail-freight-carloads", label: "Rail Freight Carloads" },
            { value: "rail-freight-intermodal", label: "Rail Freight Intermodal" },
            { value: "container-shipping-index", label: "Container Shipping Index" },
            { value: "baltic-dry-index", label: "Baltic Dry Proxy" },
          ],
        },

        composite: {
          label: "Composite",
          metrics: [
            { value: "financial-transmission-composite", label: "Financial Composite" },
            { value: "capital-composite", label: "Capital Composite" },
            { value: "liquidity-constraint-composite", label: "Liquidity Constraint Composite" },
            { value: "fragility-composite", label: "Fragility Composite" },
            { value: "dispersion-composite", label: "Dispersion Composite" },
            { value: "cflow-iv-vector-contribution", label: "C?FLOW IV[t] Vector Contribution" },
            { value: "cflow-regime-engine", label: "C?FLOW Regime Engine" },
            { value: "cflow-regime-definitions", label: "C?FLOW Regime Definitions" },
            { value: "cflow-completion-ledger", label: "C?FLOW Completion Ledger" },
          ],
        },
      },
    },

    financial: {
      label: "Capital",
      subsystems: {
        composite: {
          label: "Composite",
          metrics: [
            { value: "financial-transmission-composite", label: "Financial Composite" },
            { value: "liquidity-constraint-composite", label: "Liquidity Constraint Composite" },
          ],
        },

        funding: {
          label: "Funding",
          metrics: [
            { value: "sofr-funding", label: "SOFR Funding Stress" },
            { value: "funding-stress-composite", label: "Funding Stress Composite" },
          ],
        },

        credit: {
          label: "Credit",
          metrics: [
            { value: "hy-oas", label: "High Yield OAS" },
            { value: "credit-transmission-composite", label: "Credit Composite" },
          ],
        },

        positioning: {
          label: "Positioning",
          metrics: [
            { value: "cot-positioning", label: "COT Positioning" },
          ],
        },
      },
    },
  };
    

    const FINSTATE_IV_VECTOR_SKELETON = [
      {
        key: "P",
        name: "Pressure",
        layer: "System Pressure",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "F",
        name: "Fragility",
        layer: "Structural Fragility",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "L",
        name: "Liquidity",
        layer: "Funding Liquidity",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "D",
        name: "Dispersion",
        layer: "Structural Fragility",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "M",
        name: "Momentum",
        layer: "Economic Momentum",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "X",
        name: "Cross-Asset Stress",
        layer: "Cross-Asset Transmission",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "C",
        name: "Coherence",
        layer: "Cross-Asset Transmission",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
      {
        key: "S",
        name: "Systemicity",
        layer: "System Outcome",
        value: "--",
        change: "--",
        period: "QRT",
        direction: "--",
      },
    ];

    function getFinStateTemporalLabel(period) {
      const labels = {
        quarterly: "QRT",
        ttm: "TTM",
        annual: "YR",
      };

      return labels[period] || "QRT";
    }

    function getFinStateIVColorClass(key) {
      if (["P", "L", "M"].includes(key)) return "finstate-iv-plm";
      if (["F", "D"].includes(key)) return "finstate-iv-fd";
      if (["X", "C"].includes(key)) return "finstate-iv-xc";
      if (key === "S") return "finstate-iv-s";
      return "";
    }

    function renderFinStateIVVector() {
      const period =
        document.getElementById("finstate-period")?.value || "quarterly";

      const temporalLabel = getFinStateTemporalLabel(period);

      const items = FINSTATE_IV_VECTOR_SKELETON.map((item) => ({
        ...item,
        period: temporalLabel,
      }));

      renderModuleIVVector(
        "finstate-iv-vector",
        items,
        MODULE_QUESTIONS.finstate,
      );
    }

    function renderModuleIVVector(containerId, items, question) {
      const container = document.getElementById(containerId);
      if (!container) return;

      container.innerHTML = `
      ${question ? `<div class="iv-module-question">${question}</div>` : ""}
      <div class="finstate-iv-vector-grid">
        ${items
          .map(
            (item) => `
          <div class="finstate-iv-vector-card">
            <div class="finstate-iv-letter-box ${getFinStateIVColorClass(item.key)}">
              ${item.key === "S"
                ? `<span class="iv-systemicity-letter">S</span>`
                : item.key}
            </div>
            <div class="finstate-iv-meta">
              <div class="finstate-iv-topline">
                <span class="finstate-iv-name">${item.name}</span>
                <span class="finstate-iv-direction">${item.direction || "--"}</span>
              </div>
              <div class="finstate-iv-layer">${item.layer}</div>
              <div class="finstate-iv-bottomline">
                <span class="finstate-iv-value">${item.value || "--"}</span>
                <span class="finstate-iv-period">${item.period || "QRT"}</span>
                <span class="finstate-iv-change">${item.change || "--"}</span>
              </div>
            </div>
          </div>
        `,
          )
          .join("")}
      </div>
    `;
    }

    const FX_VECTOR_SKELETON = [
      { key: "P", name: "Pressure", layer: "FX Pressure", value: "--" },
      { key: "L", name: "Liquidity", layer: "Funding Liquidity", value: "--" },
      { key: "M", name: "Momentum", layer: "Directional Momentum", value: "--" },
      {
        key: "X",
        name: "Cross-Asset Stress",
        layer: "FX Transmission",
        value: "--",
      },
      { key: "C", name: "Coherence", layer: "Regime Coherence", value: "--" },
    ];

    const RATES_VECTOR_SKELETON = [
      { key: "P", name: "Pressure", layer: "Rates Pressure", value: "--" },
      { key: "L", name: "Liquidity", layer: "Funding Conditions", value: "--" },
      { key: "D", name: "Dispersion", layer: "Curve Dispersion", value: "--" },
      { key: "M", name: "Momentum", layer: "Yield Momentum", value: "--" },
      { key: "C", name: "Coherence", layer: "Policy Coherence", value: "--" },
      { key: "S", name: "Systemicity", layer: "System Outcome", value: "--" },
    ];

    const EQUITIES_VECTOR_SKELETON = [
      { key: "P", name: "Pressure", layer: "Equity Pressure", value: "--" },
      { key: "F", name: "Fragility", layer: "Market Fragility", value: "--" },
      { key: "L", name: "Liquidity", layer: "Market Liquidity", value: "--" },
      { key: "D", name: "Dispersion", layer: "Sector Dispersion", value: "--" },
      { key: "M", name: "Momentum", layer: "Index Momentum", value: "--" },
      {
        key: "X",
        name: "Cross-Asset Stress",
        layer: "Equity Transmission",
        value: "--",
      },
      { key: "C", name: "Coherence", layer: "Breadth Coherence", value: "--" },
      { key: "S", name: "Systemicity", layer: "System Outcome", value: "--" },
    ];

    function renderFXVector() {
      renderModuleIVVector("fx-vector", FX_VECTOR_SKELETON, MODULE_QUESTIONS.fx);
    }

    async function renderRatesVector() {
      try {
        const payload = await fetchJsonWithBust(DATA_ENDPOINTS.ratesIVVector);
        const vector = Array.isArray(payload?.vector) ? payload.vector : [];

        if (!vector.length) throw new Error("No RATES IV[t] vector rows.");

        const items = vector.map((item) => ({
          key: item.key,
          name: item.name,
          layer: item.layer,
          value: item.value == null ? "--" : formatNumber(item.value, 2),
          change: item.state || "--",
          period: item.period || "QRT",
          direction: "",
        }));

        renderModuleIVVector(
          "rates-vector",
          items,
          MODULE_QUESTIONS.rates,
        );

        return;
      } catch (err) {
        console.error("Failed loading RATES IV[t] vector", err);
      }

      renderModuleIVVector(
        "rates-vector",
        RATES_VECTOR_SKELETON,
        MODULE_QUESTIONS.rates,
      );
    }

    function escapeRatesStateText(value) {
      return String(value ?? "--")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    async function renderRatesStateEngine() {
      const panel = document.getElementById("rates-state-engine-panel");
      if (!panel) return;

      panel.hidden = false;
      panel.innerHTML = `
        <div class="panel-title">RATES State Engine</div>
        <div class="panel-placeholder">Loading RATES State Engine...</div>
      `;

      try {
        const [payload, regimePayload] = await Promise.all([
          fetchJsonWithBust(DATA_ENDPOINTS.ratesStateEngine),
          fetchJsonWithBust(DATA_ENDPOINTS.ratesRegimeEngine),
        ]);
        const drivers = Array.isArray(payload?.drivers)
          ? payload.drivers.slice(0, 3)
          : [];
        const regime = regimePayload?.regime || "--";

        if (!payload?.state) {
          throw new Error("No RATES State Engine payload.");
        }

        panel.innerHTML = `
          <div class="panel-title">RATES State Engine</div>

          <div class="chart-header-row">
            <div class="chart-stat">
              <span class="chart-stat-label">State</span>
              <span class="chart-stat-value">${escapeRatesStateText(payload.state)}</span>
            </div>
            <div class="chart-stat">
              <span class="chart-stat-label">Score</span>
              <span class="chart-stat-value">${formatNumber(payload.score, 1)}</span>
            </div>
            <div class="chart-stat">
              <span class="chart-stat-label">As Of</span>
              <span class="chart-stat-value">${escapeRatesStateText(payload.as_of)}</span>
            </div>
            <div class="chart-stat">
              <span class="chart-stat-label">Regime</span>
              <span class="chart-stat-value">${escapeRatesStateText(regime)}</span>
            </div>
          </div>

          <div class="panel-placeholder" style="display:block;text-align:left;">
            <div style="color:#d5b37c;font-size:.75rem;margin-bottom:8px;">
              TOP DRIVERS
            </div>
            ${
              drivers.length
                ? drivers
                    .map(
                      (driver) => `
                        <div style="display:flex;justify-content:space-between;gap:12px;margin-bottom:6px;color:#eef3fa;font-size:.8rem;">
                          <span>${escapeRatesStateText(driver.key)} | ${escapeRatesStateText(driver.name)}</span>
                          <span>${formatNumber(driver.value, 2)} | ${escapeRatesStateText(driver.state)}</span>
                        </div>
                      `,
                    )
                    .join("")
                : `<div style="color:#8f9aac;font-size:.8rem;">No active drivers.</div>`
            }

            <div style="margin-top:12px;color:#8f9aac;font-size:.78rem;line-height:1.45;">
              ${escapeRatesStateText(payload.diagnostic)}
            </div>
          </div>

          <div class="panel-footnote">
            Source: rates_state_engine.json | Deterministic | No forecast
          </div>
        `;
      } catch (err) {
        console.error("Failed loading RATES State Engine", err);
        panel.innerHTML = `
          <div class="panel-title">RATES State Engine</div>
          <div class="panel-placeholder">RATES State Engine unavailable.</div>
        `;
      }
    }

    function renderEquitiesVector() {
      renderModuleIVVector(
        "equities-vector",
        EQUITIES_VECTOR_SKELETON,
        MODULE_QUESTIONS.equities,
      );
    }

  async function renderCFlowVector() {
    try {
      const url =
        DATA_ENDPOINTS.cflow["cflow-iv-vector-contribution"];

      const response = await fetch(`${url}?v=${Date.now()}`, {
        cache: "no-store",
      });
      if (!response.ok) throw new Error(`Fetch failed: ${response.status}`);

      const payload = await response.json();
      const vector = payload?.iv_vector || {};

      const items = CFLOW_VECTOR_SKELETON.map((item) => {
        const live = vector[item.key];

        if (!live) return item;

        return {
          ...item,
          value: formatNumber(live.score, 2),
          change: live.state || "--",
          period: "QRT",
        };
      });

      renderModuleIVVector(
        "cflow-vector",
        items,
        MODULE_QUESTIONS.cflow,
      );

      return;
    } catch (err) {
      console.error("Failed loading C?FLOW IV[t] contribution", err);
    }

    renderModuleIVVector(
      "cflow-vector",
      CFLOW_VECTOR_SKELETON,
      MODULE_QUESTIONS.cflow,
    );
  }

  async function renderCflowOracleChamber() {
    const url = DATA_ENDPOINTS.oraclechambers?.["cflow-chamber"];

    try {
      const response = await fetch(`${url}?v=${Date.now()}`, {
        cache: "no-store",
      });
      if (!response.ok) throw new Error(`Fetch failed: ${response.status}`);

      const payload = await response.json();

      const latest = payload.latest || {};
      const attribution = payload.attribution || {};

      const target = document.querySelector(".cflow-liquidity-panel");
      if (!target) return;

      const drivers = (attribution.drivers || [])
        .map((x) => `${x.vector} ${formatNumber(x.score, 2)}`)
        .join(" ? ");

      const offsets = (attribution.offsets || [])
        .map((x) => `${x.vector} ${formatNumber(x.score, 2)}`)
        .join(" ? ");

      target.innerHTML = `
        <div class="panel-title">ORACLE CHAMBER ? C?FLOW</div>

        <div class="chart-header-row">
          <div class="chart-stat">
            <span class="chart-stat-label">Latest</span>
            <span class="chart-stat-value">${formatNumber(latest.cflow_score, 3)}</span>
          </div>

          <div class="chart-stat">
            <span class="chart-stat-label">C?FLOW</span>
            <span class="chart-stat-value">${formatNumber(latest.cflow_score, 2)}</span>
          </div>

          <div class="chart-stat">
            <span class="chart-stat-label">Mode</span>
            <span class="chart-stat-value">Explain</span>
          </div>
        </div>

        <div class="panel-placeholder" style="display:block;text-align:left;">
          <div><b>Observation</b></div>
          <div style="margin-bottom:10px;">${payload.observation || "--"}</div>

          <div><b>Diagnosis</b></div>
          <div style="margin-bottom:10px;">${payload.diagnosis || "--"}</div>

          <div><b>Drivers</b></div>
          <div>${drivers || "--"}</div>

          <div style="margin-top:8px;"><b>Offsets</b></div>
          <div>${offsets || "--"}</div>
        </div>

        <div class="panel-footnote">
          Oracle Chambers explains deterministic state only. No forecast.
        </div>
      `;
    } catch (err) {
      const target = document.querySelector(".cflow-liquidity-panel");
      if (target) {
        target.innerHTML = `
          <div class="panel-title">ORACLE CHAMBER | CFLOW</div>
          <div class="panel-placeholder">Oracle chamber endpoint pending.</div>
        `;
      }
    }
  }

  async function renderOracleChamberCard(endpointKey, containerSelector, fallbackTitle) {
    const url = DATA_ENDPOINTS.oraclechambers?.[endpointKey];
    const container = document.querySelector(containerSelector);

    if (!container) return;

    if (!url) {
      container.innerHTML = `<div class="panel-placeholder">No endpoint registered.</div>`;
      return;
    }

    try {
      const payload = await fetchJsonWithBust(url);
      const latest = payload.latest || {};
      const measurement = payload.measurement || [];
      const attribution = payload.attribution || {};

      const drivers = attribution.drivers || [];
      const offsets = attribution.offsets || [];

      container.innerHTML = `
        <div class="panel-title">${payload.metric || fallbackTitle}</div>

        <div class="cflow-live-card">
          <div style="font-size:2rem;font-weight:700;color:#eef3fa;">
            ${latest.score ?? latest.cflow_score ?? "--"}
          </div>

          <div style="margin-top:8px;color:#d5b37c;font-weight:600;">
            ${latest.state ?? latest.regime ?? "--"}
          </div>

          <div style="margin-top:10px;color:#8f9aac;font-size:.78rem;">
            ${payload.observation || ""}
          </div>

          <div style="margin-top:16px;text-align:left;">
            <div style="color:#d5b37c;font-size:.75rem;margin-bottom:6px;">
              PRIMARY DRIVERS
            </div>
            ${drivers
              .map(
                d => `
                  <div style="display:flex;justify-content:space-between;font-size:.78rem;color:#eef3fa;">
                    <span>${d.component || d.vector || d.chamber || "--"}</span>
                    <span>${d.score ?? d.value ?? "--"}</span>
                  </div>
                `
              )
              .join("")}
          </div>

          <div style="margin-top:14px;text-align:left;">
            <div style="color:#657184;font-size:.75rem;margin-bottom:6px;">
              MEASUREMENT
            </div>
            ${measurement
              .slice(0, 5)
              .map(
                m => `
                  <div style="display:flex;justify-content:space-between;font-size:.76rem;color:#8f9aac;">
                    <span>${m.component || m.vector || m.chamber || "--"}</span>
                    <span>${m.score ?? m.value ?? "--"}</span>
                  </div>
                `
              )
              .join("")}
          </div>

          <div style="margin-top:14px;color:#657184;font-size:.72rem;">
            ${latest.date || "Latest available"}
          </div>
        </div>
      `;
    } catch (err) {
      console.error(`Failed loading ${endpointKey}`, err);
      container.innerHTML = `<div class="panel-placeholder">Failed loading chamber.</div>`;
    }
  }

  async function renderRatesOracleChamber() {
    await renderOracleChamberCard(
      "rates-chamber",
      "#rates-chamber-panel",
      "Rates Chamber"
    );
  }

  async function renderFxOracleChamber() {
    await renderOracleChamberCard(
      "fx-chamber",
      "#fx-chamber-panel",
      "FX Chamber"
    );
  }

  async function renderEquitiesOracleChamber() {
    await renderOracleChamberCard(
      "equities-chamber",
      "#equities-chamber-panel",
      "Equities Chamber"
    );
  }

  function setCFlowLatestHeader(latestValue) {
    const latestLabel = document.querySelector(
      ".cflow-quality-panel .chart-header-row .chart-stat:first-child .chart-stat-label",
    );
    const latestNode = document.getElementById("cflow-regime-label");
    const scoreNode = document.getElementById("cflow-regime-score");
    const stateNode = document.getElementById("cflow-regime-state");

    if (latestLabel) latestLabel.textContent = "Latest";
    if (latestNode) latestNode.textContent = latestValue ?? "--";
    if (scoreNode) scoreNode.textContent = "";
    if (stateNode) stateNode.textContent = "";
  }

  async function renderCflowRegimeEngine() {
    const url = DATA_ENDPOINTS.cflow["cflow-regime-engine"];

    try {
      const payload = await fetchJsonWithBust(url);
      const latest = payload.latest || {};
      const attribution = payload.attribution || {};

      const score = formatNumber(latest.cflow_score, 3);

      const drivers = (attribution.primary_drivers || [])
        .map(([key, value]) => `${key} ${formatNumber(value, 2)}`)
        .join(" ? ");

      const offsets = (attribution.primary_offsets || [])
        .map(([key, value]) => `${key} ${formatNumber(value, 2)}`)
        .join(" ? ");

      const summary = document.getElementById("cflow-regime-summary");
      const note = document.getElementById("cflow-regime-note");

      setCFlowLatestHeader(score);

      if (summary) {
        summary.innerHTML = `
          <div>
            <div>Drivers: ${drivers || "--"}</div>
            <div>Offsets: ${offsets || "--"}</div>
          </div>
        `;
      }

      if (note) {
        note.textContent = attribution.diagnostic_note || "";
      }
    } catch (err) {
      console.error("Failed loading C?FLOW Regime Engine", err);
    }
  }

  async function renderCflowRegimeDefinitions() {
    const url = DATA_ENDPOINTS.cflow["cflow-regime-definitions"];

    try {
      const payload = await fetchJsonWithBust(url);
      const regimes = payload.regimes || {};

      const title = document.querySelector(".cflow-quality-panel .panel-title");
      const body = document.querySelector(".cflow-quality-panel .panel-placeholder");

      if (title) title.textContent = "C?FLOW REGIME DEFINITIONS";

      setCFlowLatestHeader("--");

      if (body) {
        body.innerHTML = `
          <div class="cflow-live-card" style="text-align:left;">
            ${Object.entries(regimes)
              .map(([name, cfg]) => `
                <div style="margin-bottom:12px;">
                  <div style="font-weight:700;color:#eef3fa;">${name}</div>
                  <div style="font-size:.78rem;color:#8f9aac;">
                    ${cfg.description || "--"}
                  </div>
                </div>
              `)
              .join("")}
          </div>
        `;
      }
    } catch (err) {
      console.error("Failed loading C?FLOW Regime Definitions", err);
    }
  }

  function renderCoreModelIVVector() {
    renderModuleIVVector(
      "core-model-iv-vector",
      FINSTATE_IV_VECTOR_SKELETON,
      "IV Components"
    );
  }

    function renderFinState() {
      const lens =
        document.getElementById("finstate-analytic-lens")?.value ||
        "i2-vinv-divergence";

      const period =
        document.getElementById("finstate-period")?.value || "quarterly";

      const industry =
        document.getElementById("finstate-industry")?.value || "all";

      const quadrantMode =
        document.getElementById("finstate-quadrant-mode")?.value ||
        "financial-quality";

      const quadrantLabelMap = {
        "financial-quality": "Financial Quality",
        "macro-regime": "Macro Regime",
        "stress-regime": "Stress Regime",
      };

      const lensLabels = {
        "i2-vinv-divergence": "IÂ² vs. VinV Divergence",
        survivability: "Survivability",
        fragility: "Fragility",
        "capital-efficiency": "Capital Efficiency",
        "credit-stress": "Credit Stress",
        "balance-sheet-quality": "Balance Sheet Quality",
      };

      const periodLabels = {
        quarterly: "QRT",
        ttm: "TTM",
        annual: "Annual",
      };

      const activeQuadrantLabel =
        quadrantLabelMap[quadrantMode] || "Financial Quality";

      const title = document.getElementById("finstate-analytic-title");

      if (title) {
        title.innerHTML =
          `<div>${lensLabels[lens] || "IÂ² vs. VinV Divergence"} Lens | ` +
          `${activeQuadrantLabel} Interpretation | ` +
          `${periodLabels[period] || "QRT"}</div>` +
          `<div class="finstate-lens-story">${getFinStateLensStory(lens, industry)}</div>`;
      }

      renderFinStateLensVisual(lens);
      renderFinStateConstituents();
      renderFinStateIVVector();

      loadFinStateSigma()
        .then((payload) => renderFinStateSigmaPanel(payload))
        .catch((error) => {
          console.error("FinState Sigma load failed:", error);

          const chart = document.getElementById("finstate-sigma-chart");
          const date = document.getElementById("finstate-sigma-date");

          if (chart) {
            chart.innerHTML = `<div class="panel-placeholder">FinState Sigma Rank load failed.</div>`;
          }

          if (date) {
            date.textContent = "--";
          }
        });
    }

    function updateFinStateCountryOptions() {
      const region =
        document.getElementById("finstate-region")?.value || "north-america";
      const countrySelect = document.getElementById("finstate-country");
      if (!countrySelect) return;

      const countries =
        FINSTATE_COUNTRIES_BY_REGION[region] ||
        FINSTATE_COUNTRIES_BY_REGION["north-america"];

      countrySelect.innerHTML = "";

      countries.forEach((country, index) => {
        const option = document.createElement("option");
        option.value = country.value;
        option.textContent = country.label;
        option.disabled = country.disabled;
        if (index === 0) option.selected = true;
        countrySelect.appendChild(option);
      });
    }
    function updateFinStateIndustryOptions() {
    const region =
      document.getElementById("finstate-region")?.value || "north-america";

    const country =
      document.getElementById("finstate-country")?.value || "all";

    const industrySelect = document.getElementById("finstate-industry");
    if (!industrySelect) return;

    const currentValue = industrySelect.value;

    const industries =
      FINSTATE_INDUSTRIES_BY_REGION_COUNTRY?.[region]?.[country] ||
      FINSTATE_INDUSTRIES_BY_REGION_COUNTRY?.[region]?.all ||
      FINSTATE_INDUSTRIES_BY_REGION_COUNTRY["north-america"].all;

    industrySelect.innerHTML = "";

    industries.forEach((industry) => {
      const option = document.createElement("option");
      option.value = industry;
      option.textContent = FINSTATE_INDUSTRY_LABELS[industry] || industry;
      industrySelect.appendChild(option);
    });

    industrySelect.value = industries.includes(currentValue)
      ? currentValue
      : "all";
  }


    document
      .getElementById("finstate-analytic-lens")
      ?.addEventListener("change", renderFinState);

  document.getElementById("finstate-region")?.addEventListener("change", () => {
    updateFinStateCountryOptions();
    updateFinStateIndustryOptions();
    renderFinState();
  });

  document.getElementById("finstate-country")?.addEventListener("change", () => {
    updateFinStateIndustryOptions();
    renderFinState();
  });

    document
      .getElementById("finstate-industry")
      ?.addEventListener("change", renderFinState);

    async function loadFinStateSigma() {
      return fetchJsonWithBust(DATA_ENDPOINTS.finstateSigma);
    }



    function normalizeFinStateSigmaPayload(payload) {
      if (!payload) {
        return {
          meta: {},
          rows: [],
        };
      }

      return {
        meta: payload.meta || {},
        rows: Array.isArray(payload.rows) ? payload.rows : [],
      };
    }

    function coerceFinStateSigmaRows(rows) {
      if (!Array.isArray(rows)) return [];

      return rows
        .map((row) => {
          const rawZ = Number(row.sigma_z);

          if (!Number.isFinite(rawZ)) return null;

          const displayInvert =
            row.direction === "higher_raw_is_worse" || row.displayInvert === true;

          const displayZ = rawZ;

          return {
            key: String(row.key || "").trim(),
            metric: String(row.metric || row.label || "").trim(),
            pair: String(row.metric || row.label || "").trim(),
            symbol: String(row.key || row.metric || "").trim(),
            z: displayZ,
            raw_z: rawZ,
            rank: Number(row.rank),
            direction: row.direction || "higher_is_better",
            displayInvert,
            state: displayZ >= 0 ? "Stronger" : "Weaker",
          };
        })
        .filter((row) => row && row.metric && Number.isFinite(row.z))
        .sort((a, b) => b.z - a.z);
    }

    function renderFinStateSigmaPanel(payload) {
      const normalized = normalizeFinStateSigmaPayload(payload);
      const rows = coerceFinStateSigmaRows(normalized.rows);

      const chart = document.getElementById("finstate-sigma-chart");
      const date = document.getElementById("finstate-sigma-date");

      if (date) {
        date.textContent = normalized.meta?.as_of_date || "--";
      }

      if (!chart) return;

      if (!rows.length) {
        chart.innerHTML = `<div class="panel-placeholder">No FinState Sigma Rank data available.</div>`;
        return;
      }

      renderAssetSigmaChart(chart, rows, rows[0]?.pair);
    }

    const RATES_COUNTRY_LABELS = {
      AU: "Australia",
      CA: "Canada",
      DE: "Germany",
      EU: "Euro Area",
      ES: "Spain",
      FR: "France",
      JP: "Japan",
      KR: "South Korea",
      UK: "United Kingdom",
      CH: "Switzerland",
      IT: "Italy",
      US: "United States",
      CN: "China",
    };

    const RATES_REGION_MAP = {
      north_america: ["US", "CA"],
      europe_plus: ["DE", "FR", "IT", "ES", "UK", "EU"],
      asia_pacific: ["JP", "CN", "AU", "KR"],
    };

    const RATES_COUNTRIES_BY_REGION = {
      north_america: [
        { value: "US", label: "United States" },
        { value: "CA", label: "Canada" },
      ],
      europe_plus: [
        { value: "EU", label: "Euro Area" },
        { value: "DE", label: "Germany" },
        { value: "FR", label: "France" },
        { value: "IT", label: "Italy" },
        { value: "ES", label: "Spain" },
        { value: "UK", label: "United Kingdom" },
      ],
      asia_pacific: [
        { value: "JP", label: "Japan" },
        { value: "CN", label: "China" },
        { value: "AU", label: "Australia" },
        { value: "KR", label: "South Korea" },
      ],
    };

    const RATES_SIGMA_COUNTRIES_BY_REGION = {
      north_america: ["US", "CA"],
      europe_plus: ["EU", "DE", "FR", "IT", "ES", "UK"],
      asia_pacific: ["JP", "AU", "CN", "KR"],
    };

    const RATES_SIGMA_DISPLAY_LABELS = {
      US: "US",
      CA: "CA",
      EU: "EU",
      IT: "Italy",
      UK: "UK",
      DE: "Germany",
      ES: "Spain",
      FR: "France",
      JP: "JP",
      AU: "AU",
      CN: "China",
      KR: "South Korea",
    };

    const RATES_COMPONENTS = [
      "Yield Family",
      "Curve Structure",
      "Policy Restriction",
      "Real Yield Pressure",
      "Duration / Term Premium",
      "Global Rates Dispersion",
      "Central Bank Commentary",
      "Regional Rates Filter",
      "Rates State Engine",
      "Rates Regime Engine",
      "Rates Oracle Chamber",
      "IV[t] RATES",
    ];

    const CENTRAL_BANK_COMMENTARY_REGISTRY = {
      purpose: "Measure policy tone and transmission language.",
      rule: {
        allowed: "Summarize current policy language.",
        forbidden: "Predict policy path.",
      },
      outputs: [
        "policy_tone",
        "restriction_language",
        "inflation_focus",
        "growth_risk_language",
        "liquidity_language",
        "guidance_uncertainty",
      ],
      sources: {
        Fed: { region: "north_america", country: "US" },
        BoC: { region: "north_america", country: "CA" },
        ECB: { region: "europe_plus", country: "EU" },
        BoE: { region: "europe_plus", country: "UK" },
        BoJ: { region: "asia_pacific", country: "JP" },
        RBA: { region: "asia_pacific", country: "AU" },
        PBoC: { region: "asia_pacific", country: "CN" },
      },
    };

    const CENTRAL_BANK_LANGUAGE_BRANDS = {
      US: {
        brand: "FedSpeak",
        institution: "Federal Reserve",
      },
      CA: {
        brand: "Dominion",
        institution: "Bank of Canada",
      },
      EU: {
        brand: "ECB",
        institution: "European Central Bank",
      },
      UK: {
        brand: "Threadneedle",
        institution: "Bank of England",
      },
      DE: {
        brand: "Bundes",
        institution: "Bundesbank",
      },
      FR: {
        brand: "Republique",
        institution: "Banque de France",
      },
      IT: {
        brand: "Roma",
        institution: "Banca d’Italia",
      },
      ES: {
        brand: "Iberia",
        institution: "Banco de Espa\u00f1a",
      },
      JP: {
        brand: "Sakura",
        institution: "Bank of Japan",
      },
      CN: {
        brand: "Dragon",
        institution: "PBoC",
      },
      AU: {
        brand: "Southern Cross",
        institution: "RBA",
      },
      KR: {
        brand: "Taebaek",
        institution: "Bank of Korea",
      },
    };

    const CENTRAL_BANK_LANGUAGE_STATE_LABELS = {
      SH: "Strong Hawkish",
      MH: "Moderate Hawkish",
      N: "Neutral",
      MD: "Moderate Dovish",
      SD: "Strong Dovish",
    };

    function formatPolicyPctHtml(value) {
      const num = Number(value);

      if (!Number.isFinite(num) || num === 0) {
        return `<span class="policy-pct-empty">--</span>`;
      }

      return `${num}%`;
    }

    function renderCentralBankLanguageBlock({
      country = "US",
      brand,
      institution,
      distribution = {},
      state = "Sentiment placeholder",
      latestDate = "--",
    } = {}) {
      const resolved = CENTRAL_BANK_LANGUAGE_BRANDS[country] || {};
      const displayBrand = brand || resolved.brand || "Policy Language";
      const displayInstitution =
        institution || resolved.institution || "Central Bank";
      const displayDistribution = {
        SH: distribution.SH,
        MH: distribution.MH,
        N: distribution.N,
        MD: distribution.MD,
        SD: distribution.SD,
      };
      const displayState = state || "Sentiment placeholder";
      const displayDate = formatRatesLanguageDate(latestDate);

      const strip = ["SH", "MH", "N", "MD", "SD"]
        .map(
          (code) => `
            <span class="central-bank-language-segment central-bank-language-segment-${code.toLowerCase()}" title="${CENTRAL_BANK_LANGUAGE_STATE_LABELS[code]}">
              ${formatPolicyPctHtml(displayDistribution[code])} ${code}
            </span>
          `,
        )
        .join('<span class="central-bank-language-pipe">|</span>');

      return `
        <div class="central-bank-language-block" data-country="${country}">
          <div class="central-bank-language-topbar">
            <div class="central-bank-language-meta">
              <span class="central-bank-language-brand">${displayBrand}</span>
              <span class="central-bank-language-dot">•</span>
              <span class="central-bank-language-date">${displayDate}</span>
            </div>
            <div class="central-bank-language-strip" aria-label="Policy language distribution">
              <span class="central-bank-language-bracket">•|</span>
              ${strip}
              <span class="central-bank-language-bracket">|•</span>
            </div>
          </div>

          <div class="central-bank-language-rule" aria-hidden="true"></div>

          <div class="central-bank-language-body">
            <div class="central-bank-language-context">GeoScen Context (Home / Systemic)</div>
            <div class="central-bank-language-context-value">Policy-language context</div>
            <div class="central-bank-language-state-wrap" aria-label="Current Language State">
              <div class="central-bank-language-state-label">Current Language State / Placeholder</div>
              ${displayState}
            </div>
          </div>
        </div>
      `;
    }

    async function renderRatesPolicyLanguageLatest() {
      const panel = document.querySelector("#view-rates .rates-policy-panel");
      const title = document.querySelector("#view-rates .rates-policy-panel .panel-title");
      const chart = document.getElementById("rates-policy-chart");
      const state = document.getElementById("rates-policy-state");
      const date = document.getElementById("rates-policy-date");
      const source = document.getElementById("rates-policy-source");

      if (!chart) return;

      try {
        panel?.classList.remove("rates-metric-blank");

        const selectedCountry = ratesControls.country?.value || "US";
        const brandMeta = CENTRAL_BANK_LANGUAGE_BRANDS[selectedCountry] || {};
        const countryLabel = RATES_COUNTRY_LABELS[selectedCountry] || selectedCountry;

        const commentaryUrl =
          DATA_ENDPOINTS.ratesCentralBankCommentary?.[selectedCountry];

        if (!commentaryUrl) {
          throw new Error(`Policy language endpoint pending for ${countryLabel}`);
        }

        const payload = await fetchJsonWithBust(commentaryUrl);
        const p = payload.policy_language || {};
        const coverage = payload.coverage || {};

        if (title) title.textContent = "Central Bank Commentary";
        if (state) state.textContent = `${coverage.classified_pct ?? "--"}% Coverage`;
        if (date) date.textContent = payload.document_date || payload.as_of || "--";

        if (source) {
          source.innerHTML =
            `Source: ${payload.central_bank || brandMeta.institution || countryLabel} | the_Spine | As of ` +
            `<span id="rates-policy-date">${payload.document_date || "--"}</span>`;
        }

        chart.innerHTML = renderCentralBankLanguageBlock({
          country: selectedCountry,
          brand: brandMeta.brand,
          institution: payload.central_bank || brandMeta.institution,
          latestDate: payload.document_date || payload.as_of || "--",
          state: `Latest statement only • ${coverage.classified_pct ?? "--"}% classified`,
          distribution: {
            SH: p.SH_pct,
            MH: p.MH_pct,
            N: p.N_pct,
            MD: p.MD_pct,
            SD: p.SD_pct,
          },
        });
      } catch (err) {
        console.error("Failed loading RATES policy language.", err);
        chart.innerHTML = `
          <div class="panel-placeholder">
            Policy language endpoint pending for ${RATES_COUNTRY_LABELS[ratesControls.country?.value] || "selected country"}.
          </div>
        `;
      }
    }    

    function formatRatesLanguageDate(value) {
      if (!value || value === "--") return "--";

      const date = new Date(`${value}T00:00:00`);
      if (Number.isNaN(date.getTime())) return value;

      const month = date.toLocaleString("en-US", { month: "short" });
      const year = String(date.getFullYear()).slice(-2);
      return `${month} '${year}`;
    }

    const RATES_HORIZON_DAYS = {
      "90D": 90,
      "1Y": 365,
      "3Y": 365 * 3,
      MAX: Infinity,
    };

    function getRatesRegionCountries(regionKey = "north_america") {
      return RATES_REGION_MAP[regionKey] || RATES_REGION_MAP.north_america;
    }

    function updateRatesCountryOptions() {
      if (!ratesControls.country) return;

      const regionKey = ratesControls.region?.value || "north_america";
      const countries =
        RATES_COUNTRIES_BY_REGION[regionKey] ||
        RATES_COUNTRIES_BY_REGION.north_america;
      const previous = ratesControls.country.value;

      ratesControls.country.innerHTML = countries
        .map(
          (country) =>
            `<option value="${country.value}">${country.label}</option>`,
        )
        .join("");

      ratesControls.country.value = countries.some(
        (country) => country.value === previous,
      )
        ? previous
        : countries[0]?.value || "US";
    }

    function filterRatesRowsByRegion(rows = [], regionKey = "north_america") {
      const countries = getRatesRegionCountries(regionKey);

      return rows.filter((row) => {
        const candidates = [
          row.country,
          row.region,
          row.market,
          row.country_code,
          row.iso2,
          row.iso3,
        ].map((value) => String(value || "").toUpperCase().replace(/[^A-Z]/g, ""));

        return countries.some((code) => {
          const label = String(RATES_COUNTRY_LABELS[code] || "")
            .toUpperCase()
            .replace(/[^A-Z]/g, "");
          return candidates.includes(code) || candidates.includes(label);
        });
      });
    }

    function filterRatesSeriesByHorizon(rows = [], horizon = "1Y") {
      if (!Array.isArray(rows) || !rows.length) return [];

      const days = RATES_HORIZON_DAYS[horizon] ?? RATES_HORIZON_DAYS["1Y"];
      if (!Number.isFinite(days)) return rows;

      const latest = new Date(`${rows[rows.length - 1].date}T00:00:00`);
      if (Number.isNaN(latest.getTime())) {
        return rows.slice(-Math.max(days, 2));
      }

      const cutoff = new Date(latest);
      cutoff.setDate(cutoff.getDate() - days);

      const scopedRows = rows.filter((row) => {
        const date = new Date(`${row.date}T00:00:00`);
        return !Number.isNaN(date.getTime()) && date >= cutoff;
      });

      return scopedRows.length >= 2 ? scopedRows : rows.slice(-2);
    }



    function renderRatesMetricTargetBlank() {
      const panel = document.querySelector("#view-rates .rates-policy-panel");
      const title = document.querySelector(
        "#view-rates .rates-policy-panel .panel-title",
      );
      const chart = document.getElementById("rates-policy-chart");
      const state = document.getElementById("rates-policy-state");
      const date = document.getElementById("rates-policy-date");
      const source = document.getElementById("rates-policy-source");
      const metricLabel =
        ratesControls.metric?.selectedOptions?.[0]?.textContent?.trim() ||
        "Metric";

      panel?.classList.add("rates-metric-blank");
      if (title) title.textContent = metricLabel;
      if (chart) chart.innerHTML = "";
      if (state) state.textContent = "";
      if (date) date.textContent = "";
      if (source) source.textContent = "";
    }

    function getNumericField(row, candidateFields) {
      for (const field of candidateFields || []) {
        const value = row?.[field];
        const num = Number(value);
        if (Number.isFinite(num)) return num;
      }

      return null;
    }

    function rowsToMetricSeries(rows, candidateFields) {
      return (Array.isArray(rows) ? rows : [])
        .map((row) => ({
          date: row.date || row.as_of_date,
          value: getNumericField(row, candidateFields),
        }))
        .filter((row) => row.date && Number.isFinite(row.value))
        .sort((a, b) => String(a.date).localeCompare(String(b.date)));
    }

    function setRatesPanelMeta({ title, state, date, source } = {}) {
      const titleEl = document.querySelector(
        "#view-rates .rates-policy-panel .panel-title",
      );
      const stateEl = document.getElementById("rates-policy-state");
      const dateEl = document.getElementById("rates-policy-date");
      const sourceEl = document.getElementById("rates-policy-source");

      if (titleEl && title != null) titleEl.textContent = title;
      if (stateEl && state != null) stateEl.textContent = state;
      if (dateEl && date != null) dateEl.textContent = date;
      if (sourceEl && source != null) sourceEl.textContent = source;
    }

    function getRatesSeriesDate(series) {
      return series?.[series.length - 1]?.date || "--";
    }

    function renderRatesSeriesToChart({
      chartId,
      rows,
      fields,
      emptyMessage,
      stateId,
      title,
      stateLabel,
      source,
    }) {
      const series = filterRatesSeriesByHorizon(
        rowsToMetricSeries(rows, fields),
        ratesControls.horizon?.value || "1Y",
      );

      if (series.length) {
        renderSpreadChart(document.getElementById(chartId), series);
        const last = series[series.length - 1];
        if (stateId) {
          updateStatValue(
            document.getElementById(stateId),
            stateLabel || formatNumber(last.value, 2),
          );
        }
        if (chartId === "rates-policy-chart") {
          setRatesPanelMeta({
            title,
            state: stateLabel || formatNumber(last.value, 2),
            date: last.date,
            source,
          });
        }
        return series;
      }

      setChartPlaceholder(chartId, emptyMessage);
      if (stateId) updateStatValue(document.getElementById(stateId), "--");
      if (chartId === "rates-policy-chart") {
        setRatesPanelMeta({ title, state: "--", date: "--", source });
      }
      return [];
    }

    async function renderRatesYieldFamilyMetric(context) {
      const yieldFields = {
        y2: ["y2", "2Y", "two_year", "yield_2y", "2y_yield", "DGS2"],
        y5: ["y5", "5Y", "five_year", "yield_5y", "5y_yield", "DGS5"],
        y10: ["y10", "10Y", "ten_year", "yield_10y", "10y_yield", "DGS10", "y10_proxy"],
        y20: ["y20", "20Y", "twenty_year", "yield_20y", "20y_yield", "DGS20"],
        y30: ["y30", "30Y", "thirty_year", "yield_30y", "30y_yield", "DGS30"],
      };

      let yieldFamilyDisplayRows = [];

      if (context.selectedCountry === "US") {
        const yfPayload = await fetchJsonWithBust(DATA_ENDPOINTS.ratesYieldFamilyChart);
        const yfRows = Array.isArray(yfPayload?.chart?.rows) ? yfPayload.chart.rows : [];
        yieldFamilyDisplayRows = filterRatesSeriesByHorizon(
          yfRows
            .map((row) => ({
              date: row.date,
              y2: getNumericField(row, yieldFields.y2),
              y5: getNumericField(row, yieldFields.y5),
              y10: getNumericField(row, yieldFields.y10),
              y20: getNumericField(row, yieldFields.y20),
              y30: getNumericField(row, yieldFields.y30),
            }))
            .filter((row) =>
              row.date &&
              ["y2", "y5", "y10", "y20", "y30"].some(
                (field) => Number.isFinite(row[field]) && row[field] > 0,
              ),
            ),
          context.horizon,
        );
      } else {
        yieldFamilyDisplayRows = filterRatesSeriesByHorizon(
          context.countryRows
            .map((row) => ({
              date: row.date || row.as_of_date,
              y2: getNumericField(row, yieldFields.y2),
              y5: getNumericField(row, yieldFields.y5),
              y10: getNumericField(row, yieldFields.y10),
              y20: getNumericField(row, yieldFields.y20),
              y30: getNumericField(row, yieldFields.y30),
            }))
            .filter((row) => row.date),
          context.horizon,
        );
      }

      const yieldSeriesDefs = [
        { field: "y2", label: "2Y", color: "#60a5fa" },
        { field: "y5", label: "5Y", color: "#2dd4bf" },
        { field: "y10", label: "10Y", color: "#d5b37c" },
        { field: "y20", label: "20Y", color: "#f59e0b" },
        { field: "y30", label: "30Y", color: "#c084fc" },
      ];

      if (yieldFamilyDisplayRows.length) {
        renderRatesYieldFamilyChart(
          document.getElementById("rates-policy-chart"),
          yieldFamilyDisplayRows,
          yieldSeriesDefs,
        );
        const latestDate = getRatesSeriesDate(yieldFamilyDisplayRows);
        setRatesPanelMeta({
          title: "Yield Family",
          state: context.selectedCountryLabel,
          date: latestDate,
          source: "Source: the_Spine | R2 Yield Family",
        });
        return;
      }

      setChartPlaceholder(
        "rates-policy-chart",
        `No Yield Family data for ${context.selectedCountryLabel}`,
      );

      setRatesPanelMeta({
        title: "Yield Family",
        state: "--",
        date: "--",
        source: "Source: the_Spine | R2 Yield Family",
      });
    }

    function renderRatesCurveStructureTopRightMetric(context) {
      const spreadFields = [
        "spread",
        "us_10y_2y_spread",
        "us_30y_5y_spread",
        "us_20y_10y_spread",
        "us_30y_10y_spread",
        "curve_spread",
        "yield_spread",
        "term_spread",
      ];

      const series = context.countryRows
        .map((row) => ({
          date: row.date || row.as_of_date,
          value: getNumericField(row, spreadFields),
        }))
        .filter((row) => row.date && Number.isFinite(row.value))
        .sort((a, b) => String(a.date).localeCompare(String(b.date)));

      const displaySeries = filterRatesSeriesByHorizon(
        series,
        context.horizon || "1Y",
      );

      if (!displaySeries.length) {
        setChartPlaceholder(
          "rates-policy-chart",
          `No Curve Structure data for ${context.selectedCountryLabel}`,
        );

        setRatesPanelMeta({
          title: "Curve Structure",
          state: "--",
          date: "--",
          source: "Source: the_Spine | Rates Curve Structure",
        });

        return [];
      }

      renderSpreadChart(
        document.getElementById("rates-policy-chart"),
        displaySeries,
      );

      const latest = displaySeries[displaySeries.length - 1];

      setRatesPanelMeta({
        title: "Curve Structure",
        state: formatNumber(latest.value, 2),
        date: latest.date,
        source: "Source: the_Spine | Rates Curve Structure",
      });

      return displaySeries;
    }

    

    function renderRatesCurveStructureMetric(context) {
      const series = renderRatesSeriesToChart({
        chartId: "rates-spread-chart",
        rows: context.countryRows,
        fields: [
          "spread",
          "us_10y_2y_spread",
          "us_30y_5y_spread",
          "us_20y_10y_spread",
          "us_30y_10y_spread",
        ],
        emptyMessage: `No Curve Structure data for ${context.selectedCountryLabel}`,
        stateId: "rates-spread-state",
      });

      const spreadDate = document.getElementById("rates-spread-date");
      if (spreadDate) spreadDate.textContent = getRatesSeriesDate(series);
      return series;
    }

    async function renderRatesPolicyRestrictionMetric(context) {
      if (context.selectedCountry === "US") {
        const payload = await fetchJsonWithBust(DATA_ENDPOINTS.ratesPolicyRestrictionChart);
        const rows = Array.isArray(payload?.chart?.rows) ? payload.chart.rows : [];

        const series = filterRatesSeriesByHorizon(
          rows
            .map((row) => ({
              date: row.date,
              value: getNumericField(row, ["restriction_score"]),
            }))
            .filter((row) => row.date && Number.isFinite(row.value)),
          context.horizon,
        );

        if (series.length) {
          renderSpreadChart(document.getElementById("rates-policy-chart"), series);

          const latest = series[series.length - 1];

          setRatesPanelMeta({
            title: "Policy Restriction",
            state: formatNumber(latest.value, 2),
            date: latest.date,
            source: payload.source || "Source: the_Spine | Policy Restriction",
          });

          return series;
        }
      }

      return renderRatesSeriesToChart({
        chartId: "rates-policy-chart",
        rows: context.countryRows,
        fields: [
          "policy_pressure_t1",
          "policy_proxy",
          "policy_restriction",
          "real_policy_rate",
          "policy_gap",
        ],
        emptyMessage: `No Policy Restriction data for ${context.selectedCountryLabel}`,
        stateId: "rates-policy-state",
        title: "Policy Restriction",
        source: "Source: the_Spine | Policy Restriction",
      });
    }

      async function renderRatesRealYieldPressureMetric(context) {
        if (context.selectedCountry === "US") {
          const payload = await fetchJsonWithBust(DATA_ENDPOINTS.ratesRealYieldPressureChart);
          const rows = Array.isArray(payload?.chart?.rows) ? payload.chart.rows : [];

          const series = filterRatesSeriesByHorizon(
            rows
              .map((row) => ({
                date: row.date,
                value: getNumericField(row, ["real_yield_pressure"]),
              }))
              .filter((row) => row.date && Number.isFinite(row.value)),
            context.horizon,
          );

          if (series.length) {
            renderSpreadChart(document.getElementById("rates-policy-chart"), series);

            const latest = series[series.length - 1];

            setRatesPanelMeta({
              title: "Real Yield Pressure",
              state: formatNumber(latest.value, 2),
              date: latest.date,
              source: payload.source || "Source: the_Spine | Real Yield Pressure",
            });

            return series;
          }
        }

        const realYieldSeries = context.countryRows
          .map((row) => {
            const explicitRealYield = getNumericField(row, [
              "real_yield_pressure",
              "real_yield",
              "real_yield_10y",
              "us_10y_real_yield",
              "tips_10y",
              "dfii10",
              "DFII10",
            ]);

            const nominal10y = getNumericField(row, [
              "y10",
              "10Y",
              "yield_10y",
              "10y_yield",
              "DGS10",
              "nominal_10y",
            ]);

            const policyProxy = getNumericField(row, [
              "policy_proxy",
              "policy_rate",
              "fed_funds",
              "EFFR",
            ]);

            const value =
              explicitRealYield ??
              (nominal10y != null && policyProxy != null
                ? nominal10y - policyProxy
                : null);

            return {
              date: row.date || row.as_of_date,
              value,
            };
          })
          .filter((row) => row.date && Number.isFinite(row.value))
          .sort((a, b) => String(a.date).localeCompare(String(b.date)));

        const series = filterRatesSeriesByHorizon(realYieldSeries, context.horizon);

        if (series.length) {
          renderSpreadChart(document.getElementById("rates-policy-chart"), series);

          const latest = series[series.length - 1];

          setRatesPanelMeta({
            title: "Real Yield Pressure",
            state: formatNumber(latest.value, 2),
            date: latest.date,
            source: "Source: the_Spine | Real Yield Pressure",
          });

          return series;
        }

        setChartPlaceholder(
          "rates-policy-chart",
          `No Real Yield Pressure data for ${context.selectedCountryLabel}`,
        );

        setRatesPanelMeta({
          title: "Real Yield Pressure",
          state: "--",
          date: "--",
          source: "Source: the_Spine | Real Yield Pressure",
        });

        return [];
      }

    function renderRatesIVMetric(context) {
      const latestRow = context.countryRows[context.countryRows.length - 1] || {};
      const spread = getNumericField(latestRow, [
        "spread",
        "us_10y_2y_spread",
        "us_30y_5y_spread",
      ]);
      const policy = getNumericField(latestRow, [
        "policy_pressure_t1",
        "policy_proxy",
        "policy_restriction",
      ]);
      const realYield = getNumericField(latestRow, [
        "real_yield",
        "real_yield_10y",
        "us_10y_real_yield",
        "tips_10y",
      ]);
      const y10 = getNumericField(latestRow, ["y10", "10Y", "yield_10y", "DGS10"]);

      const items = [
        { key: "P", name: "Pressure", layer: "Rates Pressure", value: formatNumber(y10, 2) },
        { key: "L", name: "Liquidity", layer: "Funding Conditions", value: "--" },
        { key: "D", name: "Dispersion", layer: "Curve Dispersion", value: formatNumber(spread, 2) },
        { key: "M", name: "Momentum", layer: "Yield Momentum", value: "--" },
        { key: "C", name: "Coherence", layer: "Policy Coherence", value: formatNumber(policy, 2) },
        { key: "S", name: "Systemicity", layer: "Real Yield Pressure", value: formatNumber(realYield, 2) },
      ];

      renderModuleIVVector("rates-vector", items, MODULE_QUESTIONS.rates);
      setRatesPanelMeta({
        title: "IV[t] RATES",
        state: "Vector",
        date: latestRow.date || latestRow.as_of_date || "--",
        source: "Source: the_Spine deterministic rates inputs",
      });
      setChartPlaceholder("rates-policy-chart", "IV[t] RATES vector updated.");
    }

    function renderRatesDurationTermPremiumMetric(context) {
      if (context.selectedCountry === "US") {
        return fetchJsonWithBust(DATA_ENDPOINTS.ratesDurationTermPremium).then((payload) => {
          const rows = Array.isArray(payload?.chart?.rows)
            ? payload.chart.rows
            : [];
          const series = filterRatesSeriesByHorizon(
            rows
              .map((row) => ({
                date: row.date || row.as_of_date,
                value: getNumericField(row, ["duration_term_premium"]),
              }))
              .filter((row) => row.date && Number.isFinite(row.value)),
            context.horizon,
          );

          if (series.length) {
            renderSpreadChart(document.getElementById("rates-policy-chart"), series);
            const latestSeries = series[series.length - 1];
            const latestValue = Number(payload?.latest?.value);
            const latest = {
              date: payload?.latest?.date || latestSeries.date,
              value: Number.isFinite(latestValue) ? latestValue : latestSeries.value,
              state: payload?.latest?.state || null,
            };
            const dateText = formatRatesLanguageDate(latest.date);
            const sourceText =
              payload?.source ||
              payload?.method ||
              "Source: the_Spine | Duration / Term Premium";

            setRatesPanelMeta({
              title: "Duration / Term Premium",
              state: latest.state
                ? `${formatNumber(latest.value, 2)} / ${latest.state}`
                : formatNumber(latest.value, 2),
              date: dateText,
              source: sourceText,
            });

            const sourceEl = document.getElementById("rates-policy-source");
            if (sourceEl) {
              sourceEl.innerHTML = `${escapeRatesStateText(sourceText)} | As of <span id="rates-policy-date">${escapeRatesStateText(dateText)}</span>`;
            }

            return series;
          }

          setChartPlaceholder(
            "rates-policy-chart",
            "No Duration / Term Premium data available.",
          );
          setRatesPanelMeta({
            title: "Duration / Term Premium",
            state: "--",
            date: "--",
            source:
              payload?.source ||
              payload?.method ||
              "Source: the_Spine | Duration / Term Premium",
          });
          return [];
        });
      }

      return renderRatesSeriesToChart({
        chartId: "rates-policy-chart",
        rows: context.countryRows,
        fields: [
          "term_premium",
          "term_premium_10y",
          "duration_premium",
          "acm_term_premium_10y",
          "duration_term_premium",
        ],
        emptyMessage: "Duration / Term Premium endpoint pending.",
        stateId: "rates-policy-state",
        title: "Duration / Term Premium",
        source: "Source: the_Spine | Duration / Term Premium",
      });
    }

    function renderRatesGlobalDispersionMetric(context) {
      if (context.selectedCountry === "US") {
        return fetchJsonWithBust(DATA_ENDPOINTS.ratesGlobalRatesDispersion).then((payload) => {
          const rows = Array.isArray(payload?.chart?.rows)
            ? payload.chart.rows
            : [];
          const series = filterRatesSeriesByHorizon(
            rows
              .map((row) => ({
                date: row.date || row.as_of_date,
                value: getNumericField(row, ["dispersion"]),
              }))
              .filter((row) => row.date && Number.isFinite(row.value)),
            context.horizon,
          );

          if (series.length) {
            const chart = document.getElementById("rates-policy-chart");
            renderSpreadChart(chart, series);

            const latestSeries = series[series.length - 1];
            const latestDispersion = Number(payload?.latest?.dispersion);
            const latest = {
              date: payload?.as_of || latestSeries.date,
              value: Number.isFinite(latestDispersion)
                ? latestDispersion
                : latestSeries.value,
              state: payload?.latest?.state || "--",
            };
            const dateText = formatRatesLanguageDate(latest.date);
            const countryRows = Array.isArray(payload?.latest?.countries)
              ? payload.latest.countries
              : Array.isArray(payload?.countries)
                ? payload.countries
                : [];
            const countryCodes = countryRows.map((row) => row.country).filter(Boolean);
            const countryValues = countryRows
              .map((row) => {
                const y10 = Number(row.y10);
                return `${escapeRatesStateText(row.country)} ${Number.isFinite(y10) ? formatNumber(y10, 2) : "--"}`;
              })
              .join(" &bull; ");
            const method =
              payload?.method ||
              "Cross-country standard deviation of sovereign 10Y yields.";

            if (chart && countryValues) {
              chart.insertAdjacentHTML(
                "beforeend",
                `<div style="margin-top:8px;color:#8f9aac;font-size:.78rem;text-align:center;">
                  <div>Latest ${formatNumber(latest.value, 2)}</div>
                  <div>Countries ${countryCodes.map(escapeRatesStateText).join(" &bull; ")}</div>
                  <div>${countryValues}</div>
                </div>`,
              );
            }

            setRatesPanelMeta({
              title: "Global Rates Dispersion",
              state: latest.state,
              date: dateText,
              source: method,
            });

            const sourceEl = document.getElementById("rates-policy-source");
            if (sourceEl) {
              const countryText = countryCodes.length
                ? `Countries: ${countryCodes.map(escapeRatesStateText).join(" &bull; ")} | `
                : "";
              sourceEl.innerHTML = `${countryText}${escapeRatesStateText(method)} | As of <span id="rates-policy-date">${escapeRatesStateText(dateText)}</span>`;
            }

            return series;
          }

          setChartPlaceholder(
            "rates-policy-chart",
            "No Global Rates Dispersion data available.",
          );
          setRatesPanelMeta({
            title: "Global Rates Dispersion",
            state: payload?.latest?.state || "--",
            date: payload?.as_of ? formatRatesLanguageDate(payload.as_of) : "--",
            source:
              payload?.method ||
              "Cross-country standard deviation of sovereign 10Y yields.",
          });
          return [];
        });
      }

      setChartPlaceholder(
        "rates-policy-chart",
        "Global Rates Dispersion is available for USA RATES only.",
      );
      setRatesPanelMeta({
        title: "Global Rates Dispersion",
        state: "--",
        date: "--",
        source: "Cross-country standard deviation of sovereign 10Y yields.",
      });
      return [];
    }

    async function renderRatesSelectedMetric(context) {
      const panel = document.querySelector("#view-rates .rates-policy-panel");
      panel?.classList.remove("rates-metric-blank");

      switch (context.selectedMetric) {
        case "yield-family":
          return await renderRatesYieldFamilyMetric(context);

        case "curve-structure":
          return renderRatesCurveStructureTopRightMetric(context);

        case "policy-restriction":
          return await renderRatesPolicyRestrictionMetric(context);

        case "real-yield-pressure":
          return await renderRatesRealYieldPressureMetric(context);

        case "duration-term-premium":
          return await renderRatesDurationTermPremiumMetric(context);

        case "global-rates-dispersion":
          return await renderRatesGlobalDispersionMetric(context);

        default:
          return renderRatesMetricTargetBlank();
      }
    }

    async function getRatesLanguageLatestDate(country, fallbackRows = []) {
      const fallbackDate =
        fallbackRows[fallbackRows.length - 1]?.date ||
        fallbackRows[fallbackRows.length - 1]?.as_of_date ||
        "--";

      const commentaryUrl = DATA_ENDPOINTS.ratesCentralBankCommentary;
      if (!commentaryUrl) return fallbackDate;

      try {
        const payload = await fetchJsonWithBust(commentaryUrl);
        const latest = payload?.latest || {};
        if (latest.date || latest.as_of_date) {
          return latest.date || latest.as_of_date;
        }

        const rows = Array.isArray(payload)
          ? payload
          : Array.isArray(payload?.rows)
            ? payload.rows
            : [];
        const countryRows = rows
          .filter((row) => {
            const rowCountry = String(
              row.country || row.country_code || row.market || "",
            ).toUpperCase();
            return rowCountry === String(country || "").toUpperCase();
          })
          .sort((a, b) =>
            String(a.date || a.as_of_date || "").localeCompare(
              String(b.date || b.as_of_date || ""),
            ),
          );

        return (
          countryRows[countryRows.length - 1]?.date ||
          countryRows[countryRows.length - 1]?.as_of_date ||
          fallbackDate
        );
      } catch (err) {
        console.warn("Central bank commentary date unavailable.", err);
        return fallbackDate;
      }
    }

    let ratesRenderFrame = null;

    function scheduleRatesRender() {
      if (!document.body.classList.contains("view-rates")) return;
      if (ratesRenderFrame != null) {
        cancelAnimationFrame(ratesRenderFrame);
      }

      ratesRenderFrame = requestAnimationFrame(() => {
        ratesRenderFrame = null;
        renderRates();
      });
    }

    const WTI_PANEL_URL =
      "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/wti/wti_panel.json";

    let finstateUniverseData = [];
    let finstateGlobalLiteMetricsData = [];

    const DATA_ENDPOINTS = {
      price:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_price_data.json",
      spreads:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_spreads_data.json",
      sigma:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_sigma_data.json",
      universe:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/fx_universe.json",

      equitiesIndex:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/us_equity_index_data.json",
      equitiesSectorEtf:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/us_sector_etf_data.json",
      equitiesBreadth:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/etf_pmi_breadth_by_etf.json",
      equitiesSigma:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/equities_sigma_rank.json",
      equitiesIndustryPanel:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/industry_panel_serving.json",
      finstateSigma:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/finstate/finstate_sigma_rank.json",
      finstateUniverse:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/finstate/finstate_universe_metrics_v1.json",
      finstateGlobalLiteMetrics:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/finstate/finstate_global_lite_metrics_v1.json",
      wtiInventoryOcOverlay:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/wti/wti_inventory_oc_overlay.json",
      wtiPrice:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/wti/wti_price_data.json",

      usdcadWtiInventoryCycle:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/fx/usdcad_wti_inventory_cycle.json",

      ratesPanel:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_selected_global_panel.json",
      ratesCurve:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_curve_data.json",
      ratesSpreads:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_spread_data.json",
      ratesPolicy:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_policy_pressure_data.json",
      ratesCentralBankCommentary: {
        US: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/us_policy_language_latest.json",
        EU: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/eu_policy_language_latest.json",
        UK: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/gb_policy_language_latest.json",
        JP: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/jp_policy_language_latest.json",
        CA: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/ca_policy_language_latest.json",
        AU: "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/au_policy_language_latest.json",
      },
      ratesSigma:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_sigma_rank.json",

      ratesYieldFamilyChart:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_yield_family_chart.json",

      ratesGeoScenBridge:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_geoscen_bridge.json",

      ratesPolicyRestrictionChart:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_policy_restriction_chart.json",

      ratesRealYieldPressureChart:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_real_yield_pressure_chart.json",

      ratesDurationTermPremium:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_duration_term_premium.json",

      ratesGlobalRatesDispersion:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_global_rates_dispersion.json",

      ratesIVVector:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_iv_vector.json",

      ratesStateEngine:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_state_engine.json",

      ratesRegimeEngine:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/rates/rates_regime_engine.json",

      fxDepth:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/fx/fx_depth_serving_v1.json",

      globalEquityRegionPanel:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/global_equity_region_panel.json",

      globalEquityRegionLatest:
        "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/equities/global_equity_region_latest.json",

      cflow: {
        "weekly-economic-index":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/weekly_economic_index_serving.json",

        "core-pce":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/core_pce_serving.json",

        "industrial-production":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/industrial_production_serving.json",

        "building-permits":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/building_permits_serving.json",

        "jolts-openings":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/jolts_openings_serving.json",

        "initial-jobless-claims":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/initial_jobless_claims_serving.json",

        "weekly-hours-worked":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/weekly_hours_worked_serving.json",

        "capacity-utilization":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/capacity_utilization_serving.json",

        "retail-sales":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/retail_sales_serving.json",

        "consumer-sentiment":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/consumer_sentiment_serving.json",

        "core-cpi":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/core_cpi_serving.json",

        "ppi-finished-goods":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/ppi_finished_goods_serving.json",

        "real-personal-income":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/real_personal_income_serving.json",

        "cflow-state-engine":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cflow_state_engine_serving.json",

        "cflow-state-history":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cflow_state_history_serving.json",

        "cflow-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cflow_composite_serving.json",

        "container-shipping-index":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/container_shipping_index_serving.json",

        "cass-freight-shipments":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cass_freight_shipments_serving.json",

        "freight-transportation-services":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/freight_transportation_services_serving.json",

        "rail-freight-carloads":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/rail_freight_carloads_serving.json",

        "rail-freight-intermodal":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/rail_freight_intermodal_serving.json",
          
        "labor-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/labor_composite_serving.json",

        "inflation-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/inflation_composite_serving.json",

        "diesel-demand":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/diesel_demand_serving.json",

        "distillate-inventories":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/distillate_inventories_serving.json",

        "energy-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/energy_composite_serving.json",

        "funding-stress-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/funding_stress_composite_serving.json",

        "sofr-funding":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/sofr_funding_stress_serving.json",

        "hy-oas":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/high_yield_oas_serving.json",

        "credit-transmission-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/credit_transmission_composite_serving.json",

        "baltic-dry-index":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/baltic_dry_proxy_serving.json",

        "cot-positioning":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cot_positioning_serving.json",

        "liquidity-constraint-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/liquidity_constraint_composite_serving.json",

        "financial-transmission-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/financial_transmission_composite_serving.json",

        "cflow-iv-vector-contribution":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cflow_iv_vector_contribution_serving.json",

        "transport-transmission-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/transport_transmission_composite_serving.json",

        "econ-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/econ_composite_serving.json",

        "capital-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/capital_composite_serving.json",

        "fragility-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/fragility_composite_serving.json",

        "dispersion-composite":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/dispersion_composite_serving.json",

        "cflow-regime-engine":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cflow_regime_engine_serving.json",

        "cflow-regime-definitions":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cflow_regime_definitions_serving.json",

        "cflow-completion-ledger":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/cflow/cflow_completion_ledger_serving.json",

        },

      oraclechambers: {
        "registry":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/oraclechambers/oracle_chambers_registry.json",

        "cflow-chamber":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/oraclechambers/cflow_chamber_serving.json",

        "rates-chamber":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/oraclechambers/rates_chamber_serving.json",

        "equities-chamber":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/oraclechambers/equities_chamber_serving.json",

        "fx-chamber":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/oraclechambers/fx_chamber_serving.json",

        "crossasset-chamber":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/oraclechambers/crossasset_chamber_serving.json",

        "oracle-router":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/oraclechambers/oracle_router_serving.json",

        "oracle-completion-ledger":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/oraclechambers/oracle_completion_ledger_serving.json",
      },

      geoscen: {

        "state-engine":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_state_engine_serving.json",

        "state-history":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_state_history_serving.json",

        "regime-engine":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_regime_engine_serving.json",

        "completion-ledger":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_completion_ledger_serving.json",

        "routing-engine":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_routing_engine_serving.json",

        "iv-contribution":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_iv_contribution_serving.json",

        "shipping-routes":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_shipping_routes_serving.json",

        "energy-geography":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_energy_geography_serving.json",

        "critical-minerals":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_critical_minerals_serving.json",

        "chokepoints":
          "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev/spine_us/serving/geoscen/geoscen_chokepoints_serving.json"

      },


    };


    const EQUITIES_MARKET_INDEXES = ["SPY", "QQQ", "DIA", "ITOT", "MDY", "IWM"];
    const EQUITIES_SECTOR_ETFS = [
      "XLB",
      "XLC",
      "XLE",
      "XLF",
      "XLI",
      "XLK",
      "XLP",
      "XLRE",
      "XLU",
      "XLV",
      "XLY",
    ];
    const EQUITIES_SECTOR_INDUSTRY_MAP = {
      XLB: [
        "Chemical Products",
        "Primary Metals",
        "Paper Products",
        "Plastics & Rubber Products",
        "Nonmetallic Mineral Products",
        "Wood Products",
        "Agriculture, Forestry, Fishing & Hunting",
        "Construction",
      ],

      XLC: [
        "Information",
        "Arts, Entertainment & Recreation",
        "Printing & Related Support Activities",
      ],

      XLE: ["Petroleum & Coal Products", "Mining", "Utilities"],

      XLF: ["Finance & Insurance", "Management of Companies & Support Services"],

      XLI: [
        "Machinery",
        "Transportation Equipment",
        "Fabricated Metal Products",
        "Electrical Equipment, Appliances & Components",
        "Miscellaneous Manufacturing",
        "Transportation & Warehousing",
        "Wholesale Trade",
        "Construction",
      ],

      XLK: [
        "Computer & Electronic Products",
        "Professional, Scientific & Technical Services",
        "Information",
      ],

      XLP: ["Food, Beverage & Tobacco Products", "Paper Products"],

      XLRE: ["Real Estate, Rental & Leasing"],

      XLU: ["Utilities"],

      XLV: ["Health Care & Social Assistance"],

      XLY: [
        "Retail Trade",
        "Accommodation & Food Services",
        "Apparel, Leather & Allied Products",
        "Furniture & Related Products",
        "Textile Mills",
        "Other Services",
      ],
    };

    const MODULE_QUESTIONS = {
      macro: "What Condition Exists Right Now?",
      geoscen: "Why Is It Happening Here?",
      rates: "Why Is Capital Constrained?",
      fx: "Capital Prefers What?",
      finstate: "What Is Graham Seeing?",
      equities: "Who Is Leading?",
      cflow: "Activity Propagates Where?",
    };

    function filterIndustryRowsForEtf(rows, etfFocus) {
      const allowed =
        EQUITIES_SECTOR_INDUSTRY_MAP[String(etfFocus || "").toUpperCase()] || [];

      return rows.filter((r) => {
        const directEtf = String(r.etf || "").toUpperCase();
        if (directEtf && directEtf === String(etfFocus || "").toUpperCase())
          return true;

        const industry = String(r.industry || "");
        return allowed.some((x) => industry.includes(x));
      });
    }

    const EQUITIES_LENS_CONFIG = {
      SPY: {
        universe: "Market Indexes",
        lens_primary: "Broad Market",
        lens_secondary: "Blend",
        growth_defensive: "Balanced",
        cyclical_defensive: "Balanced",
        size_profile: "Large Cap",
        notes: ["Core U.S. beta", "Broad macro read", "Blended exposure"],
      },
      QQQ: {
        universe: "Market Indexes",
        lens_primary: "Growth",
        lens_secondary: "Innovation",
        growth_defensive: "Growth",
        cyclical_defensive: "Cyclical",
        size_profile: "Large Cap",
        notes: ["Tech concentration", "Low defensive mix", "Duration-sensitive"],
      },
      DIA: {
        universe: "Market Indexes",
        lens_primary: "Legacy Quality",
        lens_secondary: "Industrial / Value Tilt",
        growth_defensive: "Balanced",
        cyclical_defensive: "Balanced",
        size_profile: "Large Cap",
        notes: [
          "Old-economy tilt",
          "Quality cyclicals",
          "Industrial sensitivity",
        ],
      },
      ITOT: {
        universe: "Market Indexes",
        lens_primary: "Total Market",
        lens_secondary: "Blend",
        growth_defensive: "Balanced",
        cyclical_defensive: "Balanced",
        size_profile: "All Cap",
        notes: ["Full U.S. market", "Benchmark baseline", "Broad participation"],
      },
      MDY: {
        universe: "Market Indexes",
        lens_primary: "Mid Cap",
        lens_secondary: "Domestic Cyclicality",
        growth_defensive: "Balanced",
        cyclical_defensive: "Cyclical",
        size_profile: "Mid Cap",
        notes: ["Economic sensitivity", "Mid-cap breadth", "Domestic exposure"],
      },
      IWM: {
        universe: "Market Indexes",
        lens_primary: "Small Cap",
        lens_secondary: "Domestic Sensitivity",
        growth_defensive: "Balanced",
        cyclical_defensive: "Cyclical",
        size_profile: "Small Cap",
        notes: [
          "Higher beta",
          "Domestic cyclicality",
          "Financial conditions sensitive",
        ],
      },
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
      if (payload && typeof payload === "object" && Array.isArray(payload.rows))
        return payload.rows;
      if (Array.isArray(payload)) return payload;
      return [];
    }

    const DEFAULT_FX_UNIVERSE = [
      "EURUSD",
      "AUDUSD",
      "GBPUSD",
      "USDCAD",
      "USDCHF",
      "USDJPY",
    ];

    const FX_LOCKED_METRICS = [
      "Energy Tax",
      "Bank Ratio",
      "Econ Surprise",
      "Eurozone Stress",
    ];

    const FX_DEPTH_CONFIG = {
      "EUR/USD": {
        leftLabel: "DE-US 2Y",
        midLabel: "Energy Tax",
        rightLabel: "Bank Ratio",
        source: "Source: the_Spine | EUR/USD Depth",
        rows: [],
      },

      "AUD/USD": {
        leftLabel: "Iron Ore",
        midLabel: "AU-US 2Y",
        rightLabel: "Copper/Gold",
        source: "Source: the_Spine | AUD/USD Depth",
        rows: [],
      },

      "GBP/USD": {
        leftLabel: "UK-US 2Y",
        midLabel: "FTSE vs. SPX",
        rightLabel: "Econ Surprise",
        source: "Source: the_Spine | GBP/USD Depth",
        rows: [],
      },

      "USD/CAD": {
        leftLabel: "WTI Inv.",
        midLabel: "US-CA 2Y",
        rightLabel: "WTI vs. NatGas",
        source: "Source: the_Spine | USD/CAD Depth",
        rows: [],
      },

      "USD/CHF": {
        leftLabel: "XAU/EUR",
        midLabel: "VIX",
        rightLabel: "Eurozone Stress",
        source: "Source: the_Spine | USD/CHF Depth",
        rows: [],
      },

      "USD/JPY": {
        leftLabel: "US 2Y",
        midLabel: "Brent Crude",
        rightLabel: "BCOM vs. Nikkei",
        source: "Source: the_Spine | USD/JPY Depth",
        rows: [],
      },
    };

    const SYMBOL_TO_DISPLAY_PAIR = {
      AUDUSD: "AUD/USD",
      EURUSD: "EUR/USD",
      GBPUSD: "GBP/USD",
      USDCAD: "USD/CAD",
      USDCHF: "USD/CHF",
      USDJPY: "USD/JPY",
    };

    const DISPLAY_PAIR_TO_SYMBOL = Object.fromEntries(
      Object.entries(SYMBOL_TO_DISPLAY_PAIR).map(([symbol, display]) => [
        display,
        symbol,
      ]),
    );

    const ACTIVE_REFRESH_MS = 60 * 1000;

    const activeDataStore = {
      price: {},
      spreads: {},
      sigma: [],
      universe: [],
    };

    let fxDepthData = {};

    const endpointHealth = {
      price: false,
      spreads: false,
      sigma: false,
      universe: false,
    };

    let activeDataLoaded = false;
    let activeDataLoadError = null;
    let activeRefreshHandle = null;

    const HORIZON_LENGTH = {
      "5D": 5,
      "15D": 15,
      "30D": 30,
      "45D": 45,
    };

    const FX_INDEXED_METRICS = new Set([
      "Iron Ore",
      "VIX",
      "XAU/EUR",
      "Copper/Gold",
      "WTI vs. NatGas",
      "Brent Crude",
      "BCOM vs. Nikkei",
    ]);

    const FX_RAW_METRICS = new Set([
      "DE-US 2Y",
      "US-CA 2Y",
      "US 2Y",
      "UK-US 2Y",
      "AU-US 2Y",
      "WTI Inv.",
    ]);

    function shouldIndexFXDepthMetric(metricName) {
      return FX_INDEXED_METRICS.has(metricName);
    }

    function getFXDepthHorizonRows(rows, metricName = "") {
      const horizon = fxControls.horizon?.value || "30D";
      const baseCount = HORIZON_LENGTH[horizon] || 30;
      const count = shouldIndexFXDepthMetric(metricName)
        ? baseCount + 15
        : baseCount;

      return Array.isArray(rows) ? rows.slice(-count) : [];
    }

    function indexFXDepthRows(rows) {
      if (!Array.isArray(rows) || !rows.length) return [];

      const base = Number(rows[0]?.value);
      if (!Number.isFinite(base) || base === 0) return rows;

      return rows.map((row, idx) => {
        const value = Number(row.value);
        const indexedValue = Number.isFinite(value) ? (value / base) * 100 : null;

        const prior = idx > 0 ? Number(rows[idx - 1]?.value) : value;
        const priorIndexed =
          Number.isFinite(prior) && base !== 0
            ? (prior / base) * 100
            : indexedValue;

        return {
          ...row,
          rawValue: value,
          value: indexedValue,
          change: indexedValue - priorIndexed,
        };
      });
    }

    const SPREAD_HORIZON_LENGTH = {
      "5D": 12,
      "15D": 18,
      "30D": 24,
      "45D": 36,
    };

    async function fetchJsonWithBust(url) {
      const bustedUrl = `${url}${url.includes("?") ? "&" : "?"}v=${Date.now()}`;

      try {
        const response = await fetch(bustedUrl, {
          method: "GET",
          cache: "no-store",
        });

        if (!response.ok) throw new Error(`Fetch failed: ${response.status}`);
        return await response.json();
      } catch (err) {
        console.error("FETCH ERROR:", bustedUrl, err);
        return {};
      }
    }

    function normalizeUniverse(values) {
      if (!Array.isArray(values)) return [];
      return values.map((x) => String(x).toUpperCase().trim()).filter(Boolean);
    }

    function normalizePricePayload(payload) {
      return payload && typeof payload === "object" && !Array.isArray(payload)
        ? payload
        : {};
    }

    function normalizeSpreadsPayload(payload) {
      return payload && typeof payload === "object" && !Array.isArray(payload)
        ? payload
        : {};
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

      const notes = (cfg.notes || []).map((note) => `<li>${note}</li>`).join("");

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

    function getEquitiesEtfUniverse() {
      return [
        "XLB",
        "XLC",
        "XLE",
        "XLF",
        "XLI",
        "XLK",
        "XLP",
        "XLRE",
        "XLU",
        "XLV",
        "XLY",
      ];
    }

    function getEquitiesComparisonUniverse(selectedSymbol) {
      return isSectorEtf(selectedSymbol)
        ? getEquitiesEtfUniverse()
        : getEquitiesIndexUniverse();
    }

    async function loadEquitiesData() {
      const results = await Promise.allSettled([
        fetchJsonWithBust(DATA_ENDPOINTS.equitiesIndex),
        fetchJsonWithBust(DATA_ENDPOINTS.equitiesSectorEtf),
        fetchJsonWithBust(DATA_ENDPOINTS.equitiesIndustryPanel),
        fetchJsonWithBust(DATA_ENDPOINTS.equitiesBreadth),
        fetchJsonWithBust(DATA_ENDPOINTS.equitiesSigma),
      ]);

      const [indexRes, sectorEtfRes, panelRes, breadthRes, sigmaRes] = results;

      const indexOk = indexRes.status === "fulfilled";
      const sectorEtfOk = sectorEtfRes.status === "fulfilled";
      const panelOk = panelRes.status === "fulfilled";
      const breadthOk = breadthRes.status === "fulfilled";
      const sigmaOk = sigmaRes.status === "fulfilled";

      return {
        index: indexOk ? normalizeEquitiesIndexPayload(indexRes.value) : {},
        sectorEtf: sectorEtfOk
          ? normalizeEquitiesIndexPayload(sectorEtfRes.value)
          : {},
        industryPanel: panelOk
          ? normalizeIndustryPanelPayload(panelRes.value)
          : [],
        breadth: breadthOk
          ? normalizeEquitiesPmiSeriesPayload(breadthRes.value)
          : [],
        sigma: sigmaOk ? normalizeEquitiesSigmaPayload(sigmaRes.value) : [],
        health: {
          index: indexOk,
          sectorEtf: sectorEtfOk,
          industryPanel: panelOk,
          breadth: breadthOk,
          sigma: sigmaOk,
        },
      };
    }

    async function loadWtiPanel() {
      return fetchJsonWithBust(WTI_PANEL_URL);
    }

    async function loadWtiInventoryOcOverlay() {
      return fetchJsonWithBust(DATA_ENDPOINTS.wtiInventoryOcOverlay);
    }

    async function loadWtiPriceData() {
      const response = await fetchJsonWithBust(DATA_ENDPOINTS.wtiPrice);

      if (Array.isArray(response)) {
        return {
          rows: response,
          meta: {},
        };
      }

      if (
        response &&
        typeof response === "object" &&
        Array.isArray(response.series)
      ) {
        return {
          rows: response.series,
          meta: response.meta || {},
        };
      }

      return {
        rows: [],
        meta: {},
      };
    }

    function normalizeEquitiesIndexPayload(payload) {
      if (Array.isArray(payload)) return payload;
      return payload && typeof payload === "object" ? payload : {};
    }

    function normalizeEquitiesPmiSeriesPayload(payload) {
      return Array.isArray(payload) ? payload : [];
    }

    function formatInt(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "--";
      return String(Math.round(num));
    }

    function formatSignedInt(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "--";
      const rounded = Math.round(num);
      return `${rounded >= 0 ? "+" : ""}${rounded}`;
    }

    function formatNumber(value, digits = 2) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "--";
      return num.toFixed(digits);
    }

    function formatFXDepthAbsolute(value, digits = 2) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "--";

      return num.toLocaleString("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
    }

    function getFXDepthIndexChange(rows) {
      if (!Array.isArray(rows) || !rows.length) return null;

      const indexedRows = indexFXDepthRows(rows);
      const latest = indexedRows[indexedRows.length - 1];
      const latestIndex = Number(latest?.value);

      if (!Number.isFinite(latestIndex)) return null;

      return latestIndex - 100;
    }

    function coerceEquitiesIndexSeries(payload, selectedSymbol = "SPY") {
      const comparisonUniverse = getEquitiesComparisonUniverse(selectedSymbol);
      const allowed = new Set(comparisonUniverse);
      const grouped = {};

      comparisonUniverse.forEach((symbol) => {
        grouped[symbol] = [];
      });

      if (!payload) return grouped;

      if (Array.isArray(payload)) {
        payload.forEach((row) => {
          const symbol = String(
            row.symbol ?? row.ticker ?? row.etf ?? row.index ?? "",
          )
            .toUpperCase()
            .trim();

          if (!allowed.has(symbol)) return;

          const date = String(
            row.date ?? row.as_of_date ?? row.timestamp ?? "",
          ).trim();
          const close = Number(
            row.close ?? row.price ?? row.last ?? row.value ?? row.adjClose,
          );

          if (!date || !Number.isFinite(close)) return;

          grouped[symbol].push({ date, close });
        });

        comparisonUniverse.forEach((symbol) => {
          grouped[symbol] = grouped[symbol].sort((a, b) =>
            a.date.localeCompare(b.date),
          );
        });

        return grouped;
      }

      if (typeof payload === "object") {
        comparisonUniverse.forEach((symbol) => {
          const rows = payload[symbol];

          if (!Array.isArray(rows) || !rows.length) {
            grouped[symbol] = [];
            return;
          }

          grouped[symbol] = rows
            .map((row) => ({
              date: String(
                row.date ?? row.as_of_date ?? row.timestamp ?? "",
              ).trim(),
              close: Number(
                row.close ?? row.price ?? row.last ?? row.value ?? row.adjClose,
              ),
            }))
            .filter((r) => r.date && Number.isFinite(r.close))
            .sort((a, b) => a.date.localeCompare(b.date));
        });

        return grouped;
      }

      return grouped;
    }

    function renderIndustryCards(rows) {
      if (!rows || !rows.length) return "";

      return `
      <div class="equities-industry-cards">
        ${rows
          .map(
            (row) => `
          <div class="industry-card">
            
            <div class="industry-card-title">
              ${row.industry}
            </div>

            <div class="industry-row">
              <span class="industry-label">PMI</span>
              <span>${formatInt(row.pmi_Idx)}</span>
              <span>${formatSignedInt(row["pmi_3M_Î”"])}</span>
              <span>${formatSignedInt(row["pmi_6M_Î”"])}</span>
            </div>

            <div class="industry-row">
              <span class="industry-label">Orders</span>
              <span>${formatInt(row.no_Idx)}</span>
              <span>${formatSignedInt(row["no_3M_Î”"])}</span>
              <span>${formatSignedInt(row["no_6M_Î”"])}</span>
            </div>

            <div class="industry-footer">
              <span>Sig: ${formatSignedInt(row.Sig)}</span>
              <span>Wt: ${formatNumber(row.Wt, 1)}</span>
            </div>

          </div>
        `,
          )
          .join("")}
      </div>
    `;
    }

    function getMonthlyIdxExtremes(rows, targetDate, bucket) {
      const scoped = rows.filter(
        (r) =>
          r.date === targetDate &&
          String(r.pmi_type || "").toLowerCase() === bucket,
      );

      const pmiVals = scoped
        .map((r) => Number(r.pmi_Idx))
        .filter(Number.isFinite);

      const noVals = scoped.map((r) => Number(r.no_Idx)).filter(Number.isFinite);

      return {
        pmiMax: pmiVals.length ? Math.max(...pmiVals) : null,
        pmiMin: pmiVals.length ? Math.min(...pmiVals) : null,
        noMax: noVals.length ? Math.max(...noVals) : null,
        noMin: noVals.length ? Math.min(...noVals) : null,
      };
    }

    async function renderCflowCompletionLedger() {
      const url =
        DATA_ENDPOINTS.cflow["cflow-completion-ledger"];

      try {
        const payload = await fetchJsonWithBust(url);

        const latest = payload.latest || {};
        const components = payload.components || [];

        const title = document.querySelector(
          ".cflow-quality-panel .panel-title"
        );

        const body = document.querySelector(
          ".cflow-quality-panel .panel-placeholder"
        );

        if (title) {
          title.textContent =
            "C?FLOW COMPLETION LEDGER";
        }

        setCFlowLatestHeader(
          latest.completion_pct == null ? "--" : `${latest.completion_pct}%`,
        );

        if (body) {
          body.innerHTML = `
            <div class="cflow-live-card">

              <div style="font-size:2rem;font-weight:700;">
                ${latest.completion_pct ?? "--"}%
              </div>

              <div style="margin-top:8px;">
                ${latest.complete_components ?? "--"}
                /
                ${latest.total_components ?? "--"}
              </div>

              <div style="margin-top:16px;text-align:left;">
                ${components
                  .map(
                    c => `
                      <div>
                        ${c.status === "complete" ? "?" : "?"}
                        ${c.name}
                      </div>
                    `
                  )
                  .join("")}
              </div>

            </div>
          `;
        }
      } catch (err) {
        console.error(err);
      }
    }

    function renderIndustryPanelTable(container, rows, etfFocus) {
      if (!container) return;

      const etfRows = filterIndustryRowsForEtf(rows, etfFocus);

      if (!etfRows.length) {
        container.innerHTML = `<div class="panel-placeholder">No industry panel data available for ${etfFocus}.</div>`;
        return;
      }

      const latestDate = etfRows
        .map((r) => r.date)
        .filter(Boolean)
        .sort()
        .slice(-1)[0];

      const latestRows = etfRows.filter((r) => r.date === latestDate);

      const isMobile = window.innerWidth <= 768;

      if (isMobile) {
        container.innerHTML = renderIndustryCards(latestRows);
        return;
      }

      const sections = ["manu", "serv"]
        .map((bucket) => {
          const bucketRows = latestRows
            .filter((r) => String(r.pmi_type || "").toLowerCase() === bucket)
            .sort(
              (a, b) =>
                Math.abs(Number(b.Sig || 0)) - Math.abs(Number(a.Sig || 0)),
            );

          if (!bucketRows.length) return "";

          const label = bucket === "manu" ? "Manufacturing" : "Services";
          const monthlyExtremes = getMonthlyIdxExtremes(rows, latestDate, bucket);

          const body = bucketRows
            .map(
              (row) => `
        <tr>
          <td>${row.industry}</td>
          <td class="num pmi-col">${formatInt(row.pmi_Idx)}</td>
          <td class="num pmi-col">${formatSignedInt(row["pmi_3M_\u0394"])}</td>
          <td class="num pmi-col">${formatSignedInt(row["pmi_6M_\u0394"])}</td>

          <td class="num group-sep-left orders-col">${formatInt(row.no_Idx)}</td>
          <td class="num orders-col">${formatSignedInt(row["no_3M_\u0394"])}</td>
          <td class="num orders-col">${formatSignedInt(row["no_6M_\u0394"])}</td>

          <td class="num group-sep-left">${formatSignedInt(row.Sig)}</td>
          <td class="num">${Number.isFinite(Number(row.Wt)) ? Number(row.Wt).toFixed(1) : "--"}</td>
        </tr>
        `,
            )
            .join("");

          const summaryRows = `
        <tr class="equities-monthly-extrema-row">
          <td class="equities-monthly-extrema-label">Mnthly MAX</td>
          <td class="num pmi-col">${formatInt(monthlyExtremes.pmiMax)}</td>
          <td></td>
          <td></td>

          <td class="num group-sep-left orders-col">${formatInt(monthlyExtremes.noMax)}</td>
          <td></td>
          <td></td>

          <td class="group-sep-left"></td>
          <td></td>
        </tr>
        <tr class="equities-monthly-extrema-row">
          <td class="equities-monthly-extrema-label">Mnthly Min</td>
          <td class="num pmi-col">${formatInt(monthlyExtremes.pmiMin)}</td>
          <td></td>
          <td></td>

          <td class="num group-sep-left orders-col">${formatInt(monthlyExtremes.noMin)}</td>
          <td></td>
          <td></td>

          <td class="group-sep-left"></td>
          <td></td>
        </tr>
        `;

          return `
          <div class="equities-industry-section">
            <div class="equities-industry-section-title">${label}</div>
            <table class="equities-industry-table equities-industry-table-combined">
              <colgroup>
                <col class="col-industry">
                <col class="col-num">
                <col class="col-num">
                <col class="col-num">
                <col class="col-num">
                <col class="col-num">
                <col class="col-num">
                <col class="col-num">
                <col class="col-num">
              </colgroup>
              <thead>
                <tr>
                  <th rowspan="2" class="center-text">Industry</th>
                  <th colspan="3" class="group-head pmi-col center-text">PMI</th>
                  <th colspan="3" class="group-head group-sep-left orders-col center-text">New Orders</th>
                  <th colspan="2" class="group-head group-sep-left center-text"></th>
                </tr>
                <tr>
                  <th class="center-text pmi-col">Idx</th>
                  <th class="center-text pmi-col">3M Î”</th>
                  <th class="center-text pmi-col">6M Î”</th>

                  <th class="group-sep-left center-text orders-col">Idx</th>
                  <th class="center-text orders-col">3M Î”</th>
                  <th class="center-text orders-col">6M Î”</th>

                  <th class="group-sep-left center-text">Sig</th>
                  <th class="center-text">Wt</th>
                </tr>
              </thead>
              <tbody>
                ${body}
                ${summaryRows}
              </tbody>
            </table>
          </div>
        `;
        })
        .join("");

      container.innerHTML = `<div class="equities-industry-table-wrap">${sections}</div>`;
    }

  function updateCFlowDropdowns({ resetSubsystem = false, resetMetric = false } = {}) {
    const domainKey = cflowControls.domain?.value || "physical";
    const domain = CFLOW_MENU[domainKey] || CFLOW_MENU.physical;

    if (!cflowControls.subsystem || !cflowControls.metric) return;

    const subsystemKeys = Object.keys(domain.subsystems);
    const previousSubsystem = cflowControls.subsystem.value;

    cflowControls.subsystem.innerHTML = subsystemKeys
      .map((key) => `<option value="${key}">${domain.subsystems[key].label}</option>`)
      .join("");

    const activeSubsystem =
      !resetSubsystem && subsystemKeys.includes(previousSubsystem)
        ? previousSubsystem
        : subsystemKeys[0];

    cflowControls.subsystem.value = activeSubsystem;

    const subsystem = domain.subsystems[activeSubsystem];
    const previousMetric = cflowControls.metric.value;

    cflowControls.metric.innerHTML = subsystem.metrics
      .map(
        (metric) => `
          <option value="${metric.value}" ${metric.disabled ? "disabled" : ""}>
            ${metric.label}
          </option>
        `,
      )
      .join("");

    const metricValues = subsystem.metrics
      .filter((metric) => !metric.disabled)
      .map((metric) => metric.value);

    cflowControls.metric.value =
      !resetMetric && metricValues.includes(previousMetric)
        ? previousMetric
        : metricValues[0];
  }


    function renderEquitiesIndexPlaceholder(containerId, title) {
      const node = document.getElementById(containerId);
      if (!node) return;
      node.innerHTML = `<div class="panel-placeholder">${title}</div>`;
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
            state: row.state ?? row.regime ?? row.composite_state ?? "Live",
          };
        })
        .filter((row) => row.date && Number.isFinite(row.composite))
        .sort((a, b) => String(a.date).localeCompare(String(b.date)));
    }

    function coerceEquitiesSigmaRows(rows) {
      if (!Array.isArray(rows)) return [];

      return rows
        .map((row) => ({
          symbol: String(row.symbol ?? row.pair ?? "")
            .toUpperCase()
            .trim(),
          pair: String(row.symbol ?? row.pair ?? "")
            .toUpperCase()
            .trim(),
          z: Number(row.z ?? row.sigma_value ?? row.value),
          rank: Number(row.rank),
          pct: Number(row.pct),
          state: row.state ?? "--",
          as_of_date: row.as_of_date ?? row.date ?? null,
        }))
        .filter(
          (row) =>
            row.symbol && Number.isFinite(row.z) && Number.isFinite(row.rank),
        )
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

    function renderEquitiesIndexChart(
      container,
      groupedSeries,
      horizon,
      selectedSymbol = null,
    ) {
      if (!container) return;

      const lineClasses = [
        "equities-line-1",
        "equities-line-2",
        "equities-line-3",
        "equities-line-4",
        "equities-line-5",
        "equities-line-6",
      ];

      const comparisonUniverse = getEquitiesComparisonUniverse(selectedSymbol);

      const symbols = comparisonUniverse.filter(
        (symbol) =>
          Array.isArray(groupedSeries[symbol]) && groupedSeries[symbol].length,
      );

      if (!symbols.length) {
        container.innerHTML = `<div class="panel-placeholder">No equity index series available.</div>`;
        return;
      }

      const sliced = {};
      symbols.forEach((symbol) => {
        sliced[symbol] = sliceEquitiesRowsByHorizon(
          groupedSeries[symbol],
          horizon,
        );
      });

      const baseSeries = symbols
        .map((symbol) => {
          const rows = sliced[symbol];
          if (!rows.length) return null;

          const base = Number(rows[0].close);
          if (!Number.isFinite(base) || base === 0) return null;

          return {
            symbol,
            rows: rows
              .map((row) => ({
                date: row.date,
                level: (Number(row.close) / base - 1) * 100,
              }))
              .filter((row) => row.date && Number.isFinite(row.level)),
          };
        })
        .filter((series) => series && series.rows.length);

      if (!baseSeries.length) {
        container.innerHTML = `<div class="panel-placeholder">No normalized equity index series available.</div>`;
        return;
      }

      const width = Math.max(container.clientWidth || 320, 320);
      const height = Math.max(container.clientHeight || 240, 240);
      const padding = { top: 22, right: 18, bottom: 34, left: 18 };

      const allValues = baseSeries.flatMap((series) =>
        series.rows.map((r) => r.level),
      );
      const min = Math.min(...allValues, 0);
      const max = Math.max(...allValues, 0);
      const range = Math.max(max - min, 1e-9);
      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const valueToY = (value) => padding.top + ((max - value) / range) * innerH;

      const paths = baseSeries
        .map((series, idx) => {
          const points = series.rows.map((row, pointIdx) => ({
            x:
              padding.left +
              (pointIdx / Math.max(series.rows.length - 1, 1)) * innerW,
            y: valueToY(row.level),
          }));

          if (!points.length) return "";

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

      if (!paths.trim()) {
        container.innerHTML = `<div class="panel-placeholder">No renderable series.</div>`;
        return;
      }

      const zeroY = valueToY(0);
      const referenceRows = baseSeries.reduce((a, b) =>
        b.rows.length > a.rows.length ? b : a,
      ).rows;

      const labelIdx = [
        0,
        Math.floor((referenceRows.length - 1) / 2),
        referenceRows.length - 1,
      ];

      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const safeIdx = Math.min(idx, referenceRows.length - 1);
          const x =
            padding.left +
            (safeIdx / Math.max(referenceRows.length - 1, 1)) * innerW;
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

      try {
        container.innerHTML = `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Equities comparison chart">
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
      } catch (e) {
        container.innerHTML = `<div class="panel-placeholder">Render crash.</div>`;
      }
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
        y: valueToY(row.composite),
      }));

      const last = points[points.length - 1];
      const zeroY = valueToY(0);

      const bars = visibleRows
        .map((row, idx) => {
          const xCenter =
            padding.left + (idx / Math.max(visibleRows.length - 1, 1)) * innerW;
          const barWidth = Math.max(
            innerW / Math.max(visibleRows.length * 1.8, 10),
            4,
          );
          const y = valueToY(row.composite);
          const baseY = zeroY;
          const barY = Math.min(y, baseY);
          const barH = Math.max(Math.abs(baseY - y), 2);
          const cls =
            row.composite >= 0
              ? "equities-bar-positive"
              : "equities-bar-negative";

          return `<rect class="${cls}" x="${xCenter - barWidth / 2}" y="${barY}" width="${barWidth}" height="${barH}" rx="2"></rect>`;
        })
        .join("");

      const labelIdx = [
        0,
        Math.floor((visibleRows.length - 1) / 2),
        visibleRows.length - 1,
      ];
      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const x =
            padding.left + (idx / Math.max(visibleRows.length - 1, 1)) * innerW;
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

    async function loadActiveData() {
      const results = await Promise.allSettled([
        fetchJsonWithBust(DATA_ENDPOINTS.price),
        fetchJsonWithBust(DATA_ENDPOINTS.spreads),
        fetchJsonWithBust(DATA_ENDPOINTS.sigma),
        fetchJsonWithBust(DATA_ENDPOINTS.universe),
      ]);

      const [priceRes, spreadsRes, sigmaRes, universeRes] = results;

      endpointHealth.price = priceRes.status === "fulfilled";
      endpointHealth.spreads = spreadsRes.status === "fulfilled";
      endpointHealth.sigma = sigmaRes.status === "fulfilled";
      endpointHealth.universe = universeRes.status === "fulfilled";

      activeDataStore.price = endpointHealth.price
        ? normalizePricePayload(priceRes.value)
        : {};

      activeDataStore.spreads = endpointHealth.spreads
        ? normalizeSpreadsPayload(spreadsRes.value)
        : {};

      activeDataStore.sigma = endpointHealth.sigma
        ? normalizeSigmaPayload(sigmaRes.value)
        : [];

      activeDataStore.universe = endpointHealth.universe
        ? normalizeUniverse(universeRes.value)
        : [];

      activeDataLoaded =
        endpointHealth.price ||
        endpointHealth.spreads ||
        endpointHealth.sigma ||
        endpointHealth.universe;

      if (!activeDataLoaded) {
        throw new Error("All FX endpoints failed to load.");
      }

      activeDataLoadError = null;
    }

    function startActiveRefreshLoop() {
      if (activeRefreshHandle) {
        clearInterval(activeRefreshHandle);
      }

      let lastView = null;

      activeRefreshHandle = setInterval(async () => {
        try {
          const currentView = document.body.className;

          if (currentView === lastView) return; // prevent unnecessary rerender

          lastView = currentView;

          if (document.body.classList.contains("view-fx")) renderFX();
          if (document.body.classList.contains("view-equities")) renderEquities();
          if (document.body.classList.contains("view-rates")) renderRates();
          if (document.body.classList.contains("view-wti")) runWTIRenderSafe();
        } catch (err) {
          console.error("Refresh failed:", err);
        }
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

    const ABOUT_VIEW_NAMES = new Set([
      "what-is",
      "what-is-vector",
      "what-is-iso",
      "what-is-not",
      "core-model",
      "metrics-defined",
      "intended-interpretation",
      "cpmai-alignment",
      "system-architect",
    ]);

    function getTopNavViewName(viewName) {
      return ABOUT_VIEW_NAMES.has(viewName) ? "what-is" : viewName;
    }

    document
      .querySelectorAll(
        ".top-nav-item[data-view], .sidebar-nav .nav-item[data-view], .module-tab[data-view]",
      )
      .forEach((button) => {
        button.addEventListener("click", (event) => {
          event.preventDefault();
          event.stopPropagation();

          if (button.disabled || button.dataset.locked === "true") return;

          const viewName = button.dataset.view;
          if (!viewName) return;

          try {
            showView(viewName);
          } catch (err) {
            console.error("Toolbar navigation failed:", viewName, err);
          }
        });
      });

    function createLinePath(points) {
      if (!points || !points.length) return "";
      return points
        .map(
          (p, i) => `${i === 0 ? "M" : "L"} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`,
        )
        .join(" ");
    }

    function renderMacro() {
      const macroView = document.getElementById("view-macro");
      if (!macroView) return;

      const region = macroControls.region?.value || "USA";
      const horizon = macroControls.horizon?.value || "30D";
      const regimeFilter = macroControls.regimeFilter?.value || "Skeleton";
      const dataMode = macroControls.dataMode?.value || "Skeleton";

      const driversPanel = macroView.querySelector(".macro-drivers-panel");
      const transmissionPanel = macroView.querySelector(
        ".macro-transmission-panel",
      );
      const lagPanel = macroView.querySelector(".macro-lag-panel");
      const exposurePanel = macroView.querySelector(".macro-exposure-panel");
      const impactPanel = macroView.querySelector(".macro-impact-panel");
      const riskStackPanel = macroView.querySelector(".macro-riskstack-panel");

      if (driversPanel) {
        driversPanel.innerHTML = `
          <div class="panel-title">Macro Drivers</div>
          <div class="panel-placeholder">${region} ? ${horizon}</div>
        `;
      }

      if (transmissionPanel) {
        transmissionPanel.innerHTML = `
          <div class="panel-title">Transmission</div>
          <div class="panel-placeholder">Rates -- CPI -- USD -- Oil</div>
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
      if (riskStackPanel) {
        riskStackPanel.innerHTML = `
      <div class="iv-header">
    <span class="iv-title-main">
      <span class="vector-part">Diagnostics</span><span class="iso-part"> Contract</span>
    </span>

  </div>

      <div class="iv-contract-group iv-group-pressure">
        <div class="iv-group-title">System Pressure Layer</div>
        <div class="macro-metric-stack">
          <div class="macro-metric-tile"><span>Pressure</span><strong>--</strong></div>
          <div class="macro-metric-tile"><span>Liquidity</span><strong>--</strong></div>
          <div class="macro-metric-tile"><span>Momentum</span><strong>--</strong></div>
        </div>
      </div>

      <div class="iv-contract-group iv-group-fragility">
        <div class="iv-group-title">Structural Fragility Layer</div>
        <div class="macro-metric-stack">
          <div class="macro-metric-tile"><span>Fragility</span><strong>--</strong></div>
          <div class="macro-metric-tile"><span>Dispersion</span><strong>--</strong></div>
        </div>
      </div>

      <div class="iv-contract-group iv-group-transmission">
        <div class="iv-group-title">Cross-Asset Transmission Layer</div>
        <div class="macro-metric-stack">
          <div class="macro-metric-tile"><span>Cross-Asset Stress</span><strong>--</strong></div>
          <div class="macro-metric-tile"><span>Coherence</span><strong>--</strong></div>
        </div>
      </div>

      <div class="iv-contract-group iv-group-outcome">
        <div class="iv-group-title">System Outcome Layer</div>
        <div class="macro-metric-stack">
          <div class="macro-metric-tile"><span>Systemicity</span><strong>--</strong></div>
        </div>
      </div>
    `;
      }
    }

    function setBodyViewClass(viewName) {
      document.body.classList.remove(
        "view-what-is",
        "view-fx",
        "view-wti",
        "view-equities",
        "view-finstate",
        "view-cflow",
        "view-macro",
        "view-rates",
        "view-oc",
        "about-sidebar-hidden",
      );

      if (viewName === "fx") {
        document.body.classList.add("view-fx", "about-sidebar-hidden");
        renderFXVector();
        return;
      }

      if (viewName === "wti") {
        document.body.classList.add("view-wti", "about-sidebar-hidden");
        return;
      }

      if (viewName === "equities") {
        document.body.classList.add("view-equities", "about-sidebar-hidden");
        renderEquitiesVector();
        return;
      }

      if (viewName === "finstate") {
        document.body.classList.add("view-finstate", "about-sidebar-hidden");
        return;
      }

      if (viewName === "cflow") {
        document.body.classList.add("view-cflow", "about-sidebar-hidden");
        void renderCFlowVector();
        return;
      }

      if (viewName === "macro") {
        document.body.classList.add("view-macro", "about-sidebar-hidden");
        return;
      }

    if (viewName === "rates") {
      document.body.classList.add("view-rates", "about-sidebar-hidden");
      void renderRatesVector();
      return;
    }

      if (viewName === "oc") {
        document.body.classList.add("view-oc", "about-sidebar-hidden");

        window.scrollTo({ top: 0, behavior: "instant" });

        requestAnimationFrame(() => {
          window.updateOracleLabsScrollPanels?.();
        });

        return;
      }

      document.body.classList.add("view-what-is");
    }

    function setActiveButtons(viewName) {
      document.querySelectorAll(`[data-view="${viewName}"]`).forEach((button) => {
        button.classList.add("active");
      });
    }
    
    async function renderRates() {
      try {
        console.log("Rendering RATES...");
        // await renderRatesOracleChamber();
        void renderRatesVector();
        void renderRatesStateEngine();

        const RATES_COUNTRY_ALIASES = {
          US: "US",
          USA: "US",
          UNITEDSTATES: "US",
          UNITEDSTATESOFAMERICA: "US",

          AU: "AU",
          AUS: "AU",
          AUSTRALIA: "AU",

          CA: "CA",
          CAN: "CA",
          CANADA: "CA",

          DE: "DE",
          GER: "DE",
          DEU: "DE",
          GERMANY: "DE",

          EU: "EU",
          EUROAREA: "EU",
          EUROZONE: "EU",

          ES: "ES",
          ESP: "ES",
          SPAIN: "ES",

          FR: "FR",
          FRA: "FR",
          FRANCE: "FR",

          JP: "JP",
          JPN: "JP",
          JAPAN: "JP",

          KR: "KR",
          KOR: "KR",
          KOREA: "KR",
          SOUTHKOREA: "KR",

          UK: "UK",
          GB: "UK",
          GBR: "UK",
          UNITEDKINGDOM: "UK",
          GREATBRITAIN: "UK",
          BRITAIN: "UK",

          CH: "CH",
          CHE: "CH",
          SWITZERLAND: "CH",
          SWISS: "CH",

          IT: "IT",
          ITA: "IT",
          ITALY: "IT",

          CN: "CN",
          CHN: "CN",
          CHINA: "CN",
          PEOPLESREPUBLICOFCHINA: "CN",
        };

        if (ratesControls.metric?.value === "central-bank-commentary") {
          await renderRatesPolicyLanguageLatest();
          return;
        }

        function normalizeRatesCountry(value) {
          const raw = String(value || "")
            .toUpperCase()
            .replace(/&/g, "AND")
            .replace(/[^A-Z]/g, "");

          return RATES_COUNTRY_ALIASES[raw] || raw;
        }

        function getRatesCountryCode(row) {
          const symbol = String(
            row.symbol || row.series_id || row.fred_id || row.source || "",
          ).toUpperCase();

          // HARD MAP FIRST - avoid broad includes like AUS -> US
          if (symbol.includes("RBCNBIS") || symbol.includes("INTDSRCNM193N"))
            return "CN";

          if (/^(US|US_)/.test(symbol)) return "US";
          if (/^(AU|AUS)/.test(symbol)) return "AU";
          if (/^(CA|CAN)/.test(symbol)) return "CA";
          if (/^(DEU|GER|DE)/.test(symbol)) return "DE";
          if (/^(EU|EA|ECB)/.test(symbol)) return "EU";
          if (/^(ES|ESP)/.test(symbol)) return "ES";
          if (/^(FR|FRA)/.test(symbol)) return "FR";
          if (/^(JP|JPN)/.test(symbol)) return "JP";
          if (/^(KR|KOR)/.test(symbol)) return "KR";
          if (/^(UK|GBR|GB)/.test(symbol)) return "UK";
          // --- STRICT ORDER: CN BEFORE CH ---
          if (/^(CN|CHN)/.test(symbol)) return "CN";

          // Switzerland ONLY when explicitly CHE or CH_
          if (/^(CHE|CH_)/.test(symbol)) return "CH";

          if (/^(IT|ITA)/.test(symbol)) return "IT";

          // fallback to your existing logic
          const candidates = [
            row.country_code,
            row.iso2,
            row.iso3,
            row.country,
            row.country_name,
            row.market,
            row.region,
            row.name,
            row.label,
          ];

          const supportedCountries = Object.keys(RATES_COUNTRY_LABELS);

          for (const value of candidates) {
            const code = normalizeRatesCountry(value);
            if (supportedCountries.includes(code)) {
              return code;
            }
          }

          return null;
        }

        function rowsToSeries(rows, field) {
          return rows
            .map((r) => ({
              date: r.date || r.as_of_date,
              value: Number(r[field]),
            }))
            .filter((r) => r.date && Number.isFinite(r.value))
            .sort((a, b) => String(a.date).localeCompare(String(b.date)));
        }

        const supportedRateCountries = Object.keys(RATES_COUNTRY_LABELS);
        const selectedRegion = ratesControls.region?.value || "north_america";
        const selectedHorizon = ratesControls.horizon?.value || "1Y";
        updateRatesCountryOptions();
        const selectedRegionCountries = getRatesRegionCountries(selectedRegion);
        const selectedCountry = normalizeRatesCountry(
          ratesControls.country?.value || "US",
        );
        const selectedCountryLabel =
          RATES_COUNTRY_LABELS[selectedCountry] || selectedCountry;

        function resolveRatesRowCountry(row) {
          const rawCode = getRatesCountryCode(row);
          const normalizedFallback = normalizeRatesCountry(
            row.country_code || row.iso2 || row.country || row.country_name,
          );

          if (supportedRateCountries.includes(rawCode)) return rawCode;
          if (supportedRateCountries.includes(normalizedFallback)) {
            return normalizedFallback;
          }

          return null;
        }

        const panelRaw = await fetchJsonWithBust(DATA_ENDPOINTS.ratesPanel);
        const panelRows = Array.isArray(panelRaw)
          ? panelRaw
          : Array.isArray(panelRaw?.rows)
            ? panelRaw.rows
            : [];

        const sigmaRaw = await fetchJsonWithBust(DATA_ENDPOINTS.ratesSigma);
        const sigmaRows = Array.isArray(sigmaRaw)
          ? sigmaRaw
          : Array.isArray(sigmaRaw?.rows)
            ? sigmaRaw.rows
            : [];

        // ? LINE ~ (same location - replace ONLY the filter line)

        const regionalPanelRows = panelRows.filter((r) => {
          const code = resolveRatesRowCountry(r);
          return code ? selectedRegionCountries.includes(code) : false;
        });

        const countryRows = regionalPanelRows
          .filter((r) => {
            const code = resolveRatesRowCountry(r);

            if (!code) return false;

            return code === selectedCountry;
          })
          .sort((a, b) =>
            String(a.date || a.as_of_date || "").localeCompare(
              String(b.date || b.as_of_date || ""),
            ),
          );

        updateRatesGeoScenToolbarLabel();

        const languageTarget = document.getElementById("rates-geoscen-context");
        const languageBrand =
          CENTRAL_BANK_LANGUAGE_BRANDS[selectedCountry] ||
          CENTRAL_BANK_LANGUAGE_BRANDS.EU ||
          CENTRAL_BANK_LANGUAGE_BRANDS.US;
        const latestRatesLanguageDate = await getRatesLanguageLatestDate(
          selectedCountry,
          countryRows,
        );

        if (selectedCountry === "US") {
          await renderRatesGeoScenBridge({
            selectedCountry,
            selectedCountryLabel,
            selectedRegion,
          });
        } else if (languageTarget && languageBrand) {
          try {
            const commentaryUrl =
              DATA_ENDPOINTS.ratesCentralBankCommentary?.[selectedCountry];

            if (!commentaryUrl) {
              throw new Error(`No policy-language endpoint for ${selectedCountry}`);
            }

            const payload = await fetchJsonWithBust(commentaryUrl);
            const p = payload.policy_language || {};
            const coverage = payload.coverage || {};

            languageTarget.innerHTML = renderCentralBankLanguageBlock({
              country: selectedCountry,
              ...languageBrand,
              latestDate: payload.document_date || payload.as_of || latestRatesLanguageDate,
              state: `Latest statement only • ${coverage.classified_pct ?? "--"}% classified`,
              distribution: {
                SH: p.SH_pct,
                MH: p.MH_pct,
                N: p.N_pct,
                MD: p.MD_pct,
                SD: p.SD_pct,
              },
            });
          } catch (err) {
            console.error("Failed loading GeoScen RATES language block.", err);

            languageTarget.innerHTML = renderCentralBankLanguageBlock({
              country: selectedCountry,
              ...languageBrand,
              latestDate: latestRatesLanguageDate,
            });
          }
        }

        let yieldFamilyDisplayRows = [];

        const yieldFamilyRows = countryRows.filter(
          (r) =>
            Number.isFinite(Number(r.y2)) ||
            Number.isFinite(Number(r.y5)) ||
            Number.isFinite(Number(r.y10)) ||
            Number.isFinite(Number(r.y20)) ||
            Number.isFinite(Number(r.y30)) ||
            Number.isFinite(Number(r.y10_proxy)),
        );

        yieldFamilyDisplayRows = filterRatesSeriesByHorizon(
          yieldFamilyRows
            .map((row) => ({
              ...row,
              date: row.date || row.as_of_date,
            }))
            .filter((row) => row.date),
          selectedHorizon,
        );

        if (yieldFamilyDisplayRows.length) {
          const yieldSeriesDefs =
            selectedCountry === "CN"
              ? [{ field: "y10_proxy", label: "10Y", color: "#d5b37c" }]
              : [
                  { field: "y2", label: "2Y", color: "#60a5fa" },
                  { field: "y5", label: "5Y", color: "#2dd4bf" },
                  { field: "y10", label: "10Y", color: "#d5b37c" },
                  { field: "y20", label: "20Y", color: "#f59e0b" },
                  { field: "y30", label: "30Y", color: "#c084fc" },
                ];

          renderRatesYieldFamilyChart(
            document.getElementById("rates-curve-chart"),
            yieldFamilyDisplayRows,
            yieldSeriesDefs,
          );

          const curveDate = document.getElementById("rates-curve-date");
          if (curveDate) {
            curveDate.textContent = formatRatesLanguageDate(
              getRatesSeriesDate(yieldFamilyDisplayRows),
            );
          }
        } else {
          setChartPlaceholder(
            "rates-curve-chart",
            `No Yield Family data for ${selectedCountryLabel}`,
          );

          const curveDate = document.getElementById("rates-curve-date");
          if (curveDate) curveDate.textContent = "--";
        }

        const spreadRows = countryRows.filter((r) =>
          Number.isFinite(Number(r.spread)),
        );

        const policyPressureRows = countryRows.filter((r) =>
          Number.isFinite(Number(r.policy_pressure_t1)),
        );

        const policyProxyRows = countryRows.filter((r) =>
          Number.isFinite(Number(r.policy_proxy)),
        );

        const rawSpreadSeries = rowsToSeries(spreadRows, "spread");
        const spreadDisplaySeries = filterRatesSeriesByHorizon(
          rawSpreadSeries,
          selectedHorizon,
        );

        if (spreadDisplaySeries.length) {
          renderSpreadChart(
            document.getElementById("rates-spread-chart"),
            spreadDisplaySeries,
          );

          const lastSpread = spreadDisplaySeries[spreadDisplaySeries.length - 1];

          updateStatValue(
            document.getElementById("rates-spread-state"),
            Number(lastSpread.value).toFixed(2),
          );

          const spreadDate = document.getElementById("rates-spread-date");
          if (spreadDate) {
            spreadDate.textContent = formatRatesLanguageDate(lastSpread.date);
          }
        } else if (selectedCountry === "CN") {
          setChartPlaceholder(
            "rates-spread-chart",
            "China hybrid row: no standard curve spread.",
          );

          updateStatValue(
            document.getElementById("rates-spread-state"),
            "Hybrid",
          );

          const spreadDate = document.getElementById("rates-spread-date");
          if (spreadDate) spreadDate.textContent = "--";
        } else {
          setChartPlaceholder(
            "rates-spread-chart",
            `No Curve Spread data for ${selectedCountryLabel}`,
          );

          updateStatValue(
            document.getElementById("rates-spread-state"),
            "--",
          );

          const spreadDate = document.getElementById("rates-spread-date");
          if (spreadDate) spreadDate.textContent = "--";
        }

        const policySeries = policyPressureRows.length
          ? rowsToSeries(policyPressureRows, "policy_pressure_t1")
          : rowsToSeries(policyProxyRows, "policy_proxy");

        const selectedMetric = ratesControls.metric?.value || "yield-family";
        await renderRatesSelectedMetric({
          selectedCountry,
          selectedCountryLabel,
          selectedRegion,
          region: selectedRegion,
          horizon: selectedHorizon,
          countryRows,
          panelRows,
          regionalPanelRows,
          sigmaRows,
          selectedMetric,
          resolveRatesRowCountry,
        });

        updateRatesGeoScenToolbarLabel();

        const sigmaByCountry = {};
        const sigmaRegionCountries =
          RATES_SIGMA_COUNTRIES_BY_REGION[selectedRegion] ||
          RATES_SIGMA_COUNTRIES_BY_REGION.north_america;

        sigmaRows
          .filter((r) => {
            const group = String(r.display_group || r.group || r.metric || "")
              .toLowerCase();

            return (
              group.includes("curve") ||
              group.includes("10y") ||
              group.includes("yield") ||
              group.includes("benchmark") ||
              group.includes("level")
            );
          })
          .forEach((r) => {
            const code =
              resolveRatesRowCountry(r) ||
              normalizeRatesCountry(
                r.country || r.country_code || r.iso2 || r.iso3 || r.name,
              );

            if (!selectedRegionCountries.includes(code)) return;
            if (code === "CN") return;

            const z = Number(r.sigma_z);
            const rank = Number(r.sigma_rank);
            const date = String(r.date || r.as_of_date || "");

            if (!date || !Number.isFinite(z)) return;

            const existing = sigmaByCountry[code];

            if (!existing || date > existing.date) {
              sigmaByCountry[code] = {
                pair: code,
                z,
                rank:
                  Number.isFinite(rank) && rank > 0
                    ? rank
                    : Number.MAX_SAFE_INTEGER,
                pct: null,
                state:
                    code === "CN"
                      ? "Hybrid"
                      : String(r.display_group || "").includes("10Y")
                        ? "10Y Level"
                        : "Live",
                  group: r.display_group || r.group || "Curve",
                date,
              };
            }
          });

        const dedupedSigma = sigmaRegionCountries.map((code) => {
          const row = sigmaByCountry[code];

          if (row) {
            return {
              ...row,
              label: RATES_SIGMA_DISPLAY_LABELS[code] || code,
              group: CENTRAL_BANK_LANGUAGE_BRANDS[code]?.brand || row.group || "--",
              isBlank: false,
            };
          }

          return {
            pair: code,
            label: RATES_SIGMA_DISPLAY_LABELS[code] || code,
            z: 0,
            rank: null,
            pct: null,
            state: code === "CN" ? "Blank" : "--",
            group: CENTRAL_BANK_LANGUAGE_BRANDS[code]?.brand || "--",
            date: "",
            isBlank: true,
          };
        });

        const selectedSigmaFinal =
          dedupedSigma.find((r) => r.pair === selectedCountry) || dedupedSigma[0];

        const sigmaLatestDate = dedupedSigma
          .map((row) => row.date)
          .filter(Boolean)
          .sort((a, b) => String(a).localeCompare(String(b)))
          .pop();
        const sigmaDateEl = document.getElementById("rates-sigma-date");
        if (sigmaDateEl) {
          sigmaDateEl.textContent = sigmaLatestDate
            ? formatRatesLanguageDate(sigmaLatestDate)
            : "--";
        }

        updateStatValue(
          document.getElementById("rates-sigma-selected"),
          selectedSigmaFinal?.label || selectedSigmaFinal?.pair || selectedCountry,
        );

        updateStatValue(
          document.getElementById("rates-sigma-z"),
          selectedSigmaFinal && Number.isFinite(selectedSigmaFinal.z)
            ? `${selectedSigmaFinal.z >= 0 ? "+" : ""}${formatNumber(selectedSigmaFinal.z, 2)}`
            : "--",
          selectedSigmaFinal && Number.isFinite(selectedSigmaFinal.z)
            ? selectedSigmaFinal.z >= 0
              ? "positive"
              : "negative"
            : null,
        );

        updateStatValue(
          document.getElementById("rates-sigma-rank"),
          selectedSigmaFinal &&
            !selectedSigmaFinal.isBlank &&
            Number.isFinite(selectedSigmaFinal.z)
            ? `${dedupedSigma.findIndex((r) => r.pair === selectedSigmaFinal.pair) + 1}/${dedupedSigma.length}`
            : "--",
        );

        if (!dedupedSigma.length) {
          setChartPlaceholder("rates-sigma-chart", "No Sigma Rank data available.");
        } else {
          renderRatesSigmaChart(
            document.getElementById("rates-sigma-chart"),
            dedupedSigma,
            selectedSigmaFinal?.pair || selectedCountry,
          );
        }
      } catch (error) {
        console.error("Rates load failed:", error);

        setChartPlaceholder("rates-curve-chart", "Rates load failed.");
        setChartPlaceholder("rates-spread-chart", "Rates load failed.");
        setChartPlaceholder("rates-policy-chart", "Rates load failed.");
        setChartPlaceholder("rates-sigma-chart", "Sigma load failed.");

        updateStatValue(document.getElementById("rates-spread-state"), "--");
        updateStatValue(document.getElementById("rates-policy-state"), "--");
      }
    }

    function renderRatesSigmaChart(container, rows, selectedKey) {
      if (!container) return;

      if (!rows.length) {
        container.innerHTML = `<div class="panel-placeholder">No Sigma Rank data available.</div>`;
        return;
      }

      const ranked = [...rows];
      const finiteZ = ranked
        .map((r) => Number(r.z))
        .filter((value) => Number.isFinite(value));
      const maxAbs = Math.max(...finiteZ.map((value) => Math.abs(value)), 0.1);

      container.innerHTML = `
        <div class="rates-sigma-stack">
          ${ranked
            .map((row) => {
              const z = Number(row.z);
              const hasValue = Number.isFinite(z);
              const pct = hasValue ? Math.min(100, (Math.abs(z) / maxAbs) * 100) : 0;
              const isSelected = row.pair === selectedKey;
              const sign = hasValue && z >= 0 ? "+" : "";
              const label = row.group || "Curve";
              const isBlank = row.isBlank || !hasValue;

              return `
                <div class="rates-sigma-row ${isSelected ? "selected" : ""} ${isBlank ? "blank" : ""}">
                  <div class="rates-sigma-label">
                    <span>${row.label || row.pair}</span>
                    <small>${label}</small>
                  </div>

                  <div class="rates-sigma-track">
                    <div class="rates-sigma-bar" style="width:${pct}%"></div>
                  </div>

                  <div class="rates-sigma-value">${hasValue ? `${sign}${z.toFixed(2)}` : "--"}</div>
                </div>
              `;
            })
            .join("")}
        </div>
      `;
    }

    function runWTIRenderSafe() {
      try {
        Promise.resolve(renderWTI()).catch((err) => {
          console.error("WTI RENDER FAILURE:", err);
          setChartPlaceholder("wti-inventory-chart", "WTI render failed.");
          setChartPlaceholder("wti-price-chart", "WTI render failed.");
          setChartPlaceholder("wti-macro-chart", "WTI render failed.");
          setChartPlaceholder("wti-sigma-chart", "WTI render failed.");
        });
      } catch (err) {
        console.error("WTI HARD FAILURE:", err);
        setChartPlaceholder("wti-inventory-chart", "WTI render failed.");
        setChartPlaceholder("wti-price-chart", "WTI render failed.");
        setChartPlaceholder("wti-macro-chart", "WTI render failed.");
        setChartPlaceholder("wti-sigma-chart", "WTI render failed.");
      }
    }

    function resizeActivePlotlyCharts() {
      if (!window.Plotly?.Plots?.resize) return;

      document
        .querySelectorAll(".content-view.active .js-plotly-plot")
        .forEach((plot) => {
          try {
            window.Plotly.Plots.resize(plot);
          } catch (err) {
            console.warn("Plotly resize failed:", err);
          }
        });
    }

    function scheduleActivePlotlyResize() {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          resizeActivePlotlyCharts();
        });
      });

      window.setTimeout(resizeActivePlotlyCharts, 250);
    }

    function showView(viewName) {
      console.log("Switching view:", viewName);

      contentViews.forEach((view) => {
        view.classList.remove("active");
        view.removeAttribute("style");
      });
      viewButtons.forEach((button) => button.classList.remove("active"));

      const targetView = document.getElementById(`view-${viewName}`);

      if (!targetView) {
        console.error("Missing target view:", `view-${viewName}`);
        return;
      }

      targetView.classList.add("active");

      setBodyViewClass(viewName);
      setActiveButtons(viewName);
      syncSidebarHierarchy(viewName);

      moduleTabs.forEach((tab) => tab.classList.remove("active"));

      const topNavViewName = getTopNavViewName(viewName);

      document
        .querySelectorAll(
          `.module-tab[data-view="${topNavViewName}"], .top-nav-item[data-view="${topNavViewName}"]`,
        )
        .forEach((tab) => tab.classList.add("active"));

  if (viewName === "cflow") {
    void Promise.resolve(renderCFlow()).finally(scheduleActivePlotlyResize);
  }

  if (viewName === "finstate") {
    try {
      updateFinStateCountryOptions();
      updateFinStateIndustryOptions();

      renderFinState();
      scheduleActivePlotlyResize();

      Promise.allSettled([
        fetchJsonWithBust(DATA_ENDPOINTS.finstateUniverse),
        fetchJsonWithBust(DATA_ENDPOINTS.finstateGlobalLiteMetrics),
      ]).then(([universeRes, globalLiteRes]) => {
        const universePayload =
          universeRes.status === "fulfilled" ? universeRes.value : {};

        const globalLitePayload =
          globalLiteRes.status === "fulfilled" ? globalLiteRes.value : {};

        finstateUniverseData = Array.isArray(universePayload)
          ? universePayload
          : Array.isArray(universePayload?.rows)
            ? universePayload.rows
            : [];

        finstateGlobalLiteMetricsData = Array.isArray(globalLitePayload)
          ? globalLitePayload
          : Array.isArray(globalLitePayload?.rows)
          ? globalLitePayload.rows
          : [];

        renderFinState();
        scheduleActivePlotlyResize();
      });
    } catch (err) {
      console.error("FINSTATE render failed:", err);
    }
  }

      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          try {
            if (viewName === "fx") {
              populateFxPairOptions();
              renderFXDeferred();
              scheduleActivePlotlyResize();
            }

            if (viewName === "wti") {
              runWTIRenderSafe();
              scheduleActivePlotlyResize();
            }

            if (viewName === "equities") {
              void Promise.resolve(renderEquities()).finally(
                scheduleActivePlotlyResize,
              );
            }

            if (viewName === "macro") {
              renderMacro();
              scheduleActivePlotlyResize();
            }

            if (viewName === "rates") {
              void Promise.resolve(renderRates()).finally(
                scheduleActivePlotlyResize,
              );
            }

            if (viewName === "oc") {
              console.log("OC view active");
            }
          } catch (err) {
            console.error("View render failed:", viewName, err);
          }
        });
      });
    }

    function renderGlobalEquityRegionTable() {
      const target = document.getElementById("global-equity-region-body");
      if (!target) return;

      const rows = globalEquityRegionPanelData || [];

      target.innerHTML = rows.map((r) => `
        <tr>
          <td>${r.region ?? "--"}</td>
          <td>${r.symbol ?? "--"}</td>
          <td>${r.date ?? "--"}</td>
          <td>${formatNumber(r.close, 2)}</td>
          <td>${formatNumber((r.ret_20d ?? 0) * 100, 2)}%</td>
          <td>${formatNumber((r.ret_60d ?? 0) * 100, 2)}%</td>
          <td>${formatNumber(r.equity_region_score, 2)}</td>
          <td>${r.state ?? "--"}</td>
        </tr>
      `).join("");
      }

    async function renderRatesGeoScenBridge({
      selectedCountry = "US",
      selectedCountryLabel = "United States",
      selectedRegion,
    } = {}) {
      const target = document.getElementById("rates-geoscen-context");
      if (!target) return;

      try {
        const payload = await fetchJsonWithBust(DATA_ENDPOINTS.ratesGeoScenBridge);
        const evidence = Array.isArray(payload?.published_evidence)
          ? payload.published_evidence
          : [];

        if (!evidence.length) {
          target.innerHTML = `<div class="panel-placeholder">No RATES GeoScen evidence available.</div>`;
          return;
        }

        const contextBullets = [
          "Capital remains constrained because long-duration yields are still carrying elevated pressure.",
          "The curve is not signaling broad structural disorder; dispersion remains contained despite elevated rate levels.",
          "Policy restriction is close to neutral, so the main pressure is coming from yield levels rather than acute policy shock.",
          "Positive real-yield pressure keeps discount-rate sensitivity relevant for duration assets, credit, and equity multiples.",
        ];

        let topbarHtml = `
          <div class="central-bank-language-topbar">
            <div class="central-bank-language-meta">
              <span class="central-bank-language-brand">GeoScen</span>
              <span class="central-bank-language-dot">•</span>
              <span class="central-bank-language-date">${selectedCountryLabel}</span>
            </div>

            <div class="central-bank-language-strip" aria-label="Policy language distribution">
              <span class="central-bank-language-bracket">•|</span>
              <span class="central-bank-language-segment central-bank-language-segment-sh" title="${CENTRAL_BANK_LANGUAGE_STATE_LABELS.SH}">-- SH</span>
              <span class="central-bank-language-pipe">|</span>
              <span class="central-bank-language-segment central-bank-language-segment-mh" title="${CENTRAL_BANK_LANGUAGE_STATE_LABELS.MH}">-- MH</span>
              <span class="central-bank-language-pipe">|</span>
              <span class="central-bank-language-segment central-bank-language-segment-n" title="${CENTRAL_BANK_LANGUAGE_STATE_LABELS.N}">-- N</span>
              <span class="central-bank-language-pipe">|</span>
              <span class="central-bank-language-segment central-bank-language-segment-md" title="${CENTRAL_BANK_LANGUAGE_STATE_LABELS.MD}">-- MD</span>
              <span class="central-bank-language-pipe">|</span>
              <span class="central-bank-language-segment central-bank-language-segment-sd" title="${CENTRAL_BANK_LANGUAGE_STATE_LABELS.SD}">-- SD</span>
              <span class="central-bank-language-bracket">|•</span>
            </div>
          </div>
        `;

        try {
          const commentary = await fetchJsonWithBust(
            DATA_ENDPOINTS.ratesCentralBankCommentary.US,
          );
          const policyLanguage = commentary?.policy_language || {};
          const hasPolicyLanguage = ["SH_pct", "MH_pct", "N_pct", "MD_pct", "SD_pct"]
            .some((key) => Number.isFinite(Number(policyLanguage[key])));

          if (hasPolicyLanguage) {
            const brand =
              CENTRAL_BANK_LANGUAGE_BRANDS.US?.brand || "FedSpeak";
            const displayDate = formatRatesLanguageDate(
              commentary.document_date || commentary.as_of || "--",
            );
            const strip = [
              ["SH", policyLanguage.SH_pct],
              ["MH", policyLanguage.MH_pct],
              ["N", policyLanguage.N_pct],
              ["MD", policyLanguage.MD_pct],
              ["SD", policyLanguage.SD_pct],
            ]
              .map(
                ([code, value]) => `
                  <span class="central-bank-language-segment central-bank-language-segment-${code.toLowerCase()}" title="${CENTRAL_BANK_LANGUAGE_STATE_LABELS[code]}">
                    ${formatPolicyPctHtml(value)} ${code}
                  </span>
                `,
              )
              .join('<span class="central-bank-language-pipe">|</span>');

            topbarHtml = `
              <div class="central-bank-language-topbar">
                <div class="central-bank-language-meta">
                  <span class="central-bank-language-brand">${brand}</span>
                  <span class="central-bank-language-dot">•</span>
                  <span class="central-bank-language-date">${displayDate}</span>
                </div>

                <div class="central-bank-language-strip" aria-label="Policy language distribution">
                  <span class="central-bank-language-bracket">•|</span>
                  ${strip}
                  <span class="central-bank-language-bracket">|•</span>
                </div>
              </div>
            `;
          }
        } catch (err) {
          console.warn("RATES FedSpeak topbar unavailable.", err);
        }

        target.innerHTML = `
          <div class="central-bank-language-block rates-geoscen-evidence-block">
            ${topbarHtml}

            <div class="central-bank-language-rule" aria-hidden="true"></div>

            <div class="central-bank-language-body rates-geoscen-body">
              <div class="central-bank-language-context-value rates-geoscen-context-list">
                ${contextBullets.map((text) => `<div>• ${text}</div>`).join("")}
              </div>

              <div class="central-bank-language-state-wrap">
                <div class="central-bank-language-state-label">Governance</div>
                Forecast Guard: ENABLED • Evidence Rule: REQUIRED • LLM Decisions: DISABLED
              </div>
            </div>
          </div>
        `;
      } catch (err) {
        console.error("Failed loading RATES GeoScen bridge.", err);
        target.innerHTML = `<div class="panel-placeholder">RATES GeoScen bridge failed to load.</div>`;
      }
    }


    async function loadGlobalEquityRegionData() {
      const [panelPayload, latestPayload] = await Promise.all([
        fetchJsonWithBust(DATA_ENDPOINTS.globalEquityRegionPanel),
        fetchJsonWithBust(DATA_ENDPOINTS.globalEquityRegionLatest),
      ]);

      globalEquityRegionPanelData = Array.isArray(panelPayload)
        ? panelPayload
        : Array.isArray(panelPayload?.rows)
          ? panelPayload.rows
          : [];

      globalEquityRegionLatestData = latestPayload || null;
    }

    function syncSidebarHierarchy(viewName) {
      clearSidebarState();

      if (
        viewName === "what-is" ||
        viewName === "what-is-vector" ||
        viewName === "what-is-iso"
      ) {
        openSubnav("what-is");

        const parent = document.querySelector(
          '.nav-parent[data-parent="what-is"]',
        );
        if (parent) parent.classList.add("active");

        if (viewName !== "what-is") {
          const child = document.querySelector(
            `.nav-child[data-view="${viewName}"]`,
          );
          if (child) child.classList.add("active");
        } else {
          const landing = document.querySelector(
            '.nav-parent[data-view="what-is"]',
          );
          if (landing) landing.classList.add("active");
        }

        return;
      }

      closeAllSubnavs();

      const direct = document.querySelector(
        `.sidebar-nav .nav-item[data-view="${viewName}"]`,
      );
      if (direct) {
        direct.classList.add("active");
      }
    }

    document
      .getElementById("finstate-quadrant-mode")
      ?.addEventListener("change", renderFinState);

    function getPairDigits(pair) {
      return pair === "USD/JPY" ? 2 : 4;
    }

    function formatDateLabelParts(dateStr) {
      const d = new Date(`${dateStr}T00:00:00`);
      if (Number.isNaN(d.getTime())) {
        return { top: String(dateStr), bottom: "" };
      }

      return {
        top: d.toLocaleDateString("en-US", {
          month: "short",
          day: "numeric",
        }),
        bottom: `'${String(d.getFullYear()).slice(-2)}`,
      };
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
        prev = values[i] * k + prev * (1 - k);
        ema[i] = Number(prev.toFixed(6));
      }

      return ema;
    }

    function getLockedUniverseSymbols() {
      const fromUniverse = normalizeUniverse(activeDataStore.universe);

      if (fromUniverse.length) return fromUniverse;

      const priceKeys = Object.keys(activeDataStore.price || {}).map((x) =>
        x.toUpperCase(),
      );
      const spreadKeys = Object.keys(activeDataStore.spreads || {}).map((x) =>
        x.toUpperCase(),
      );
      const sigmaKeys = (activeDataStore.sigma || []).map((row) =>
        String(row.symbol || "").toUpperCase(),
      );

      const intersection = priceKeys
        .filter((sym) => spreadKeys.includes(sym) && sigmaKeys.includes(sym))
        .sort();

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
          pair:
            SYMBOL_TO_DISPLAY_PAIR[String(row.symbol || "").toUpperCase()] ||
            row.pair ||
            row.symbol,
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
      const labelNode = document.querySelector(
        "[data-wti-geoscen-toolbar-label]",
      );
      if (!labelNode || !wtiControls.geoscen) return;

      const mode = String(wtiControls.geoscen.value || "").toLowerCase();

      labelNode.textContent =
        mode === "systemic"
          ? "GeoScen | OC | Systemic, Groups"
          : "GeoScen | OC | Home, Countries";
    }

    function renderWtiInventoryOcOverlayChart(container, payload) {
      if (!container) return;

      const rows = Array.isArray(payload?.rows) ? payload.rows : [];
      if (!rows.length) {
        container.innerHTML = `<div class="panel-placeholder">No OC overlay inventory data available.</div>`;
        return;
      }

      const usable = rows.filter(
        (r) =>
          Number.isFinite(Number(r.week)) &&
          (Number.isFinite(Number(r.min)) ||
            Number.isFinite(Number(r.avg)) ||
            Number.isFinite(Number(r.max)) ||
            Number.isFinite(Number(r.current))),
      );

      if (!usable.length) {
        container.innerHTML = `<div class="panel-placeholder">No usable OC overlay inventory data available.</div>`;
        return;
      }

      const width = Math.max(container.clientWidth, 300);
      const height = Math.max(container.clientHeight, 230);

      const padding = {
        top: 34,
        right: 16,
        bottom: 34,
        left: 16,
      };

      const seriesValues = usable
        .flatMap((row) => [row.min, row.avg, row.max, row.current])
        .filter(
          (v) => v !== null &&
                v !== undefined &&
                Number.isFinite(Number(v))
        )
        .map(Number);

      const rawMin = Math.min(...seriesValues);
      const rawMax = Math.max(...seriesValues);
      const rawRange = Math.max(rawMax - rawMin, 1e-9);

      const pad = rawRange * 0.15;

      const minVal = rawMin - pad;
      const maxVal = rawMax + pad;
      const range = Math.max(maxVal - minVal, 1e-9);

      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const valueToY = (value) =>
        padding.top + ((maxVal - value) / range) * innerH;

      const xStep = innerW / Math.max(usable.length - 1, 1);

      function buildPoints(key) {
        const points = [];

        usable.forEach((row, idx) => {
          const raw = row[key];
          const value = Number(raw);

          if (!Number.isFinite(value)) return;

          if (key === "current" && raw == null) return;

          points.push({
            x: padding.left + idx * xStep,
            y: valueToY(value),
            week: Number(row.week),
          });
        });

        return points;
      }

      const minPoints = buildPoints("min");
      const avgPoints = buildPoints("avg");
      const maxPoints = buildPoints("max");
      const currentPoints = buildPoints("current");

      const minPath = createLinePath(minPoints);
      const avgPath = createLinePath(avgPoints);
      const maxPath = createLinePath(maxPoints);
      const currentPath = createLinePath(currentPoints);

      const currentLast = currentPoints.length
        ? currentPoints[currentPoints.length - 1]
        : null;
      const currentWeekLabel = currentLast?.week ?? null;

      const yTicks = [maxVal, (maxVal + minVal) / 2, minVal]
        .map((v, idx) => {
          const y = padding.top + (idx / 2) * innerH;
          return `
          <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
          <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(v, 2)}</text>
        `;
        })
        .join("");

      const labelIdx = [
        0,
        Math.floor((usable.length - 1) / 2),
        usable.length - 1,
      ];
      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const x = padding.left + idx * xStep;
          return `<text class="fx-axis-label" x="${x}" y="${height - 10}" text-anchor="middle">W${usable[idx].week}</text>`;
        })
        .join("");

      container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="WTI Inventory OC Overlay Chart">
        ${yTicks}

        ${maxPath ? `<path d="${maxPath}" fill="none" stroke="#dc2626" stroke-width="1.8" stroke-dasharray="5 4"></path>` : ""}
        ${avgPath ? `<path d="${avgPath}" fill="none" stroke="#9ca3af" stroke-width="1.8" stroke-dasharray="6 4"></path>` : ""}
        ${minPath ? `<path d="${minPath}" fill="none" stroke="#16a34a" stroke-width="1.8" stroke-dasharray="5 4"></path>` : ""}
        ${currentPath ? `<path d="${currentPath}" fill="none" stroke="#ffffff" stroke-width="2.8" style="filter: drop-shadow(0 0 1.5px rgba(255,255,255,0.35));"></path>` : ""}

        ${currentLast ? `<circle cx="${currentLast.x}" cy="${currentLast.y}" r="4.5" fill="#ffffff" stroke="rgba(17,24,39,0.8)" stroke-width="1.2"></circle>` : ""}
        ${currentLast && currentWeekLabel != null ? `<text x="${currentLast.x + 8}" y="${currentLast.y - 8}" fill="#e5e7eb" font-size="10" font-weight="700">W${currentWeekLabel}</text>` : ""}

        <text x="${padding.left + 8}" y="16" fill="#dc2626" font-size="10" font-weight="700" letter-spacing="0.04em">MAX</text>
        <text x="${padding.left + 48}" y="16" fill="#9ca3af" font-size="10" font-weight="700" letter-spacing="0.04em">AVG</text>
        <text x="${padding.left + 88}" y="16" fill="#16a34a" font-size="10" font-weight="700" letter-spacing="0.04em">MIN</text>
        <text x="${padding.left + 128}" y="16" fill="#e5e7eb" font-size="10" font-weight="700" letter-spacing="0.04em">CURR</text>

        ${xLabels}
      </svg>
    `;
    }

    function updateEquitiesGeoScenToolbarLabel() {
      const labelNode = document.querySelector(
        "[data-equities-geoscen-toolbar-label]",
      );
      if (!labelNode || !equitiesControls.geoscen) return;

      const mode = String(equitiesControls.geoscen.value || "").toLowerCase();

      labelNode.textContent =
        mode === "systemic"
          ? "GeoScen | OC | Systemic, Groups"
          : "GeoScen | OC | Home, Countries";
    }


    function updateRatesGeoScenToolbarLabel() {
      const labelNode = document.querySelector(
        "[data-rates-geoscen-toolbar-label]",
      );

      if (!labelNode || !ratesControls.geoscen) return;

      const mode = String(ratesControls.geoscen.value || "").toLowerCase();
      const regionKey = ratesControls.region?.value || "north_america";
      const regionLabel =
        ratesControls.region?.selectedOptions?.[0]?.textContent || "North America";
      const countryCode = ratesControls.country?.value || getRatesRegionCountries(regionKey)[0];
      const countryLabel = RATES_COUNTRY_LABELS[countryCode] || countryCode;

      if (mode !== "systemic" && countryCode === "US") {
        labelNode.textContent = "GEOSCEN | OC | NORTH AMERICA | UNITED STATES";
        return;
      }

      labelNode.textContent =
        mode === "systemic"
          ? `GeoScen | OC | Systemic | ${regionLabel}`
          : `GeoScen | OC | ${regionLabel} | ${countryLabel}`;
    }

    function renderEquitiesTopRightSkeleton(mode) {
      const titleEl = document.querySelector(
        "#view-equities .equities-top-right-panel .panel-title",
      );
      const subtitleEl = document.getElementById("equities-top-right-subtitle");
      const badgeEl = document.getElementById("equities-top-right-badge");
      const primaryEl = document.getElementById("equities-top-right-primary");
      const secondaryEl = document.getElementById("equities-top-right-secondary");
      const stateEl = document.getElementById("equities-top-right-state");
      const chartEl = document.getElementById("equities-top-right-chart");

      if (
        !titleEl ||
        !subtitleEl ||
        !badgeEl ||
        !primaryEl ||
        !secondaryEl ||
        !stateEl ||
        !chartEl
      ) {
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

  function normalizeGlobalEquityRegionPayload(payload) {
    return Array.isArray(payload?.rows) ? payload.rows : [];
  }

  function filterGlobalEquityRowsByRegion(rows, region) {
    if (region === "Europe+") {
      return rows.filter((r) => r.region === "Europe+");
    }

    if (region === "Asia-Pacific") {
      return rows.filter((r) =>
        ["Asia-Pacific", "Japan", "Australia", "Hong Kong", "China Gateway"].includes(r.region)
      );
    }

    return [];
  }

  function renderGlobalEquityRegionTable(container, rows, region) {
    if (!container) return;

    if (!rows.length) {
      container.innerHTML = `<div class="panel-placeholder">No global equity region data available for ${region}.</div>`;
      return;
    }

    container.innerHTML = `
      <table class="equities-industry-table">
        <thead>
          <tr>
            <th>Region</th>
            <th>ETF</th>
            <th>Date</th>
            <th>Close</th>
            <th>20D</th>
            <th>60D</th>
            <th>Score</th>
            <th>State</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map((r) => `
            <tr>
              <td>${r.region ?? "--"}</td>
              <td>${r.symbol ?? "--"}</td>
              <td>${r.date ?? "--"}</td>
              <td>${formatNumber(r.close, 2)}</td>
              <td>${formatNumber(Number(r.ret_20d) * 100, 2)}%</td>
              <td>${formatNumber(Number(r.ret_60d) * 100, 2)}%</td>
              <td>${formatNumber(r.equity_region_score, 2)}</td>
              <td>${r.state ?? "--"}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    `;
  }

  async function renderGlobalEquityRegion(region, horizon) {
    const payload = await fetchJsonWithBust(DATA_ENDPOINTS.globalEquityRegionPanel);
    const rows = filterGlobalEquityRowsByRegion(
      normalizeGlobalEquityRegionPayload(payload),
      region
    );

    renderGlobalEquityRegionTable(
      document.getElementById("equities-index-chart"),
      rows,
      region
    );

    updateStatValue(document.getElementById("equities-index-focus"), region);
    updateStatValue(document.getElementById("equities-index-state"), "Live");
    updateStatValue(document.getElementById("equities-index-mode"), "Global Region ETF");

    const latestDate = rows.map((r) => r.date).filter(Boolean).sort().slice(-1)[0] || "--";
    const indexDate = document.getElementById("equities-index-date");
    if (indexDate) indexDate.textContent = latestDate;

    renderEquitiesIndexPlaceholder(
      "equities-industry-chart",
      `${region} PMI composite not wired yet. Region ETF diagnostics are live.`
    );

    updateStatValue(document.getElementById("equities-industry-state"), "Region ETF Live");
  }



    async function renderEquities() {
      updateEquitiesGeoScenToolbarLabel();
      renderEquitiesVector();
      await renderEquitiesOracleChamber();

      const region = equitiesControls.region?.value || "USA";
      const horizon = equitiesControls.horizon?.value || "30D";
      const topRightMode =
        equitiesControls.topRightMode?.value || "Market Breadth";
      const etfFocus = String(
        equitiesControls.etfFocus?.value || "SPY",
      ).toUpperCase();

      let latestIndexDate = "--";

      const indexBadge = document.getElementById("equities-index-badge");
      const indexDate = document.getElementById("equities-index-date");
      const industryDate = document.getElementById("equities-industry-date");
      const topRightDate = document.getElementById("equities-top-right-date");
      const sigmaDate = document.getElementById("equities-sigma-date");

      if (indexBadge) indexBadge.textContent = `${region} ? ${horizon}`;

      const regionIsLive = region === "USA";

      if (!regionIsLive) {
        await renderGlobalEquityRegion(region, horizon);
        renderEquitiesTopRightSkeleton(topRightMode);
        return;
      }

      try {
        const equitiesData = await loadEquitiesData();

        const selectedIndexPayload = isSectorEtf(etfFocus)
          ? equitiesData.sectorEtf
          : equitiesData.index;

        const payloadLatest =
          selectedIndexPayload?.meta?.latest_date ||
          selectedIndexPayload?.latest_date ||
          null;

        const selectedIndexHealth = isSectorEtf(etfFocus)
          ? equitiesData.health.sectorEtf
          : equitiesData.health.index;

        if (!selectedIndexHealth) {
          renderEquitiesIndexPlaceholder(
            "equities-index-chart",
            isSectorEtf(etfFocus)
              ? "Sector ETF endpoint unavailable."
              : "Equity index endpoint unavailable.",
          );
          updateStatValue(
            document.getElementById("equities-index-state"),
            "Endpoint Unavailable",
          );
        } else {
          const groupedSeries = coerceEquitiesIndexSeries(
            selectedIndexPayload,
            etfFocus,
          );
          const comparisonUniverse = getEquitiesComparisonUniverse(etfFocus);

          const availableSymbols = comparisonUniverse.filter(
            (symbol) =>
              Array.isArray(groupedSeries[symbol]) &&
              groupedSeries[symbol].length,
          );

          const selectedSymbol = availableSymbols.includes(etfFocus)
            ? etfFocus
            : null;

          if (!availableSymbols.length) {
            renderEquitiesIndexPlaceholder(
              "equities-index-chart",
              isSectorEtf(etfFocus)
                ? "No sector ETF series available."
                : "No equity index series available.",
            );
            updateStatValue(
              document.getElementById("equities-index-state"),
              "No Data",
            );
          } else {
            renderEquitiesIndexChart(
              document.getElementById("equities-index-chart"),
              groupedSeries,
              horizon,
              selectedSymbol,
            );

            updateStatValue(
              document.getElementById("equities-index-state"),
              "Live",
            );
            updateStatValue(
              document.getElementById("equities-index-focus"),
              formatEquitiesFocusLabel(etfFocus || "SPY"),
            );
            updateStatValue(
              document.getElementById("equities-index-mode"),
              isSectorEtf(etfFocus) ? "Sector ETF (USA)" : "USA Multi-Index",
            );

            const selectedSeries = groupedSeries[etfFocus] || [];

            latestIndexDate =
              payloadLatest ||
              (selectedSeries.length
                ? selectedSeries[selectedSeries.length - 1].date
                : null) ||
              "--";

            if (indexDate) {
              indexDate.textContent = latestIndexDate;
            }
          }
        }

        if (!equitiesData.health.industryPanel) {
          renderEquitiesIndexPlaceholder(
            "equities-industry-chart",
            "Industry panel endpoint unavailable.",
          );
          updateStatValue(
            document.getElementById("equities-industry-state"),
            "Endpoint Unavailable",
          );
        } else {
          const industrySubtitle = document.getElementById(
            "equities-industry-subtitle",
          );

          if (isSectorEtf(etfFocus)) {
            const panelRows = Array.isArray(equitiesData.industryPanel)
              ? equitiesData.industryPanel
              : [];
            const etfRows = filterIndustryRowsForEtf(panelRows, etfFocus);

            if (industrySubtitle) {
              industrySubtitle.textContent = formatEquitiesFocusLabel(etfFocus);
            }

            if (!etfRows.length) {
              renderEquitiesIndexPlaceholder(
                "equities-industry-chart",
                `No industry panel available for ${etfFocus}.`,
              );
              updateStatValue(
                document.getElementById("equities-industry-pmi"),
                "--",
              );
              updateStatValue(
                document.getElementById("equities-industry-bias"),
                "--",
              );
              updateStatValue(
                document.getElementById("equities-industry-state"),
                "No Data",
              );
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
                avgSig == null
                  ? "--"
                  : `${avgSig >= 0 ? "+" : ""}${formatNumber(avgSig, 2)}`,
                avgSig == null ? null : avgSig >= 0 ? "positive" : "negative",
              );

              updateStatValue(
                document.getElementById("equities-industry-bias"),
                "Panel",
              );
              updateStatValue(
                document.getElementById("equities-industry-state"),
                "Live",
              );

              renderIndustryPanelTable(
                document.getElementById("equities-industry-chart"),
                panelRows,
                etfFocus,
              );

              if (industryDate) {
                industryDate.textContent = latestDate || "--";
              }

              const industrySource = document.getElementById(
                "equities-industry-source",
              );
              if (industrySource) {
                industrySource.innerHTML = `Source: Manual citation | ISM (OC) | Showing ${formatMonthYearLabel(latestDate)}`;
              }
            }
          } else if (isMarketIndexEtf(etfFocus)) {
            const cfg = EQUITIES_LENS_CONFIG[etfFocus];

            if (industrySubtitle) {
              industrySubtitle.textContent = `Lens Structure - ${formatEquitiesFocusLabel(etfFocus)}`;
            }

            updateStatValue(
              document.getElementById("equities-industry-pmi"),
              cfg?.lens_primary || "--",
            );
            updateStatValue(
              document.getElementById("equities-industry-bias"),
              cfg?.lens_secondary || "--",
            );
            updateStatValue(
              document.getElementById("equities-industry-state"),
              cfg?.cyclical_defensive || "--",
            );

            renderEquitiesLensCard(
              document.getElementById("equities-industry-chart"),
              etfFocus,
            );

            if (industryDate) {
              industryDate.textContent = latestIndexDate;
            }

            const industrySource = document.getElementById(
              "equities-industry-source",
            );

            if (industrySource) {
              industrySource.innerHTML =
                latestIndexDate && latestIndexDate !== "--"
                  ? `Source: Tiingo | the_Spine | As of ${latestIndexDate}`
                  : "Source: Tiingo | the_Spine";
            }
          } else {
            renderEquitiesIndexPlaceholder(
              "equities-industry-chart",
              `No industry/lens definition available for ${etfFocus}.`,
            );
            updateStatValue(
              document.getElementById("equities-industry-pmi"),
              "--",
            );
            updateStatValue(
              document.getElementById("equities-industry-bias"),
              "--",
            );
            updateStatValue(
              document.getElementById("equities-industry-state"),
              "No Data",
            );
          }
        }

        if (topRightMode === "Market Breadth") {
          const titleEl = document.querySelector(
            "#view-equities .equities-top-right-panel .panel-title",
          );
          const subtitleEl = document.getElementById(
            "equities-top-right-subtitle",
          );

          if (titleEl) titleEl.textContent = "Market Breadth";
          if (subtitleEl) subtitleEl.textContent = `${etfFocus} Breadth Signal`;

          if (!equitiesData.health.breadth) {
            renderEquitiesIndexPlaceholder(
              "equities-top-right-chart",
              "Breadth endpoint unavailable.",
            );
            updateStatValue(
              document.getElementById("equities-top-right-primary"),
              "--",
            );
            updateStatValue(
              document.getElementById("equities-top-right-secondary"),
              "--",
            );
            updateStatValue(
              document.getElementById("equities-top-right-state"),
              "Endpoint Unavailable",
            );
          } else {
            const breadthRows = coerceEquitiesPmiSeries(
              equitiesData.breadth.filter(
                (row) => String(row.etf || "").toUpperCase() === etfFocus,
              ),
            );

            const latestBreadth = breadthRows.length
              ? breadthRows[breadthRows.length - 1]
              : null;

            if (latestBreadth) {
              updateStatValue(
                document.getElementById("equities-top-right-primary"),
                formatNumber(latestBreadth.composite, 2),
                latestBreadth.composite >= 0 ? "positive" : "negative",
              );
              updateStatValue(
                document.getElementById("equities-top-right-secondary"),
                String(latestBreadth.bias),
              );
              updateStatValue(
                document.getElementById("equities-top-right-state"),
                String(latestBreadth.state),
              );

              renderEquitiesPmiChart(
                document.getElementById("equities-top-right-chart"),
                breadthRows,
                horizon,
              );

              if (topRightDate) {
                topRightDate.textContent = latestBreadth.date || "--";
              }
            } else {
              renderEquitiesIndexPlaceholder(
                "equities-top-right-chart",
                `No breadth data available for ${etfFocus}.`,
              );
              updateStatValue(
                document.getElementById("equities-top-right-primary"),
                "--",
              );
              updateStatValue(
                document.getElementById("equities-top-right-secondary"),
                "--",
              );
              updateStatValue(
                document.getElementById("equities-top-right-state"),
                "No Data",
              );
            }
          }
        }

        if (!equitiesData.health.sigma) {
          setChartPlaceholder(
            "equities-sigma-chart",
            "Sigma endpoint unavailable.",
          );
          if (sigmaDate) {
          }
        } else {
          const sigmaRows = coerceEquitiesSigmaRows(equitiesData.sigma);
          const sigmaRow = sigmaRows.find((row) => row.symbol === etfFocus);

          if (sigmaDate) {
            sigmaDate.textContent = sigmaRow?.as_of_date || "--";
          }

          if (sigmaRow) {
            updateStatValue(
              document.getElementById("equities-sigma-selected"),
              sigmaRow.symbol,
            );
            updateStatValue(
              document.getElementById("equities-sigma-z"),
              `${sigmaRow.z >= 0 ? "+" : ""}${formatNumber(sigmaRow.z, 2)}`,
              sigmaRow.z >= 0 ? "positive" : "negative",
            );
            updateStatValue(
              document.getElementById("equities-sigma-rank"),
              `${sigmaRow.rank}/${sigmaRows.length}`,
            );

            renderAssetSigmaChart(
              document.getElementById("equities-sigma-chart"),
              sigmaRows,
              sigmaRow.symbol,
            );
          } else {
            setChartPlaceholder(
              "equities-sigma-chart",
              `No sigma data available for ${etfFocus}.`,
            );
          }
        }
      } catch (error) {
        console.error("Equities panel load failed:", error);
        renderEquitiesIndexPlaceholder(
          "equities-index-chart",
          "Equities index load failed.",
        );
        renderEquitiesIndexPlaceholder(
          "equities-industry-chart",
          "PMI Composite load failed.",
        );
        renderEquitiesIndexPlaceholder(
          "equities-top-right-chart",
          "Breadth load failed.",
        );
        setChartPlaceholder("equities-sigma-chart", "Sigma load failed.");
        updateStatValue(
          document.getElementById("equities-index-state"),
          "Load Failed",
        );
        updateStatValue(
          document.getElementById("equities-industry-state"),
          "Load Failed",
        );
        updateStatValue(
          document.getElementById("equities-top-right-state"),
          "Load Failed",
        );
      }
    }

    function safeSetText(id, value) {
      const el = document.getElementById(id);
      if (el) el.textContent = value;
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
          close: Number(row.close),
        }))
        .filter(
          (row) =>
            row.date &&
            Number.isFinite(row.open) &&
            Number.isFinite(row.high) &&
            Number.isFinite(row.low) &&
            Number.isFinite(row.close),
        )
        .sort((a, b) => a.date.localeCompare(b.date));

      if (!full.length) return null;

      const closes = full.map((d) => d.close);
      const sma150 = simpleMovingAverage(closes, 150);
      const ema20 = exponentialMovingAverage(closes, 20);

      const enriched = full.map((row, idx) => ({
        ...row,
        SMA_150: sma150[idx],
        EMA_20: ema20[idx],
      }));

      const visible = HORIZON_LENGTH[horizon] || 30;
      return enriched.slice(-visible);
    }


    function buildSpreadSeries(rows, spreadMode) {
      if (!Array.isArray(rows)) return [];

      if (spreadMode === "Bond Spread Lens") {
        return rows
          .map((r, i) => ({
            date: r.date,
            value: i === 0 ? 0 : Number(r.value) - Number(rows[i - 1].value),
          }))
          .filter((r) => r.date && Number.isFinite(r.value));
      }

      return rows
        .map((r) => ({
          date: r.date,
          value: Number(r.value),
        }))
        .filter((r) => r.date && Number.isFinite(r.value));
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
      XLY: "Consumer Discretionary",
    };

    function getEquitiesTickerLabel(symbol) {
      return (
        EQUITIES_TICKER_LABELS[String(symbol || "").toUpperCase()] ||
        String(symbol || "").toUpperCase()
      );
    }

    function formatEquitiesFocusLabel(symbol) {
      const normalizedSymbol = String(symbol || "").toUpperCase();
      const tickerLabel = getEquitiesTickerLabel(normalizedSymbol);

      return tickerLabel && tickerLabel !== normalizedSymbol
        ? `${normalizedSymbol} | ${tickerLabel}`
        : normalizedSymbol;
    }

    function getSpreadModeConfig(mode) {
      if (mode === "Bond Spread Lens") {
        return {
          badge: "Bond Spread Lens",
          modeLabel: "Î” 10Y Diff",
          subtitle: "Change in 10Y sovereign differential",
          transform: "delta",
        };
      }

      return {
        badge: "Comparative Spread",
        modeLabel: "Comparative",
        subtitle: "10Y sovereign comparative spread",
        transform: "level",
      };
    }

    function getEmbeddedSpreadRows(pair, horizon) {
      const store = activeDataStore.spreads || {};
      const symbol = DISPLAY_PAIR_TO_SYMBOL[pair];
      if (!symbol) return [];

      const raw =
        store[pair]?.rows ||
        store[pair] ||
        store[symbol]?.rows ||
        store[symbol] ||
        store[reverseSymbol(symbol)]?.rows ||
        store[reverseSymbol(symbol)] ||
        [];

      return raw
        .map((r) => ({
          date: r.date || r.as_of_date,
          value: Number(
            r.yld_10y_diff ?? r.spread ?? r.value ?? r.comparative_spread,
          ),
        }))
        .filter((r) => r.date && Number.isFinite(r.value))
        .sort((a, b) => String(a.date).localeCompare(String(b.date)))
        .slice(-(SPREAD_HORIZON_LENGTH[horizon] || 24));
    }



    function createSeriesLinePath(values, width, height, padding) {
      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;
      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = Math.max(max - min, 1e-9);

      const points = values.map((value, index) => {
        const x =
          padding.left + (index / Math.max(values.length - 1, 1)) * innerW;
        const y = padding.top + ((max - value) / range) * innerH;
        return { x, y, value };
      });

      const path = points
        .map(
          (p, i) => `${i === 0 ? "M" : "L"} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`,
        )
        .join(" ");
      return { points, path, min, max };
    }

    function renderPriceChart(container, rows, pair, viewMode = "system") {
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
          const y = padding.top + (idx / 2) * innerH;
          return `
            <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
            <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(v, getPairDigits(pair))}</text>
          `;
        })
        .join("");

      const candles = rows
        .map((row, idx) => {
          const x = padding.left + idx * candleStep + candleStep / 2;

          if (viewMode === "system") {
            return `
            <line
              class="fx-candle-up"
              x1="${x - bodyWidth / 2}"
              x2="${x + bodyWidth / 2}"
              y1="${priceToY(row.close)}"
              y2="${priceToY(row.close)}"
            ></line>
          `;
          }

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
        })
        .join("");

      const smaPoints = rows
        .map((row, idx) =>
          row.SMA_150 === null
            ? null
            : {
                x: padding.left + idx * candleStep + candleStep / 2,
                y: priceToY(row.SMA_150),
              },
        )
        .filter(Boolean);

      const emaPoints = rows
        .map((row, idx) =>
          row.EMA_20 === null
            ? null
            : {
                x: padding.left + idx * candleStep + candleStep / 2,
                y: priceToY(row.EMA_20),
              },
        )
        .filter(Boolean);

      const smaPath = createLinePath(smaPoints);
      const emaPath = createLinePath(emaPoints);

      const labelIdx = [0, Math.floor((rows.length - 1) / 2), rows.length - 1];
      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const x = padding.left + idx * candleStep + candleStep / 2;
          const label = formatDateLabelParts(rows[idx].date);

          return `
            <text class="fx-axis-label" x="${x}" y="${height - 18}" text-anchor="middle">
              <tspan x="${x}" dy="0">${label.top}</tspan>
              <tspan x="${x}" dy="12">${label.bottom}</tspan>
            </text>
          `;
        })
        .join("");

      container.innerHTML = `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX Price Candlestick Chart">
          ${grid}
          ${candles}
          ${smaPath ? `<path class="fx-sma-line" d="${smaPath}"></path>` : ""}
          ${emaPath ? `<path class="fx-ema-line" d="${emaPath}"></path>` : ""}
          ${xLabels}
        </svg>
      `;
    }

    function renderSpreadChart(container, rows) {
      if (!container || !rows || !rows.length) return;

      const values = rows.map((r) => Number(r.value)).filter(Number.isFinite);
      if (!values.length) {
        container.innerHTML = `<div class="panel-placeholder">No spread data available.</div>`;
        return;
      }

      const width = Math.max(container.clientWidth, 300);
      const height = Math.max(container.clientHeight, 230);
      const padding = { top: 18, right: 16, bottom: 32, left: 16 };

      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const min = Math.min(...values, 0);
      const max = Math.max(...values, 0);
      const range = Math.max(max - min, 1e-9);

      const valueToY = (value) => padding.top + ((max - value) / range) * innerH;
      const zeroY = valueToY(0);

      const barStep = innerW / Math.max(rows.length, 1);
      const barWidth = Math.max(Math.min(barStep * 0.62, 14), 4);

      const bars = rows
        .map((row, idx) => {
          const value = Number(row.value);
          if (!Number.isFinite(value)) return "";

          const xCenter = padding.left + idx * barStep + barStep / 2;
          const y = valueToY(value);
          const barY = Math.min(y, zeroY);
          const barH = Math.max(Math.abs(zeroY - y), 2);
          const fill = value >= 0 ? "#2dd4bf" : "#ef4444";
          const isLast = idx === rows.length - 1;
          const stroke = isLast ? "rgba(255,255,255,0.65)" : "none";
          const strokeWidth = isLast ? "1" : "0";

          return `
          <rect
            x="${xCenter - barWidth / 2}"
            y="${barY}"
            width="${barWidth}"
            height="${barH}"
            rx="2"
            fill="${fill}"
            stroke="${stroke}"
            stroke-width="${strokeWidth}"
          ></rect>
        `;
        })
        .join("");

      const labelIdx = [0, Math.floor((rows.length - 1) / 2), rows.length - 1];
      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const x = padding.left + idx * barStep + barStep / 2;
          return `<text class="fx-axis-label" x="${x}" y="${height - 8}" text-anchor="middle">${formatCompactDateLabel(rows[idx].date)}</text>`;
        })
        .join("");

      container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX Spread Bar Chart">
        <line class="fx-grid-line" x1="${padding.left}" y1="${padding.top}" x2="${width - padding.right}" y2="${padding.top}"></line>
        <line class="fx-zero-line" x1="${padding.left}" y1="${zeroY}" x2="${width - padding.right}" y2="${zeroY}"></line>
        <line class="fx-grid-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>

        <text class="fx-axis-label" x="${width - padding.right}" y="${padding.top - 6}" text-anchor="end">${formatNumber(max, 2)}</text>
        <text class="fx-axis-label" x="${width - padding.right}" y="${zeroY - 6}" text-anchor="end">0.00</text>
        <text class="fx-axis-label" x="${width - padding.right}" y="${height - padding.bottom - 6}" text-anchor="end">${formatNumber(min, 2)}</text>

        ${bars}
        ${xLabels}
      </svg>
    `;
    }

    function renderRatesYieldFamilyChart(container, rows, seriesDefs) {
      if (!container || !Array.isArray(rows) || !rows.length) return;

      const activeSeries = seriesDefs
        .map((series) => ({
          ...series,
          points: rows
            .map((row) => ({
              date: row.date || row.as_of_date,
              value: Number(row[series.field]),
            }))
            .filter((row) => row.date && Number.isFinite(row.value) && row.value > 0),
        }))
        .filter((series) => series.points.length >= 2);

      if (!activeSeries.length) {
        container.innerHTML = `<div class="panel-placeholder">No yield family data available.</div>`;
        return;
      }

      const width = Math.max(container.clientWidth, 300);
      const height = Math.max(container.clientHeight, 230);
      const padding = { top: 20, right: 18, bottom: 38, left: 18 };
      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const values = activeSeries.flatMap((series) =>
        series.points.map((point) => point.value),
      );

      if (!values.length) {
        container.innerHTML = `<div class="panel-placeholder">No positive yield family data available.</div>`;
        return;
      }
      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = Math.max(max - min, 1e-9);

      const dateRows = rows.filter((row) => row.date || row.as_of_date);
      const xStep = innerW / Math.max(dateRows.length - 1, 1);
      const dateIndex = new Map(
        dateRows.map((row, idx) => [String(row.date || row.as_of_date), idx]),
      );

      const valueToY = (value) => padding.top + ((max - value) / range) * innerH;
      const seriesPaths = activeSeries
        .map((series) => {
          const points = series.points
            .map((point) => {
              const idx = dateIndex.get(String(point.date));
              if (idx == null) return null;
              return {
                x: padding.left + idx * xStep,
                y: valueToY(point.value),
              };
            })
            .filter(Boolean);

          return `
            <path class="rates-yield-family-line" d="${createLinePath(points)}" style="stroke:${series.color}"></path>
          `;
        })
        .join("");

      const yTicks = [max, (max + min) / 2, min]
        .map((value, idx) => {
          const y = padding.top + (idx / 2) * innerH;
          return `
            <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
            <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(value, 2)}</text>
          `;
        })
        .join("");

      const labelIndexes = [0, Math.floor((dateRows.length - 1) / 2), dateRows.length - 1];
      const xLabels = [...new Set(labelIndexes)]
        .filter((idx) => dateRows[idx])
        .map((idx) => {
          const x = padding.left + idx * xStep;
          return `<text class="fx-axis-label" x="${x}" y="${height - 12}" text-anchor="middle">${formatCompactDateLabel(dateRows[idx].date || dateRows[idx].as_of_date)}</text>`;
        })
        .join("");

      const legend = activeSeries
        .map(
          (series, idx) => `
            <g transform="translate(${padding.left + idx * 54}, ${padding.top - 6})">
              <line x1="0" y1="0" x2="12" y2="0" class="rates-yield-family-line" style="stroke:${series.color}"></line>
              <text x="16" y="3" class="fx-axis-label">${series.label}</text>
            </g>
          `,
        )
        .join("");

      container.innerHTML = `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Rates Yield Family">
          ${yTicks}
          ${seriesPaths}
          ${legend}
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
      const barWidth =
        (innerW - barGap * Math.max(barCount - 1, 0)) / Math.max(barCount, 1);
      const maxAbs = Math.max(...rows.map((r) => Math.abs(r.z)), 0.1);
      const zeroY = padding.top + innerH / 2;

      const bars = rows
        .map((row, idx) => {
          const x = padding.left + idx * (barWidth + barGap);
          const scaled = (Math.abs(row.z) / maxAbs) * (innerH / 2 - 12);
          const isPositive = row.z >= 0;
          const y = isPositive ? zeroY - scaled : zeroY;
          const h = Math.max(scaled, 4);
          const selectedClass = row.pair === selectedPair ? "selected" : "";

          return `
          <rect class="fx-bar ${selectedClass}" x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="4"></rect>
          <text class="fx-bar-label" x="${x + barWidth / 2}" y="${height - 12}" text-anchor="middle">${row.pair}</text>
          <text class="fx-bar-value" x="${x + barWidth / 2}" y="${y - 6}" text-anchor="middle">${row.z.toFixed(2)}</text>
        `;
        })
        .join("");

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
      const barWidth =
        (innerW - barGap * Math.max(barCount - 1, 0)) / Math.max(barCount, 1);
      const maxAbs = Math.max(...rows.map((r) => Math.abs(r.z)), 0.1);
      const zeroY = padding.top + innerH / 2;

      const bars = rows
        .map((row, idx) => {
          const x = padding.left + idx * (barWidth + barGap);
          const scaled = (Math.abs(row.z) / maxAbs) * (innerH / 2 - 12);
          const isPositive = row.z >= 0;
          const y = isPositive ? zeroY - scaled : zeroY;
          const h = Math.max(scaled, 4);
          const selectedClass = row.pair === selectedKey ? "selected" : "";

          return `
        <rect class="fx-bar ${selectedClass}" x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="4"></rect>
        <text class="fx-bar-label" x="${x + barWidth / 2}" y="${height - 12}" text-anchor="middle">${row.pair}</text>
        <text class="fx-bar-value" x="${x + barWidth / 2}" y="${y - 6}" text-anchor="middle">${row.z.toFixed(2)}</text>
      `;
        })
        .join("");

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

    function renderFXDepth(pair) {
      const displayPair = pair || fxControls.pair?.value || "EUR/USD";

      const title = document.getElementById("fx-depth-title");
      const buttonRow = document.getElementById("fx-depth-metric-buttons");
      const chart = document.getElementById("eurusd-depth-chart");
      const description = document.getElementById(
        "fx-supporting-metrics-description",
      );

      if (!buttonRow || !chart) return;

      if (title) {
        title.textContent =
          FX_DEPTH_DESCRIPTIONS[displayPair] || `${displayPair} DEPTH`;
      }

      let metrics =
        FX_SUPPORTING_METRICS[displayPair] ||
        FX_SUPPORTING_METRICS["EUR/USD"] ||
        [];

      metrics = metrics.filter(
        (metric, index, self) =>
          index === self.findIndex((m) => m.name === metric.name),
      );

      function setActiveMetric(metric) {
        if (description) {
          description.textContent =
            metric.description ||
            "Select a supporting metric to confirm the future data pipe.";
        }

        buttonRow.querySelectorAll(".fx-depth-metric-button").forEach((btn) => {
          btn.classList.toggle("active", btn.dataset.metric === metric.name);
        });

        const horizon = fxControls.horizon?.value || "30D";

        let livePayload = getFXDepthMetricPayload(displayPair, metric.name);
        let liveRows = Array.isArray(livePayload?.rows) ? livePayload.rows : [];

        if (!liveRows.length && ["DE-US 2Y", "US-CA 2Y"].includes(metric.name)) {
          const spreadRows = getEmbeddedSpreadRows(displayPair, horizon) || [];

          liveRows = spreadRows.map((row, idx) => {
            const prior = spreadRows[idx - 1];
            const change = prior ? Number(row.value) - Number(prior.value) : 0;

            return {
              date: row.date,
              value: Number(row.value),
              change,
            };
          });

          livePayload = {
            metric: metric.name,
            source: "Source: the_Spine | FX Depth | Bond Spread Lens",
            rows: liveRows,
          };
        }

        if (liveRows.length) {
          const displayRows = getFXDepthHorizonRows(liveRows, metric.name);
          const validWtiRows = liveRows.filter(
            (row) => row?.date && Number.isFinite(Number(row.inventory_mmbbl)),
          );
          const latest =
            displayPair === "USD/CAD" && metric.name === "WTI Inv."
              ? validWtiRows[validWtiRows.length - 1]
              : displayRows[displayRows.length - 1] ||
                liveRows[liveRows.length - 1];

          const indexChange = shouldIndexFXDepthMetric(metric.name)
            ? getFXDepthIndexChange(displayRows)
            : null;

          if (liveRows.length || (displayPair === "USD/CAD" && metric.name === "WTI Inv.")) {
    const displayRows = getFXDepthHorizonRows(liveRows, metric.name);
    const latest = displayRows[displayRows.length - 1] || liveRows[liveRows.length - 1];

    const indexChange = shouldIndexFXDepthMetric(metric.name)
      ? getFXDepthIndexChange(displayRows)
      : null;

          if (displayPair === "USD/CAD" && metric.name === "WTI Inv.") {
    chart.innerHTML = `
      <div class="fx-depth-live-summary">
        <div class="fx-depth-live-title">WTI INVENTORY CYCLE</div>
        <div class="fx-depth-live-value">Current vs Historical Range</div>
        <div class="fx-depth-live-meta">Indexed to W#1 = 100</div>
      </div>
      <div class="fx-depth-live-chart" id="fx-depth-live-chart" style="height:280px;"></div>
    `;

    loadUSDCADWtiInventoryCycle().then((payload) => {
      const rows =
        Array.isArray(payload) ? payload :
        Array.isArray(payload?.rows) ? payload.rows :
        Array.isArray(payload?.data) ? payload.data :
        Array.isArray(payload?.series) ? payload.series :
        [];

      renderUSDCADWtiInventoryCycleChart(
        document.getElementById("fx-depth-live-chart"),
        rows
      );
    });

    return;
  }

          chart.innerHTML = `
            <div class="fx-depth-live-summary">
              <div class="fx-depth-live-title">LIVE SIGNAL</div>

              <div class="fx-depth-live-value ${Number(latest.value) >= 0 ? "positive" : "negative"}">
                ${formatFXDepthAbsolute(latest.value, 2)}
              </div>

              <div class="fx-depth-live-meta">
                ${latest.date} | ${
                  shouldIndexFXDepthMetric(metric.name)
                    ? `Indx @ 100 ${indexChange >= 0 ? "+" : ""}${formatNumber(indexChange, 1)}%`
                    : `DoD ${Number(latest.change) >= 0 ? "+" : ""}${formatNumber(latest.change || 0, 2)}`
                }
              </div>
            </div>

            <div class="fx-depth-live-chart" id="fx-depth-live-chart"></div>
          `;

          const sourceNode = document.getElementById("fx-depth-source");
          if (sourceNode) {
            sourceNode.textContent =
              livePayload?.source || "Source: the_Spine | FX Depth | Daily (UTC)";
          }

          const liveChartEl = document.getElementById("fx-depth-live-chart");
          if (liveChartEl) {
            renderFXDepthLiveLineChart(liveChartEl, liveRows, displayPair, metric.name);
          }
        } else {
          chart.innerHTML = `
            <div class="fx-depth-selected-metric">
              Future data pipe pending.
            </div>
          `;
        }
          } }

      buttonRow.innerHTML = metrics
        .map(
          (metric, index) => `
    <button
      type="button"
      class="fx-depth-metric-button ${index === 0 ? "active" : ""}"
      data-metric="${metric.name}">
      ${metric.name}
    </button>
  `,
        )
        .join("");

      buttonRow.querySelectorAll(".fx-depth-metric-button").forEach((button) => {
        const metric = metrics.find(
          (item) => item.name === button.dataset.metric,
        );

        button.addEventListener("click", () => {
          if (metric) setActiveMetric(metric);
        });
      });

      if (metrics.length) {
        setActiveMetric(metrics[0]);
      }
    }

    function formatFXDepthSignal(latest, metricName) {
      if (metricName === "FTSE vs. SPX") {
        const z = Number(latest.z_score ?? latest.value);
        const pct = Number(latest.percentile);

        return {
          value: `${z >= 0 ? "+" : ""}${formatNumber(z, 2)}?`,
          meta: Number.isFinite(pct)
            ? `Percentile: ${formatNumber(pct, 0)}th`
            : "Percentile: --",
          label: "RELATIVE STRENGTH",
        };
      }

      return {
        value: formatFXDepthAbsolute(latest.value, 2),
        meta: `${latest.date} | DoD ${Number(latest.change) >= 0 ? "+" : ""}${formatNumber(latest.change || 0, 2)}`,
        label: "LIVE SIGNAL",
      };
    }

    async function loadFXDepthData() {
      const payload = await fetchJsonWithBust(DATA_ENDPOINTS.fxDepth);

      fxDepthData = payload && typeof payload === "object" ? payload : {};

      return fxDepthData;
    }

    async function loadUSDCADWtiInventoryCycle() {
      return fetchJsonWithBust(DATA_ENDPOINTS.usdcadWtiInventoryCycle);
    }


    function renderSimpleDepthChart(container, rows, pair) {
      if (!container || !rows.length) return;

      const width = Math.max(container.clientWidth || 320, 320);
      const height = Math.max(container.clientHeight || 220, 220);
      const padding = { top: 20, right: 18, bottom: 30, left: 18 };

      const values = rows.map((r) => Number(r.value)).filter(Number.isFinite);
      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = Math.max(max - min, 1e-9);

      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const points = rows.map((row, idx) => ({
        x: padding.left + (idx / Math.max(rows.length - 1, 1)) * innerW,
        y: padding.top + ((max - Number(row.value)) / range) * innerH,
      }));

      container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
        <line class="fx-grid-line" x1="${padding.left}" y1="${padding.top}" x2="${width - padding.right}" y2="${padding.top}"></line>
        <line class="fx-grid-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>
        <path class="fx-line-secondary" d="${createLinePath(points)}"></path>
        <circle class="fx-point-last" cx="${points[points.length - 1].x}" cy="${points[points.length - 1].y}" r="4"></circle>
      </svg>
    `;
    }

    function getFXDepthHorizonRows(rows, metricName = "") {
      const horizon = fxControls.horizon?.value || "30D";
      const baseCount = HORIZON_LENGTH[horizon] || 30;

      const count = shouldIndexFXDepthMetric(metricName)
        ? baseCount + 15
        : baseCount;

      return Array.isArray(rows) ? rows.slice(-count) : [];
    }

  function renderUSDCADWtiInventoryCycleChart(container, rows) {
    if (!container || !Array.isArray(rows) || !rows.length) return;

    const data = rows
      .map((r) => ({
        week: Number(r["W#"] ?? r.week),
        min: Number(r.min),
        avg: Number(r.avg),
        max: Number(r.max),
        current: r.current == null ? null : Number(r.current),
      }))
      .filter((r) => Number.isFinite(r.week));

    const width = Math.max(container.clientWidth || 320, 320);
    const height = Math.max(container.clientHeight || 230, 230);
    const pad = { top: 24, right: 28, bottom: 34, left: 42 };

    const values = data
      .flatMap((d) => [d.min, d.avg, d.max, d.current])
      .filter(Number.isFinite);

    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);
    const range = Math.max(maxVal - minVal, 1e-9);

    const x = (week) =>
      pad.left + ((week - 1) / 52) * (width - pad.left - pad.right);

    const y = (value) =>
      pad.top + ((maxVal - value) / range) * (height - pad.top - pad.bottom);

    const path = (key) =>
      data
        .filter((d) => Number.isFinite(d[key]))
        .map((d, i) => `${i === 0 ? "M" : "L"} ${x(d.week)} ${y(d[key])}`)
        .join(" ");

    const x10 = x(10);

    container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
        <line class="fx-grid-line" x1="${pad.left}" y1="${pad.top}" x2="${width - pad.right}" y2="${pad.top}"></line>
        <line class="fx-grid-line" x1="${pad.left}" y1="${height - pad.bottom}" x2="${width - pad.right}" y2="${height - pad.bottom}"></line>

        <line x1="${x10}" x2="${x10}" y1="${pad.top}" y2="${height - pad.bottom}"
          stroke="orange" stroke-width="2" stroke-dasharray="5 4" opacity="0.8"></line>

        <path d="${path("min")}" fill="none" stroke="#16a34a" stroke-width="1.8" stroke-dasharray="5 4"></path>
        <path d="${path("avg")}" fill="none" stroke="#9ca3af" stroke-width="1.8" stroke-dasharray="6 4"></path>
        <path d="${path("max")}" fill="none" stroke="#dc2626" stroke-width="1.8" stroke-dasharray="5 4"></path>
        <path d="${path("current")}" fill="none" stroke="#ffffff" stroke-width="3"></path>

        <text class="fx-axis-label" x="${pad.left}" y="16">MIN / AVG / MAX / CURRENT</text>
        <text class="fx-axis-label" x="${x10 + 5}" y="${pad.top + 12}">W10</text>
        <text class="fx-axis-label" x="${width - pad.right}" y="${height - 10}" text-anchor="end">Week #</text>
      </svg>
    `;
  }

  function buildWTIInventoryIndex(rows) {
    const clean = rows
      .map((r) => ({
        date: new Date(r.date),
        year: new Date(r.date).getFullYear(),
        week: Number(r.week ?? r["W#"]),
        wklyInv: Number(r.wkly_inv ?? r.inventory),
      }))
      .filter((r) => Number.isFinite(r.week) && Number.isFinite(r.wklyInv));

    const badYears = new Set([1997, 2003, 2008, 2014]);
    const byYearStart = new Map();

    clean.forEach((r) => {
      if (r.week === 1 && !byYearStart.has(r.year)) {
        byYearStart.set(r.year, r.wklyInv);
      }
    });

    const indexed = clean
      .filter((r) => !badYears.has(r.year))
      .map((r) => {
        const start = byYearStart.get(r.year);
        if (!start) return null;

        const index = r.week === 1
          ? 100
          : Number((((r.wklyInv / start) - 1) * 100 + 100).toFixed(2));

        return { ...r, index };
      })
      .filter(Boolean);

    const currentYear = Math.max(...indexed.map((r) => r.year));

    const weeks = [...new Set(indexed.map((r) => r.week))]
      .filter((w) => w <= 52)
      .sort((a, b) => a - b);

    return weeks.map((week) => {
      const vals = indexed
        .filter((r) => r.week === week)
        .map((r) => r.index);

      const current = indexed.find(
        (r) => r.year === currentYear && r.week === week
      );

      return {
        week,
        min: Math.min(...vals),
        avg: vals.reduce((a, b) => a + b, 0) / vals.length,
        max: Math.max(...vals),
        current: current?.index ?? null,
          };
        });
      }

  function renderWTIInventoryIndexChart(containerId, rows) {
    const el = document.getElementById(containerId);
    if (!el) return;

    const data = buildWTIInventoryIndex(rows);

    const width = 720;
    const height = 300;
    const pad = { top: 24, right: 24, bottom: 34, left: 46 };

    const values = data.flatMap((d) =>
      [d.min, d.avg, d.max, d.current].filter(Number.isFinite)
    );

    const rawMin = Math.min(...values);
    const rawMax = Math.max(...values);
    const span = Math.max(rawMax - rawMin, 1e-9);
    const yMin = rawMin - span * 0.15;
    const yMax = rawMax + span * 0.15;

    const x = (week) =>
      pad.left + ((week - 1) / 51) * (width - pad.left - pad.right);

    const y = (value) =>
      height - pad.bottom -
      ((value - yMin) / (yMax - yMin)) * (height - pad.top - pad.bottom);

    const path = (key) =>
      data
        .filter((d) => Number.isFinite(d[key]))
        .map((d, i) => `${i === 0 ? "M" : "L"} ${x(d.week)} ${y(d[key])}`)
        .join(" ");

      el.innerHTML = `
        <svg viewBox="0 0 ${width} ${height}" class="wti-inv-index-svg">
          <path d="${path("min")}" class="wti-inv-line min"></path>
          <path d="${path("avg")}" class="wti-inv-line avg"></path>
          <path d="${path("max")}" class="wti-inv-line max"></path>
          <path d="${path("current")}" class="wti-inv-line current"></path>

          <text x="${pad.left}" y="18" class="wti-inv-title">
            WTI Weekly Inventory Index
          </text>
          <text x="${width - pad.right}" y="${height - 10}" text-anchor="end" class="wti-inv-axis">
            Week #
          </text>
        </svg>
        `;
      }



    function formatWtiInventoryDisplay(row) {
      const inv = Number(row?.inventory_mmbbl);

      if (row?.inventory_display) {
        return row.inventory_display;
      }

      if (!Number.isFinite(inv)) return "-- inv.";

      return `${inv.toFixed(1)}k inv.`;
    }

    function formatWtiStdDisplay(row) {
      const std = Number(row?.std_from_3yr_avg);
      if (!Number.isFinite(std)) return "-- std";

      return `${std >= 0 ? "+" : ""}${std.toFixed(1)} std`;
    }

    function getWtiInventoryDirection(row) {
      return row?.inventory_direction || "--";
    }

    function renderWtiInventorySeasonalChart(container, rows) {
      if (!container || !Array.isArray(rows) || !rows.length) return;

      const latest = rows
        .filter((r) => r.date && Number.isFinite(Number(r.value)))
        .slice(-1)[0];
      const latestYear = Number(String(latest.date).slice(0, 4));
      const profileByWeek = new Map();

      rows.forEach((row) => {
        const week = Number(row.week);
        if (!Number.isFinite(week)) return;

        profileByWeek.set(week, {
          week,
          high: Number(row.hist_max_index_wk1 ?? row.hist_max),
          avg: Number(row.hist_avg_index_wk1 ?? row.hist_avg),
          low: Number(row.hist_min_index_wk1 ?? row.hist_min),
        });
      });

      const currentRows = rows
        .filter((row) => Number(String(row.date).slice(0, 4)) === latestYear)
        .map((row) => ({
          week: Number(row.week),
          current: Number(row.value ?? row.inventory_mmbbl),
        }))
        .filter(
          (row) => Number.isFinite(row.week) && Number.isFinite(row.current),
        );

      const seasonalRows = Array.from({ length: 52 }, (_, i) => {
        const week = i + 1;
        const profile = profileByWeek.get(week) || {};
        const current = currentRows.find((row) => row.week === week);
        return {
          week,
          high: profile.high,
          avg: profile.avg,
          low: profile.low,
          current: current?.current,
        };
      });

      const values = seasonalRows
        .flatMap((r) => [r.high, r.avg, r.low, r.current])
        .filter(Number.isFinite);
      if (!values.length) return;

      const width = Math.max(container.clientWidth || 320, 320);
      const height = Math.max(container.clientHeight || 230, 230);
      const padding = { top: 18, right: 24, bottom: 34, left: 46 };

      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = Math.max(max - min, 1e-9);

      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const xForWeek = (week) => padding.left + ((week - 1) / 51) * innerW;
      const yForValue = (value) => padding.top + ((max - value) / range) * innerH;

      const makePath = (key) =>
        createLinePath(
          seasonalRows
            .filter((row) => Number.isFinite(row[key]))
            .map((row) => ({ x: xForWeek(row.week), y: yForValue(row[key]) })),
        );

      const yTicks = [max, (max + min) / 2, min]
        .map(
          (v) =>
            `<text class="fx-axis-label" x="6" y="${yForValue(v)}">${v.toFixed(1)}</text>`,
        )
        .join("");

      const labelWeeks = [1, 13, 26, 39, 52]
        .map(
          (week) =>
            `<text class="fx-axis-label" x="${xForWeek(week)}" y="${height - 10}" text-anchor="middle">${week}</text>`,
        )
        .join("");

      container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
        <line class="fx-grid-line" x1="${padding.left}" y1="${padding.top}" x2="${width - padding.right}" y2="${padding.top}"></line>
        <line class="fx-grid-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>
        ${yTicks}
        <path class="fx-depth-seasonal-line high" d="${makePath("high")}"></path>
        <path class="fx-depth-seasonal-line avg" d="${makePath("avg")}"></path>
        <path class="fx-depth-seasonal-line low" d="${makePath("low")}"></path>
        <path class="fx-depth-seasonal-line current" d="${makePath("current")}"></path>
        ${labelWeeks}
      </svg>
    `;
    }

    function renderFXDepthLiveLineChart(container, rows, pair, metricName) {
      if (!container || !Array.isArray(rows) || !rows.length) return;

      const shouldIndex = shouldIndexFXDepthMetric(metricName);

      const slicedRows = getFXDepthHorizonRows(rows, metricName)
        .map((row) => ({
          date: row.date,
          value: Number(row.value),
          change: Number(row.change),
        }))
        .filter((row) => row.date && Number.isFinite(row.value));

      const cleanRows = shouldIndex ? indexFXDepthRows(slicedRows) : slicedRows;
      if (!cleanRows.length) return;

      const width = Math.max(container.clientWidth || 320, 320);
      const height = Math.max(container.clientHeight || 230, 230);
      const padding = { top: 26, right: 34, bottom: 34, left: 18 };

      const values = cleanRows.map((row) => row.value);

      const rawMin = shouldIndex ? Math.min(...values, 100) : Math.min(...values, 0);
      const rawMax = shouldIndex ? Math.max(...values, 100) : Math.max(...values, 0);

      const rawRange = Math.max(rawMax - rawMin, 1e-9);
      const pad = Math.max(rawRange * 0.18, Math.abs(rawMax) * 0.035);

      const min = rawMin - pad;
      const max = rawMax + pad;
      const range = Math.max(max - min, 1e-9);

      const innerW = width - padding.left - padding.right;
      const innerH = height - padding.top - padding.bottom;

      const valueToY = (value) => padding.top + ((max - value) / range) * innerH;

      const points = cleanRows.map((row, idx) => ({
        x: padding.left + (idx / Math.max(cleanRows.length - 1, 1)) * innerW,
        y: valueToY(row.value),
      }));

      const latest = cleanRows[cleanRows.length - 1];
      const lastPoint = points[points.length - 1];
      const zeroValue = shouldIndex ? 100 : 0;
      const zeroY = valueToY(zeroValue);
      const fillClass = latest.value >= zeroValue ? "positive" : "negative";

      const yTicks = shouldIndex
        ? [max, (max + zeroValue) / 2, zeroValue, (min + zeroValue) / 2, min]
        : [max, (max + min) / 2, min];

      const yGrid = [...new Set(yTicks.map((v) => Number(v.toFixed(1))))]
        .map((value) => {
          const y = valueToY(value);
          return `
          <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
          <text class="fx-axis-label" x="${width - padding.right + 4}" y="${y + 3}" text-anchor="start">
            ${shouldIndex ? formatNumber(value, 1) : formatNumber(value, 2)}
          </text>
        `;
        })
        .join("");

      function buildAreaPath(points, values, zeroY, positive = true) {
        const pathPoints = points.map((pt, i) => {
          const value = values[i];

          if (
            (positive && value >= zeroValue) ||
            (!positive && value <= zeroValue)
          ) {
            return { x: pt.x, y: pt.y };
          }

          return { x: pt.x, y: zeroY };
        });

        return `
        M ${pathPoints[0].x.toFixed(2)} ${zeroY.toFixed(2)}
        ${pathPoints.map((p) => `L ${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(" ")}
        L ${pathPoints[pathPoints.length - 1].x.toFixed(2)} ${zeroY.toFixed(2)}
        Z
      `;
      }

      const positiveAreaPath = buildAreaPath(points, values, zeroY, true);
      const negativeAreaPath = buildAreaPath(points, values, zeroY, false);

      const labelCount = Math.min(8, cleanRows.length);
      const labelIdx = Array.from({ length: labelCount }, (_, i) =>
        Math.round((i / Math.max(labelCount - 1, 1)) * (cleanRows.length - 1)),
      );

      const xLabels = [...new Set(labelIdx)]
        .map((idx) => {
          const x =
            padding.left + (idx / Math.max(cleanRows.length - 1, 1)) * innerW;

          const d = new Date(cleanRows[idx].date);

          const monthDay = d.toLocaleDateString("en-US", {
            month: "short",
            day: "numeric",
          });

          const year =
            "'" +
            d.toLocaleDateString("en-US", {
              year: "2-digit",
            });

          return `
        <text class="fx-axis-label" x="${x}" y="${height - 24}" text-anchor="middle">
          <tspan x="${x}" dy="0">${monthDay}</tspan>
          <tspan x="${x}" dy="12">${year}</tspan>
        </text>
      `;
        })
        .join("");

      container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${pair} ${metricName}">
        ${yGrid}

        <line class="fx-zero-line" x1="${padding.left}" y1="${zeroY}" x2="${width - padding.right}" y2="${zeroY}"></line>

        <path class="fx-depth-live-area-positive" d="${positiveAreaPath}"></path>
        <path class="fx-depth-live-area-negative" d="${negativeAreaPath}"></path>

        <path class="fx-depth-live-line ${fillClass}" d="${createLinePath(points)}"></path>

        <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4"></circle>

        ${xLabels}
      </svg>
    `;
    }

    const FX_SUPPORTING_METRICS = {
      "EUR/USD": [
        {
          name: "DE-US 2Y",
          description:
            "Measures near-term ECB vs. Fed policy divergence driving EUR/USD.",
        },
        {
          name: "Energy Tax",
          description:
            "Measures Europe's relative energy burden versus the United States.",
        },
        {
          name: "Bank Ratio",
          description:
            "Measures capital allocation between European and U.S. banking systems.",
        },
      ],

      "AUD/USD": [
        {
          name: "Iron Ore",
          description:
            "Measures demand for Australia's most important export commodity.",
        },
        {
          name: "AU-US 2Y",
          description:
            "Measures monetary policy divergence between Australia and the United States.",
        },
        {
          name: "Copper/Gold",
          description:
            "Measures global risk appetite through growth versus safety demand.",
        },
      ],

      "GBP/USD": [
        {
          name: "UK-US 2Y",
          description:
            "Measures monetary policy divergence between the Bank of England and Fed.",
        },
        {
          name: "FTSE vs. SPX",
          description:
            "Measures relative capital rotation between UK and U.S. equities.",
        },
        {
          name: "Econ Surprise",
          description:
            "Measures whether UK economic data is outperforming U.S. expectations.",
        },
      ],

      "USD/CAD": [
        {
          name: "WTI Inv.",
          description:
            "Measures physical crude oil supply pressure through U.S. inventory changes.",
        },
        {
          name: "US-CA 2Y",
          description:
            "Measures policy divergence between Canada and the United States.",
        },
        {
          name: "WTI vs. NatGas",
          description:
            "Measures relative energy market leadership across key North American fuels.",
        },
      ],

      "USD/CHF": [
        {
          name: "XAU/EUR",
          description:
            "Measures demand for monetary safety within the European region.",
        },
        {
          name: "VIX",
          description:
            "Measures global market stress and demand for defensive assets.",
        },
        {
          name: "Eurozone Stress",
          description: "Measures systemic financial stress across the Eurozone.",
        },
      ],

      "USD/JPY": [
        {
          name: "US 2Y",
          description:
            "Measures U.S. short-term yield pressure driving carry trade dynamics.",
        },
        {
          name: "Brent Crude",
          description:
            "Measures energy cost pressure on Japan's import-dependent economy.",
        },
        {
          name: "BCOM vs. Nikkei",
          description:
            "Measures commodity pressure relative to Japanese equity performance.",
        },
      ],
    };

    async function renderFX() {
      updateGeoScenToolbarLabel();
      renderFXVector();

      const pair = fxControls.pair?.value || "EUR/USD";

      await loadFXDepthData();
      renderFXDepth(pair);
      await renderFxOracleChamber();

      if (!activeDataLoaded) {
        setChartPlaceholder(
          "fx-price-chart",
          activeDataLoadError ? "FX price fetch failed." : "Loading FX price...",
        );
        setChartPlaceholder(
          "fx-spread-chart",
          activeDataLoadError ? "Spread fetch failed." : "Loading spreads...",
        );
        setChartPlaceholder(
          "fx-sigma-chart",
          activeDataLoadError ? "Sigma fetch failed." : "Loading sigma...",
        );
        return;
      }

      const spreadMode = fxControls.spreads?.value || "Comparative Spread";
      const horizon = fxControls.horizon?.value || "30D";
      const viewMode = fxControls.viewMode?.value || "system";

      const priceRows = getEmbeddedPriceRows(pair, horizon) || [];
      const spreadRows = getEmbeddedSpreadRows(pair, horizon) || [];
      const spreadSeries = buildSpreadSeries(spreadRows, spreadMode);
      const spreadConfig = getSpreadModeConfig(spreadMode);
      const sigmaRows = getHarmonizedSigmaRows();
      const sigmaRow = sigmaRows.find((row) => row.pair === pair);

      const priceDateEl = document.getElementById("fx-price-date");
      if (priceDateEl) {
        priceDateEl.textContent = priceRows.length
          ? formatUTC(priceRows[priceRows.length - 1].date)
          : "--";
      }

      const spreadDateEl = document.getElementById("fx-spread-date");
      if (spreadDateEl) {
        spreadDateEl.textContent = spreadRows.length
          ? formatUTC(spreadRows[spreadRows.length - 1].date)
          : "--";
      }

      const sigmaDateEl = document.getElementById("fx-sigma-date");
      if (sigmaDateEl) {
        sigmaDateEl.textContent = sigmaRow?.as_of_date
          ? formatUTC(sigmaRow.as_of_date)
          : "--";
      }

      const priceBadge = document.getElementById("fx-price-badge");
      const spreadBadge = document.getElementById("fx-spread-badge");
      const spreadSubtitle = document.getElementById("fx-spread-subtitle");
      const sigmaBadge = document.getElementById("fx-sigma-badge");

      if (priceBadge) priceBadge.textContent = `${pair} ? ${horizon}`;
      if (spreadBadge) spreadBadge.textContent = spreadConfig.badge;
      if (spreadSubtitle) spreadSubtitle.textContent = spreadConfig.subtitle;
      if (sigmaBadge) sigmaBadge.textContent = "Cross-Pair Z Context";

      if (!priceRows.length) {
        updateStatValue(document.getElementById("fx-price-last"), "--");
        updateStatValue(document.getElementById("fx-price-change"), "--");
        updateStatValue(document.getElementById("fx-price-range"), "--");
        setChartPlaceholder(
          "fx-price-chart",
          endpointHealth.price
            ? `No active FX price data for ${pair}.`
            : "Price endpoint unavailable.",
        );
      } else {
        const priceLast = priceRows[priceRows.length - 1].close;
        const priceFirst = priceRows[0].close;
        const priceChangePct = (priceLast / priceFirst - 1) * 100;
        const priceMin = Math.min(...priceRows.map((d) => d.low));
        const priceMax = Math.max(...priceRows.map((d) => d.high));
        const priceDigits = getPairDigits(pair);

        updateStatValue(
          document.getElementById("fx-price-last"),
          formatNumber(priceLast, priceDigits),
        );
        updateStatValue(
          document.getElementById("fx-price-change"),
          `${priceChangePct >= 0 ? "+" : ""}${formatNumber(priceChangePct, 2)}%`,
          priceChangePct >= 0 ? "positive" : "negative",
        );
        updateStatValue(
          document.getElementById("fx-price-range"),
          `${formatNumber(priceMin, priceDigits)} - ${formatNumber(priceMax, priceDigits)}`,
        );

        renderPriceChart(
          document.getElementById("fx-price-chart"),
          priceRows,
          pair,
          viewMode,
        );
      }

      if (!spreadSeries.length) {
        updateStatValue(document.getElementById("fx-spread-mode"), "No Data");
        updateStatValue(document.getElementById("fx-spread-last"), "--");
        updateStatValue(document.getElementById("fx-spread-avg"), "--");
        setChartPlaceholder(
          "fx-spread-chart",
          endpointHealth.spreads
            ? `No spread data for ${pair}.`
            : "Spread endpoint unavailable.",
        );
      } else {
        const spreadValues = spreadSeries.map((d) => d.value);
        const spreadLast = spreadValues[spreadValues.length - 1];
        const spreadAvg =
          spreadValues.reduce((a, b) => a + b, 0) / spreadValues.length;

        updateStatValue(
          document.getElementById("fx-spread-mode"),
          spreadConfig.modeLabel,
        );
        updateStatValue(
          document.getElementById("fx-spread-last"),
          `${spreadLast >= 0 ? "+" : ""}${formatNumber(spreadLast, 2)}`,
          spreadLast >= 0 ? "positive" : "negative",
        );
        updateStatValue(
          document.getElementById("fx-spread-avg"),
          `${spreadAvg >= 0 ? "+" : ""}${formatNumber(spreadAvg, 2)}`,
          spreadAvg >= 0 ? "positive" : "negative",
        );
        renderSpreadChart(
          document.getElementById("fx-spread-chart"),
          spreadSeries,
        );
      }

      updateStatValue(document.getElementById("fx-sigma-selected"), pair);
      updateStatValue(
        document.getElementById("fx-sigma-z"),
        sigmaRow ? `${sigmaRow.z >= 0 ? "+" : ""}${sigmaRow.z.toFixed(2)}` : "--",
        sigmaRow ? (sigmaRow.z >= 0 ? "positive" : "negative") : null,
      );
      updateStatValue(
        document.getElementById("fx-sigma-rank"),
        sigmaRow ? `${sigmaRow.rank}/${sigmaRows.length}` : "--",
      );

      renderSigmaChart(
        document.getElementById("fx-sigma-chart"),
        sigmaRows,
        pair,
      );
    }

    fxControls.pair?.addEventListener("change", () => {
      renderFX();
    });
    fxControls.spreads?.addEventListener("change", renderFX);
    fxControls.horizon?.addEventListener("change", renderFX);
    fxControls.viewMode?.addEventListener("change", renderFX);

    fxControls.horizon?.addEventListener("change", renderFX);

  cflowControls.domain?.addEventListener("change", () => {
    updateCFlowDropdowns({ resetSubsystem: true, resetMetric: true });
    renderCFlow();
  });

  const CFLOW_HORIZON_DAYS = {
    "30D": 30,
    "90D": 90,
    "1Y": 365,
    MAX: Infinity,
  };

  function getCFlowMetricRows(payload) {
    const dateKeys = [
      "date",
      "as_of_date",
      "timestamp",
      "period",
      "observation_date",
    ];

    const valueKeys = [
      "value",
      "score",
      "close",
      "level",
      "index",
      "metric_value",
      "cflow_score",
      "transmission_score",
      "systemicity_proxy",
    ];

    function normalizeRows(rows) {
      if (!Array.isArray(rows)) return [];

      return rows
        .map((row) => {
          if (!row || typeof row !== "object") return null;

          const dateKey = dateKeys.find((key) => row[key] != null);
          const date = String(dateKey ? row[dateKey] : "").trim();

          const preferredValueKey = valueKeys.find((key) =>
            Number.isFinite(Number(row?.[key])),
          );

          const fallbackValueKey = Object.keys(row).find(
            (key) =>
              !dateKeys.includes(key) &&
              Number.isFinite(Number(row[key])),
          );

          const valueKey = preferredValueKey || fallbackValueKey;
          const value = valueKey ? Number(row[valueKey]) : NaN;

          return { date, value };
        })
        .filter((row) => row?.date && Number.isFinite(row.value))
        .sort((a, b) => String(a.date).localeCompare(String(b.date)));
    }

    const rowSets = [
      payload?.history,
      payload?.rows,
      payload?.series,
      payload?.data,
      payload?.observations,
      payload?.values,
      Array.isArray(payload) ? payload : null,
    ];

    for (const candidate of rowSets) {
      const normalized = normalizeRows(candidate);
      if (normalized.length) return normalized;
    }

    const stack = [payload];
    const seen = new Set();

    while (stack.length) {
      const current = stack.pop();
      if (!current || typeof current !== "object" || seen.has(current)) continue;
      seen.add(current);

      if (Array.isArray(current)) {
        const normalized = normalizeRows(current);
        if (normalized.length) return normalized;
        current.forEach((item) => stack.push(item));
        continue;
      }

      Object.values(current).forEach((value) => {
        if (value && typeof value === "object") stack.push(value);
      });
    }

    return [];
  }

  function filterCFlowRowsByHorizon(rows, horizon) {
    if (!Array.isArray(rows) || !rows.length) return [];

    const days = CFLOW_HORIZON_DAYS[horizon] ?? Infinity;
    if (!Number.isFinite(days)) return rows;

    const latest = new Date(`${rows[rows.length - 1].date}T00:00:00`);
    if (Number.isNaN(latest.getTime())) return rows.slice(-Math.max(days, 2));

    const cutoff = new Date(latest);
    cutoff.setDate(cutoff.getDate() - days);

    const scopedRows = rows.filter((row) => {
      const d = new Date(`${row.date}T00:00:00`);
      return !Number.isNaN(d.getTime()) && d >= cutoff;
    });

    return scopedRows.length >= 2 ? scopedRows : rows.slice(-2);
  }

  function cleanCFlowDisplayText(value) {
    return String(value ?? "")
      .replace(/\bTransmission\b\s*/gi, "")
      .replace(/\s{2,}/g, " ")
      .trim();
  }

  function getCFlowSourceLabel() {
    return cleanCFlowDisplayText(
      cflowControls.subsystem?.selectedOptions?.[0]?.textContent || "C-FLOW",
    );
  }

  function installCFlowPlotlyHoverDelay(container) {
    if (!container || !window.Plotly) return;

    if (container.__cflowHoverDelayHandlers) {
      const previous = container.__cflowHoverDelayHandlers;
      container.removeEventListener("mouseenter", previous.onEnter);
      container.removeEventListener("mousemove", previous.onMove);
      container.removeEventListener("mouseleave", previous.onLeave);
    }

    let hoverTimer = null;
    let hoverEnabled = false;

    const clearHoverTimer = () => {
      if (hoverTimer) {
        clearTimeout(hoverTimer);
        hoverTimer = null;
      }
    };

    const enableHover = () => {
      hoverTimer = null;
      hoverEnabled = true;
      window.Plotly.relayout(container, { hovermode: "x unified" });
    };

    const queueHover = () => {
      if (hoverEnabled || hoverTimer) return;
      hoverTimer = setTimeout(enableHover, 2000);
    };

    const disableHover = () => {
      clearHoverTimer();
      hoverEnabled = false;
      window.Plotly.Fx?.unhover(container);
      window.Plotly.relayout(container, { hovermode: false });
    };

    container.addEventListener("mouseenter", queueHover);
    container.addEventListener("mousemove", queueHover);
    container.addEventListener("mouseleave", disableHover);
    container.__cflowHoverDelayHandlers = {
      onEnter: queueHover,
      onMove: queueHover,
      onLeave: disableHover,
    };
  }

  function isCFlowCompositeMetric(metricKey) {
    return String(metricKey || "").includes("composite");
  }

  function renderCFlowMetricChart(container, rows, labelText) {
    if (!container) return;

    if (!rows.length) {
      container.innerHTML = `<div class="panel-placeholder">No history available for this metric.</div>`;
      return;
    }

    const width = Math.max(container.clientWidth, 300);
    const height = Math.max(container.clientHeight, 220);
    const padding = { top: 20, right: 18, bottom: 36, left: 18 };
    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;

    const values = rows.map((row) => row.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = Math.max(max - min, 1e-9);
    const step = innerW / Math.max(rows.length - 1, 1);

    const valueToY = (value) => padding.top + ((max - value) / range) * innerH;

    const points = rows.map((row, idx) => ({
      x: padding.left + idx * step,
      y: valueToY(row.value),
    }));

    const yTicks = [max, (max + min) / 2, min]
      .map((value, idx) => {
        const y = padding.top + (idx / 2) * innerH;
        return `
          <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
          <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(value, 2)}</text>
        `;
      })
      .join("");

    const labelIndexes = [0, Math.floor((rows.length - 1) / 2), rows.length - 1];
    const xLabels = [...new Set(labelIndexes)]
      .map((idx) => {
        const x = padding.left + idx * step;
        const label = formatDateLabelParts(rows[idx].date);

        return `
          <text class="fx-axis-label" x="${x}" y="${height - 20}" text-anchor="middle">
            <tspan x="${x}" dy="0">${label.top}</tspan>
            <tspan x="${x}" dy="12">${label.bottom}</tspan>
          </text>
        `;
      })
      .join("");

    const lastPoint = points[points.length - 1];

    container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${labelText}">
        ${yTicks}
        <path class="fx-line-secondary cflow-metric-line" d="${createLinePath(points)}"></path>
        <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
        ${xLabels}
      </svg>
    `;
  }

  function renderCFlowPlotlyChart(container, rows, labelText) {
    if (!container) return;

    if (!rows.length) {
      container.innerHTML = `<div class="panel-placeholder">No history available for this metric.</div>`;
      return;
    }

    if (!window.Plotly) {
      renderCFlowMetricChart(container, rows, labelText);
      return;
    }

    container.innerHTML = "";
    const width = Math.max(container.clientWidth || container.parentElement?.clientWidth || 320, 320);
    const height = Math.max(container.clientHeight || 260, 250);

    const x = rows.map((row) => row.date);
    const y = rows.map((row) => row.value);
    const xRange = x.length > 1 ? [x[0], x[x.length - 1]] : null;
    const finiteY = y.filter(Number.isFinite);
    const yMin = finiteY.length ? Math.min(...finiteY) : 0;
    const yMax = finiteY.length ? Math.max(...finiteY) : 1;
    const ySpan = Math.max(yMax - yMin, Math.abs(yMax) * 0.08, 1e-6);
    const yPad = ySpan * 0.12;
    const yRange = [yMin - yPad, yMax + yPad];

    const trace = {
      type: "scatter",
      mode: "lines",
      x,
      y,
      line: {
        color: "#4A90E2",
        dash: "solid",
        shape: "spline",
        smoothing: 0.45,
        width: 2.75,
      },
      hovertemplate: "%{x}<br>%{y:.3f}<extra></extra>",
    };

    const layout = {
      autosize: true,
      width,
      height,
      margin: { t: 16, r: 18, b: 36, l: 42 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: {
        color: "#8f9aac",
        family: "Inter, sans-serif",
        size: 10,
      },
      xaxis: {
        autorange: !xRange,
        range: xRange || undefined,
        fixedrange: false,
        gridcolor: "rgba(126,153,196,0.12)",
        linecolor: "rgba(126,153,196,0.18)",
        showspikes: true,
        spikecolor: "#D4AF37",
        spikedash: "solid",
        spikemode: "across",
        spikesnap: "cursor",
        spikethickness: 1.5,
        tickfont: { color: "#8f9aac", size: 10 },
        zeroline: false,
      },
      yaxis: {
        autorange: false,
        fixedrange: true,
        range: yRange,
        gridcolor: "rgba(126,153,196,0.12)",
        linecolor: "rgba(126,153,196,0.18)",
        tickfont: { color: "#8f9aac", size: 10 },
        showspikes: false,
        zeroline: false,
      },
      dragmode: "zoom",
      showlegend: false,
      hovermode: false,
      hoverdistance: -1,
      spikedistance: -1,
      hoverlabel: {
        bgcolor: "#F7F8FA",
        bordercolor: "#D4AF37",
        font: {
          color: "#111827",
          family: "Inter, sans-serif",
          size: 11,
        },
      },
    };

    const config = {
      displayModeBar: false,
      responsive: true,
    };

    window.Plotly.newPlot(container, [trace], layout, config).then(() => {
      window.Plotly.Plots.resize(container);
      installCFlowPlotlyHoverDelay(container);
    });
  }

  async function renderCFlow() {
    updateCFlowDropdowns();
    await renderCFlowVector();
    await renderCflowOracleChamber();

    const metricKey = cflowControls.metric?.value;
    const horizon = cflowControls.horizon?.value || "MAX";
    const summaryBody = document.getElementById("cflow-regime-summary");
    summaryBody?.classList.remove("cflow-chart-host");

    if (metricKey === "cflow-regime-engine") {
      await renderCflowRegimeEngine();
      return;
    }

    if (metricKey === "cflow-regime-definitions") {
      await renderCflowRegimeDefinitions();
      return;
    }

    if (
      metricKey === "cflow-completion-ledger"
    ) {
      await renderCflowCompletionLedger();
      return;
    }

    const endpoint = DATA_ENDPOINTS.cflow?.[metricKey];

    const title = document.querySelector(".cflow-quality-panel .panel-title");
    const body = document.querySelector(".cflow-quality-panel .panel-placeholder");
    const note = document.getElementById("cflow-regime-note");

    if (!endpoint) {
      if (title) title.textContent = "C?FLOW";
      if (body) body.innerHTML = "No endpoint registered.";
      return;
    }

    const payload = await fetchJsonWithBust(endpoint);
    const meta = payload.meta || {};
    const latest = payload.latest || {};
    const primary =
      latest.transmission_score ??
      latest.systemicity_proxy ??
      latest.value ??
      latest.score ??
      "--";
    const formattedPrimary = Number.isFinite(Number(primary))
      ? formatNumber(primary, 3)
      : primary;

    const metricRowsRaw = getCFlowMetricRows(payload);
    const latestDate = latest.date || meta.as_of_date || meta.latest_date || "";
    const primaryNumber = Number(primary);
    const metricRows = filterCFlowRowsByHorizon(
      metricRowsRaw.length
        ? metricRowsRaw
        : latestDate && Number.isFinite(primaryNumber)
          ? [{ date: latestDate, value: primaryNumber }]
          : [],
      horizon,
    );

    const sourceLabel = getCFlowSourceLabel();
    const displayName = cleanCFlowDisplayText(meta.name || metricKey);

    if (title) {
      title.textContent = displayName;
    }

    setCFlowLatestHeader(formattedPrimary);
    if (note) {
      note.textContent = `Source: ${sourceLabel} | ${latestDate || "Latest available"}`;
    }

    if (body) {
      body.classList.add("cflow-chart-host");
      body.innerHTML = `
        <div class="cflow-metric-chart fx-chart"></div>
      `;

      renderCFlowMetricChart(
        body.querySelector(".cflow-metric-chart"),
        metricRows,
        displayName,
      );
    }
  }




    cflowControls.subsystem?.addEventListener("change", () => {
      updateCFlowDropdowns();
      renderCFlow();
    });

    cflowControls.metric?.addEventListener("change", renderCFlow);
    cflowControls.lens?.addEventListener("change", renderCFlow);
    cflowControls.horizon?.addEventListener("change", renderCFlow);
    cflowControls.geoscen?.addEventListener("change", renderCFlow);

    function renderWtiOcSignal(container, overlay, meta) {
      if (!container) return;

      const z = Number(overlay?.z);
      const state = String(overlay?.state || "neutral").toUpperCase();
      const asOfDate = meta?.as_of_date || "--";
      const source = meta?.source || "EIA";

      container.innerHTML = `
      <div class="panel-placeholder">
        <div>
          <div style="font-size: 0.72rem; letter-spacing: 0.12em; text-transform: uppercase; color: #7c5cff;">
            OC Overlay Active
          </div>
          <div style="margin-top: 12px; font-size: 1.8rem; font-weight: 700; color: #eef3fa;">
            Z: ${Number.isFinite(z) ? z.toFixed(2) : "--"}
          </div>
          <div style="margin-top: 8px; font-size: 1rem; color: #d9e2ef;">
            State: ${state}
          </div>
          <div style="margin-top: 8px; font-size: 0.78rem; color: #7f8ea3;">
            ${source} | ${asOfDate}
          </div>
        </div>
      </div>
    `;
    }

    async function renderWTI() {
      updateWTIGeoScenToolbarLabel();

      const HORIZON_LENGTH_WTI = {
        "5D": 5,
        "15D": 15,
        "30D": 30,
        "45D": 45,
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
          regime: document.getElementById("wti-inventory-regime"),
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

        const valueToY = (value) =>
          padding.top + ((max - value) / range) * innerH;
        const step = innerW / Math.max(rows.length - 1, 1);

        const points = rows.map((row, idx) => ({
          x: padding.left + idx * step,
          y: valueToY(row.close),
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
            const label = formatDateLabelParts(rows[idx].date);

            return `
              <text class="fx-axis-label" x="${x}" y="${height - 18}" text-anchor="middle">
                <tspan x="${x}" dy="0">${label.top}</tspan>
                <tspan x="${x}" dy="12">${label.bottom}</tspan>
              </text>
            `;
          })
          .join("");

        const lastPoint = points[points.length - 1];

        container.innerHTML = `
          <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${labelText}">
            ${yTicks}
            <path class="fx-line-secondary" d="${path}"></path>
            <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
            ${xLabels}
          </svg>
        `;
      }

      const inventorySourceEl = document.getElementById("wti-inventory-source");
      const inventorySourcePrefixEl = document.getElementById(
        "wti-inventory-source-prefix",
      );
      const inventorySourcePrefix =
        inventorySourceEl?.getAttribute("data-source-prefix") ||
        "Source: EIA | the_Spine | As of ";

      if (inventorySourcePrefixEl) {
        inventorySourcePrefixEl.textContent = inventorySourcePrefix;
      }

      const rows = payload.rows || [];

      const historicalRows = rows.filter(d =>
        d.min != null &&
        d.avg != null &&
        d.max != null
      );

      const currentRows = rows.filter(d =>
        d.current != null &&
        !Number.isNaN(Number(d.current))
      );

      drawLine(historicalRows, "W#", "min", "green");
      drawLine(historicalRows, "W#", "avg", "grey");
      drawLine(historicalRows, "W#", "max", "red");

      drawLine(currentRows, "W#", "current", "black");
      drawVerticalLine(10, "orange");



      const inventoryDate = document.getElementById("wti-inventory-date");
      const priceDate = document.getElementById("wti-price-date");
      const macroDate = document.getElementById("wti-macro-date");
      const sigmaDate = document.getElementById("wti-sigma-date");

      const inventoryBadge = document.getElementById("wti-inventory-badge");
      const priceBadge = document.getElementById("wti-price-badge");
      const macroBadge = document.getElementById("wti-macro-badge");
      const sigmaBadge = document.getElementById("wti-sigma-badge");

      if (inventoryBadge) inventoryBadge.textContent = horizon;
      if (priceBadge) priceBadge.textContent = `WTI ? ${horizon}`;
      if (macroBadge) macroBadge.textContent = "Pressure / Macro";
      if (sigmaBadge) sigmaBadge.textContent = "Cross-Series Z Context";

      ensureWtiInventoryControls();

      const selectedOcOverlay = String(
        wtiControls.ocOverlay?.value || "off",
      ).toLowerCase();
      const ocOverlayOn = selectedOcOverlay === "on";

      const inventoryPanelEl = document.querySelector(".wti-inventory-panel");
      if (inventoryPanelEl) {
        inventoryPanelEl.classList.toggle("wti-oc-active", ocOverlayOn);
      }

      if (inventorySourceEl) {
        inventorySourceEl.setAttribute(
          "data-source-prefix",
          ocOverlayOn
            ? "Source: EIA | the_Spine | 15Y Seasonal Overlay | As of "
            : "Source: EIA | the_Spine | As of ",
        );
      }

      try {
        const panel = await loadWtiPanel();
        const priceRowsFromPanel = Array.isArray(panel.price) ? panel.price : [];
        const inventoryRowsFromPanel = Array.isArray(panel.inventory)
          ? panel.inventory
          : [];

        const rows = inventoryRowsFromPanel.map((row) => ({
          date: row.date,
          inventory_mmbbl: Number(row.value),
        }));

        const summary = {
          inventory: {
            history_end: panel.summary?.inventory_as_of || panel.as_of_date,
            last_inventory_mmbbl: rows.length
              ? rows[rows.length - 1].inventory_mmbbl
              : null,
            min_inventory_mmbbl: rows.length
              ? Math.min(
                  ...rows.map((r) => r.inventory_mmbbl).filter(Number.isFinite),
                )
              : null,
            max_inventory_mmbbl: rows.length
              ? Math.max(
                  ...rows.map((r) => r.inventory_mmbbl).filter(Number.isFinite),
                )
              : null,
          },
          inflation_pressure: {},
          sigma_rank: {},
        };

        const inventorySummary = summary.inventory || {};
        const inflationSummary = summary.inflation_pressure || {};
        const sigmaSummary = summary.sigma_rank || {};

        const visible = rows.slice(-(HORIZON_LENGTH_WTI[horizon] || 30));
        const latestRow = rows.length ? rows[rows.length - 1] : null;

        if (latestRow && inventoryDate) {
          inventoryDate.textContent = formatUTC(latestRow.date);
        }

        updateStatValue(
          document.getElementById("wti-inventory-last"),
          formatNumber(inventorySummary.last_inventory_mmbbl, 2),
        );

        const inventoryChange =
          rows.length >= 2 &&
          Number.isFinite(Number(rows[rows.length - 1]?.inventory_mmbbl)) &&
          Number.isFinite(Number(rows[rows.length - 2]?.inventory_mmbbl))
            ? Number(rows[rows.length - 1].inventory_mmbbl) -
              Number(rows[rows.length - 2].inventory_mmbbl)
            : null;

        updateStatValue(
          document.getElementById("wti-inventory-change"),
          inventoryChange == null
            ? "--"
            : `${inventoryChange >= 0 ? "+" : ""}${formatNumber(inventoryChange, 2)}`,
          inventoryChange == null
            ? null
            : inventoryChange >= 0
              ? "positive"
              : "negative",
        );

        updateStatValue(
          document.getElementById("wti-inventory-range"),
          inventorySummary.min_inventory_mmbbl != null &&
            inventorySummary.max_inventory_mmbbl != null
            ? `${formatNumber(inventorySummary.min_inventory_mmbbl, 2)} - ${formatNumber(inventorySummary.max_inventory_mmbbl, 2)}`
            : "--",
        );

        updateStatValue(
          document.getElementById("wti-macro-cpi"),
          formatNumber(inflationSummary.last_inflation_pressure_z, 2),
          Number(inflationSummary.last_inflation_pressure_z) >= 0
            ? "positive"
            : "negative",
        );
        updateStatValue(
          document.getElementById("wti-macro-trend"),
          inflationSummary.components_used || "--",
        );
        updateStatValue(
          document.getElementById("wti-macro-state"),
          inflationSummary.last_state || "--",
        );

        updateStatValue(document.getElementById("wti-sigma-selected"), "WTI");
        updateStatValue(
          document.getElementById("wti-sigma-z"),
          formatNumber(sigmaSummary.last_sigma_value, 2),
          Number(sigmaSummary.last_sigma_value) >= 0 ? "positive" : "negative",
        );
        updateStatValue(
          document.getElementById("wti-sigma-rank"),
          sigmaSummary.last_sigma_rank != null
            ? String(sigmaSummary.last_sigma_rank)
            : "--",
        );

        if (inventoryDate) {
          inventoryDate.textContent = rows.length
            ? formatUTC(rows[rows.length - 1].date)
            : "--";
        }
        if (
          inventorySourceEl &&
          inventoryDate &&
          inventoryDate.textContent !== "--"
        ) {
          inventorySourceEl.innerHTML = `${inventorySourcePrefix}${inventoryDate.textContent}`;
        }
        if (macroDate) {
          macroDate.textContent = inflationSummary.history_end
            ? formatUTC(inflationSummary.history_end)
            : "--";
        }
        if (sigmaDate) {
          sigmaDate.textContent = sigmaSummary.history_end
            ? formatUTC(sigmaSummary.history_end)
            : "--";
        }
        if (priceDate) {
          priceDate.textContent = visible.length
            ? formatUTC(visible[visible.length - 1].date)
            : "--";
        }

        const inventoryChartNode = document.getElementById("wti-inventory-chart");

        if (ocOverlayOn) {
          const ocPayload = await loadWtiInventoryOcOverlay();

          const ocRows = Array.isArray(ocPayload?.rows) ? ocPayload.rows : [];
          const ocOverlay = ocPayload?.overlay || {};
          const ocMeta = ocPayload?.meta || {};

          if (ocRows.length) {
            renderWtiInventoryOcOverlayChart(inventoryChartNode, {
              rows: ocRows,
            });
          } else {
            setChartPlaceholder(
              "wti-inventory-chart",
              "OC Overlay: No seasonal rows available.",
            );
          }

          updateStatValue(
            document.getElementById("wti-inventory-last"),
            Number.isFinite(Number(ocOverlay.z))
              ? Number(ocOverlay.z).toFixed(2)
              : "--",
          );

          updateStatValue(
            document.getElementById("wti-inventory-change"),
            ocOverlay.state ? String(ocOverlay.state).toUpperCase() : "--",
          );

          updateStatValue(
            document.getElementById("wti-inventory-range"),
            "15Y RANGE",
          );

          if (inventoryDate) {
            inventoryDate.textContent = ocMeta.as_of_date
              ? formatUTC(ocMeta.as_of_date)
              : "--";
          }

          if (inventorySourcePrefixEl) {
            inventorySourcePrefixEl.textContent =
              "Source: EIA | the_Spine | 15Y Seasonal Overlay | As of ";
          }
        } else {
          const inventoryRows = visible
            .map((row) => ({
              date: row.date,
              close: Number(row.inventory_mmbbl),
            }))
            .filter((row) => row.date && Number.isFinite(row.close));

          if (inventoryRows.length) {
            renderWTILineChart(
              inventoryChartNode,
              inventoryRows,
              "Inventory Index",
            );
          } else {
            setChartPlaceholder(
              "wti-inventory-chart",
              "No inventory data available.",
            );
          }
        }

        const macroRows = visible
          .map((row) => ({
            date: row.date,
            close: Number(row.inflation_pressure_z),
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
            state: sigmaSummary.last_state ?? "--",
          });
        }

        if (macroRows.length) {
          renderWTILineChart(
            document.getElementById("wti-macro-chart"),
            macroRows,
            "Inflation Pressure",
          );
        } else {
          setChartPlaceholder("wti-macro-chart", "No inflation data available.");
        }

        if (wtiSigmaRows.length) {
          renderAssetSigmaChart(
            document.getElementById("wti-sigma-chart"),
            wtiSigmaRows,
            "WTI",
          );
        } else {
          setChartPlaceholder("wti-sigma-chart", "No sigma data available.");
        }

        const wtiPricePayload = await loadWtiPriceData();
        const wtiPriceRows = wtiPricePayload.rows
          .map((row) => ({
            date: row.date ?? row.as_of_date ?? row.timestamp ?? null,
            close: Number(row.close ?? row.price ?? row.last ?? row.value),
            high: Number(
              row.high ?? row.close ?? row.price ?? row.last ?? row.value,
            ),
            low: Number(
              row.low ?? row.close ?? row.price ?? row.last ?? row.value,
            ),
          }))
          .filter((row) => row.date && Number.isFinite(row.close))
          .sort((a, b) => String(a.date).localeCompare(String(b.date)))
          .slice(-(HORIZON_LENGTH_WTI[horizon] || 30));

        if (wtiPriceRows.length) {
          const wtiLast = wtiPriceRows[wtiPriceRows.length - 1].close;
          const wtiFirst = wtiPriceRows[0].close;
          const wtiChangePct = (wtiLast / wtiFirst - 1) * 100;
          const wtiMin = Math.min(...wtiPriceRows.map((d) => d.low));
          const wtiMax = Math.max(...wtiPriceRows.map((d) => d.high));

          updateStatValue(
            document.getElementById("wti-price-last"),
            formatNumber(wtiLast, 2),
          );
          updateStatValue(
            document.getElementById("wti-price-change"),
            `${wtiChangePct >= 0 ? "+" : ""}${formatNumber(wtiChangePct, 2)}%`,
            wtiChangePct >= 0 ? "positive" : "negative",
          );
          updateStatValue(
            document.getElementById("wti-price-range"),
            `${formatNumber(wtiMin, 2)} - ${formatNumber(wtiMax, 2)}`,
          );

          renderWTILineChart(
            document.getElementById("wti-price-chart"),
            wtiPriceRows.map((row) => ({
              date: row.date,
              close: row.close,
            })),
            "WTI Price",
          );

          if (priceDate) {
            priceDate.textContent = formatUTC(
              wtiPriceRows[wtiPriceRows.length - 1].date,
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
      }
    }

  function getRatesHorizonRows(rows = []) {
    const horizon = ratesControls.horizon?.value || "1Y";

    const horizonCount = {
      "30D": 30,
      "90D": 90,
      "1Y": 12,
      "MAX": rows.length,
    };

    const count = horizonCount[horizon] || 12;
    return Array.isArray(rows) ? rows.slice(-count) : [];
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
                runWTIRenderSafe();
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
            window.requestAnimationFrame(() => {
              window.requestAnimationFrame(() => {
                void renderEquities();
              });
            });
          }
        });
      }
    });

    async function loadCflowRegimeEngine() {
      const url = "data/serving/cflow/cflow_regime_engine_serving.json";

      try {
        const res = await fetch(url, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);

        const data = await res.json();
        const latest = data.latest || {};
        const attribution = data.attribution || {};

        const score = latest.cflow_score ?? "--";

        const drivers = (attribution.primary_drivers || [])
          .map(([name, value]) => `${name} ${Number(value).toFixed(2)}`)
          .join(" ? ");


        const offsets = (attribution.primary_offsets || [])
          .map(([name, value]) => `${name} ${Number(value).toFixed(2)}`)
          .join(" ? ");

        setCFlowLatestHeader(
          score === "--" ? "--" : Number(score).toFixed(3),
        );
        const driversNode = document.getElementById("cflow-regime-drivers");
        const offsetsNode = document.getElementById("cflow-regime-offsets");
        const noteNode = document.getElementById("cflow-regime-note");

        if (driversNode) driversNode.textContent = drivers || "--";
        if (offsetsNode) offsetsNode.textContent = offsets || "--";
        if (noteNode) noteNode.textContent = data.attribution?.diagnostic_note || "";
      } catch (err) {
        console.error("Failed to load C?FLOW Regime Engine:", err);
      }
    }


      Object.values(ratesControls).forEach((el) => {
        if (el) {
          el.addEventListener("change", () => {
            if (el === ratesControls.region) {
              updateRatesCountryOptions();
            }

            updateRatesGeoScenToolbarLabel();

            scheduleRatesRender();
          });
        }
      });

    document.addEventListener("change", (event) => {
      const target = event.target;
      if (!target) return;

      if (
        target.id === "wti-inventory-view" ||
        target.id === "wti-inventory-regime"
      ) {
        if (document.body.classList.contains("view-wti")) {
          runWTIRenderSafe();
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
            runWTIRenderSafe();
          });
        });
      }

      if (document.body.classList.contains("view-rates")) {
        window.requestAnimationFrame(() => {
          window.requestAnimationFrame(() => {
            renderRates();
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

    const ORACLE_LABS = {
      "vinv": {
        title: "VinV",
        cardTitle: "VinV",
        modalTitle: "Value in Vogue (VinV)",
        status: "deployed",
        repository: "https://github.com/sobcza11/VinV",
        report: "https://github.com/sobcza11/VinV/blob/main/reports/VinV.pdf",
        purpose:
          "Evaluate whether shareholder-oriented companies exhibit measurable financial resilience through disciplined capital allocation.",
        origin:
          "Inspired by Benjamin Graham's emphasis on shareholder stewardship, VinV explores whether firms that balance dividends, reinvestment, and financial discipline demonstrate stronger long-term fundamentals. It now connects into FINSTATE for comparative financial-state diagnostics.",
        methodology:
          "Factor Modeling • Multi-Factor Scoring • Financial Statements and Market Data • Capital Allocation Analysis • Walk-Forward Validation • Shareholder Quality Diagnostics",
      },
      "fedspeak": {
        title: "FedSpeak",
        status: "deployed",
        repository: "https://github.com/sobcza11/FedSpeak",
        purpose:
          "Translate central bank communication into structured, explainable policy diagnostics.",
        origin:
          "Created to determine whether qualitative Federal Reserve language could be converted into deterministic policy measurements. FedSpeak became a precursor to GeoScen and broader OracleChambers language interpretation.",
        methodology:
          "NLP Pipeline • Central Bank Language Parsing • Federal Reserve Communications • Hawkish/Dovish Feature Extraction • Policy Attribution • Deterministic Policy Diagnostics",
      },
      "us-inflation": {
        title: "US Inflation",
        status: "deployed",
        repository: "https://github.com/sobcza11/The-US-Inflation-Phenomenon",
        report:
          "https://github.com/sobcza11/The-US-Inflation-Phenomenon/blob/main/reports/Presentation%20-%20The%20US%20Inflation%20Phenomenon%2C%20It's%20Oil%20Silly.pdf",
        purpose:
          "Measure inflation using deterministic econometric diagnostics.",
        origin:
          "Inspired by an econometrics discussion that challenged whether inflation could ever be fully understood through quantitative methods.",
        methodology:
          "Supervised ML • Random Forest Regression • investpy and Public Macroeconomic Data • Inflation Feature Engineering • Feature Importance and Attribution • Deterministic Inflation Diagnostics",
      },
      "nlp-hknsl": {
        title: "NLP HKNSL",
        modalTitle: "Hong Kong National Security Law, NLP",
        status: "deployed",
        repository: "https://github.com/sobcza11/NLP_HK_Security_Law",
        report:
          "https://github.com/sobcza11/NLP_HK_Security_Law/blob/main/reports/NLP%20HK%20National%20Security%20Law%20Presentation.pdf",
        purpose:
          "Provide neutral, evidence-based interpretation of policy language surrounding the Hong Kong National Security Law.",
        origin:
          "Developed while the architect was living and working in Hong Kong during a sensitive geopolitical period. The goal was neutral language analysis, not advocacy.",
        methodology:
          "Unsupervised ML • NLP Clustering and Topic Modeling • BeautifulSoup Web Collection • Text Preprocessing and Embeddings • Explainable Narrative Classification • Neutral Policy Diagnostics",
      },
      "naval-mobility": {
        title: "Naval Mobility",
        status: "development",
        purpose:
          "Measure how naval deployments, Naval Carrier movements, maritime security, and strategic chokepoints influence global trade-route resilience.",
        origin:
          "Planned as a GeoScen extension for observing maritime pressure transmission using public visual evidence and independent verification.",
        methodology:
          "Computer Vision Planned • Satellite and Public Aerial Imagery • Geospatial Change Detection Planned • Public Maritime and Defense Reporting • Observed / Verified / Attributed • Maritime Transmission Diagnostics",
      },
      "stablecoin-infrastructure": {
        title: "Stablecoin Infra.",
        cardTitle: "Stablecoin Infra.",
        modalTitle: "Stablecoin Infrastructure",
        status: "development",
        purpose:
          "Measure the evolution of digital dollar infrastructure and its connection to digital asset markets.",
        origin:
          "Planned to track stablecoins as a durable financial infrastructure trend linking traditional dollar liquidity, Treasury collateral, cross-border settlement, and digital asset markets.",
        methodology:
          "Digital Asset Infrastructure Mapping • Stablecoin Supply Tracking • Treasury Collateral Analysis • Settlement Flow Diagnostics • Cross-Border Dollar Transmission • Non-Price Infrastructure Measurement",
      },
      "treasury-microstructure": {
        title: "UST Microstructure",
        cardTitle: "UST Microstructure",
        modalTitle: "Treasury Microstructure",
        status: "development",
        purpose:
          "Evaluate the health and functioning of the U.S. Treasury market through liquidity and market-structure diagnostics.",
        origin:
          "Planned as a RATES extension recognizing that rates are shaped by auctions, liquidity, dealer balance sheets, and market plumbing, not only policy.",
        methodology:
          "Auction Diagnostics • Dealer Capacity Monitoring • Repo and Funding Context • Liquidity Stress Measures • Curve Functioning Analysis • Treasury Transmission Diagnostics",
      },
      "dollar-liquidity": {
        title: "Dollar Liquidity",
        status: "development",
        purpose:
          "Measure the availability and transmission of global U.S. dollar funding.",
        origin:
          "Planned as a cross-domain funding layer connecting C•FLOW, RATES, FX, FINSTATE, and Stablecoin Infrastructure.",
        methodology:
          "SOFR and Repo Diagnostics • Cross-Currency Basis • Swap Line Context • Reserve Balance Monitoring • Stablecoin Linkage • Global Dollar Funding Diagnostics",
      },
    };

    function initOracleLabsModal() {
      const modal = document.getElementById("oracle-labs-modal");
      const titleNode = document.getElementById("oracle-labs-modal-title");
      const purposeNode = document.getElementById("oracle-labs-modal-purpose");
      const originNode = document.getElementById("oracle-labs-modal-origin");
      const methodologyNode = document.getElementById(
        "oracle-labs-modal-methodology",
      );
      const linksNode = document.getElementById("oracle-labs-modal-links");
      const panel = modal?.querySelector(".oracle-labs-modal-panel");

      if (
        !modal ||
        !titleNode ||
        !purposeNode ||
        !originNode ||
        !methodologyNode ||
        !linksNode
      ) {
        return;
      }

      let lastFocusedCard = null;

      function openModal(labId, trigger) {
        const lab = ORACLE_LABS[labId];
        if (!lab) return;

        lastFocusedCard = trigger || document.activeElement;
        titleNode.textContent = lab.modalTitle || lab.title;
        purposeNode.textContent = lab.purpose;
        originNode.textContent = lab.origin;
        methodologyNode.textContent = lab.methodology;
        linksNode.replaceChildren();

        if (lab.status === "deployed" && lab.repository) {
          const footerItems = [
            { label: "Links", href: lab.repository },
            { label: "Report", href: lab.report },
          ].filter((item) => item.href);

          const openingBullet = document.createElement("span");
          openingBullet.className = "oracle-footer-bullet";
          openingBullet.textContent = "•";
          linksNode.append(openingBullet);

          footerItems.forEach((item, index) => {
            if (index > 0) {
              const separator = document.createElement("span");
              separator.className = "oracle-footer-separator";
              separator.textContent = "•|•";
              linksNode.append(separator);
            }

            const link = document.createElement("a");
            link.className = "oracle-footer-link";
            link.href = item.href;
            link.textContent = item.label;
            link.target = "_blank";
            link.rel = "noopener noreferrer";
            link.addEventListener("click", (event) => {
              event.stopPropagation();
            });

            linksNode.append(link);
          });

          const trailingBullet = document.createElement("span");
          trailingBullet.className = "oracle-footer-bullet";
          trailingBullet.textContent = "•";
          linksNode.append(trailingBullet);
        }

        modal.classList.add("is-open");
        modal.setAttribute("aria-hidden", "false");
        document.body.classList.add("oracle-labs-modal-open");
        panel?.focus();
      }

      function closeModal() {
        if (!modal.classList.contains("is-open")) return;

        modal.classList.remove("is-open");
        modal.setAttribute("aria-hidden", "true");
        document.body.classList.remove("oracle-labs-modal-open");

        if (lastFocusedCard && typeof lastFocusedCard.focus === "function") {
          lastFocusedCard.focus();
        }
      }

      document.querySelectorAll("#view-oc [data-lab-id]").forEach((card) => {
        card.addEventListener("click", () => {
          openModal(card.dataset.labId, card);
        });
      });

      modal.querySelectorAll("[data-lab-modal-close]").forEach((target) => {
        target.addEventListener("click", closeModal);
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
          closeModal();
        }
      });
    }

    initOracleLabsModal();

    function initOracleLabsScrollPanels() {
      const view = document.getElementById("view-oc");
      if (!view) return;

      const stage = view.querySelector(".oracle-labs-section");
      const hero = view.querySelector(".oracle-labs-hero");
      const oracleImage = hero?.querySelector(".oracle-labs-image");
      const panels = Array.from(view.querySelectorAll(".oracle-chamber-panel"));
      const controls = Array.from(
        view.querySelectorAll("[data-oracle-chamber-step]"),
      );

      if (!stage || !hero || !panels.length) return;

      let currentChamberIndex = -1;
      let isChamberLocked = false;
      const transitionMs = 700;
      const ORACLE_TITLE_OFFSET = 30;

      function clampChamberIndex(index) {
        return Math.max(-1, Math.min(panels.length - 1, index));
      }

      function setActivePanel(index) {
        panels.forEach((panel, i) => {
          panel.classList.toggle("is-active", i === index);
        });
      }

      function updateOracleCupAnchor() {
        if (!oracleImage) return;

        const imageRect = oracleImage.getBoundingClientRect();
        if (!imageRect.width || !imageRect.height) return;

        const groupY =
          imageRect.bottom - imageRect.height * 0.15 + ORACLE_TITLE_OFFSET;
        document.documentElement.style.setProperty(
          "--oracle-cup-y",
          `${groupY}px`,
        );
      }

      function getTargetY(index) {
        if (index < 0) return stage.offsetTop;

        return stage.offsetTop + window.innerHeight * (index + 1);
      }

      function goToChamber(index, behavior = "smooth") {
        const nextIndex = clampChamberIndex(index);

        currentChamberIndex = nextIndex;
        setActivePanel(nextIndex);

        window.scrollTo({
          top: getTargetY(nextIndex),
          behavior,
        });
      }

      function syncChamberFromScroll() {
        updateOracleCupAnchor();

        if (!document.body.classList.contains("view-oc")) {
          setActivePanel(-1);
          currentChamberIndex = -1;
          return;
        }

        const stageProgress = window.scrollY - stage.offsetTop;

        if (stageProgress < window.innerHeight * 0.55) {
          currentChamberIndex = -1;
          setActivePanel(-1);
          return;
        }

        const rawIndex = Math.round(stageProgress / window.innerHeight) - 1;
        const nextIndex = clampChamberIndex(rawIndex);

        currentChamberIndex = nextIndex;
        setActivePanel(nextIndex);
      }

      function stepChamber(direction) {
        if (!document.body.classList.contains("view-oc")) return;
        if (document.body.classList.contains("oracle-labs-modal-open")) return;
        if (isChamberLocked) return;

        isChamberLocked = true;
        syncChamberFromScroll();
        goToChamber(currentChamberIndex + direction);

        window.setTimeout(() => {
          isChamberLocked = false;
          syncChamberFromScroll();
        }, transitionMs);
      }

      function onWheel(event) {
        if (!document.body.classList.contains("view-oc")) return;
        if (document.body.classList.contains("oracle-labs-modal-open")) return;

        const target = event.target;

        if (
          target.closest(".topbar") ||
          target.closest(".top-nav") ||
          target.closest("button") ||
          target.closest("select") ||
          target.closest("a") ||
          target.closest("input") ||
          target.closest("textarea")
        ) {
          return;
        }

        if (!target.closest("#view-oc")) return;

        event.preventDefault();

        if (Math.abs(event.deltaY) < 8) return;

        stepChamber(event.deltaY > 0 ? 1 : -1);
      }

      window.updateOracleLabsScrollPanels = () => {
        updateOracleCupAnchor();
        syncChamberFromScroll();
      };

      window.addEventListener("wheel", onWheel, { passive: false });
      window.addEventListener("scroll", syncChamberFromScroll, { passive: true });
      window.addEventListener("resize", () => {
        if (!document.body.classList.contains("view-oc")) return;
        if (document.body.classList.contains("oracle-labs-modal-open")) return;

        updateOracleCupAnchor();
        goToChamber(currentChamberIndex, "auto");
      });

      oracleImage?.addEventListener("load", updateOracleCupAnchor);

      if (oracleImage?.complete) {
        updateOracleCupAnchor();
      }

      hero.addEventListener("click", () => {
        stepChamber(1);
      });

      hero.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;

        event.preventDefault();
        stepChamber(1);
      });

      controls.forEach((control) => {
        control.addEventListener("click", () => {
          const direction = Number(control.dataset.oracleChamberStep || 0);
          if (!direction) return;

          stepChamber(direction);
        });
      });

      syncChamberFromScroll();
    }

initOracleLabsScrollPanels();



    (async function boot() {
      try {
        await loadActiveData();
        finstateUniverseData = await fetchJsonWithBust(
          DATA_ENDPOINTS.finstateUniverse,
        );

        if (!Array.isArray(finstateUniverseData)) {
          finstateUniverseData = [];
        }

        populateFxPairOptions();
      } catch (error) {
        activeDataLoadError = error;
        console.error("Initial active FX data load failed:", error);
      }

      startActiveRefreshLoop();
      showView("what-is");
    })();
  });



