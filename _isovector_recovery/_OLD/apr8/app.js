document.addEventListener("DOMContentLoaded", () => {
  const viewButtons = document.querySelectorAll("[data-view]");
  const contentViews = document.querySelectorAll(".content-view");
  const navParents = document.querySelectorAll(".nav-parent");
  const subnavs = document.querySelectorAll(".subnav");
  const moduleTabs = document.querySelectorAll(".module-tab[data-view]");

  const fxPairSelect = document.getElementById("fx-pair");
  const fxSpreadsSelect = document.getElementById("fx-spreads");
  const fxHorizonSelect = document.getElementById("fx-horizon");
  const fxGeoScenModeSelect = document.getElementById("fx-geoscen-mode");

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
    document.body.classList.remove("view-what-is", "view-fx");
    document.body.classList.add(viewName === "fx" ? "view-fx" : "view-what-is");
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
    if (targetView) targetView.classList.add("active");

    setActiveButtons(viewName);
    syncSidebarHierarchy(viewName);
    setBodyViewClass(viewName);

    moduleTabs.forEach((tab) => tab.classList.remove("active"));
    document.querySelectorAll(`.module-tab[data-view="${viewName}"]`).forEach((tab) => {
      tab.classList.add("active");
    });
  }

  function normalizePairLabel(rawValue) {
    if (!rawValue) return "AUD/USD";

    const compact = String(rawValue).replace(/\s+/g, "").replace("/", "").toUpperCase();

    if (compact === "DXY") return "DXY";
    if (compact.length === 6) return `${compact.slice(0, 3)}/${compact.slice(3, 6)}`;

    return String(rawValue).toUpperCase();
  }

  function normalizePairKey(rawValue) {
    if (!rawValue) return "AUDUSD";
    return String(rawValue).replace(/\s+/g, "").replace("/", "").toUpperCase();
  }

  function parseDateValue(value) {
    const dt = new Date(value);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }

  function formatUTC(dateStr) {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return "--";
    return (
      d.getUTCFullYear() +
      "-" +
      String(d.getUTCMonth() + 1).padStart(2, "0") +
      "-" +
      String(d.getUTCDate()).padStart(2, "0") +
      " (UTC)"
    );
  }

  function safeNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function formatSigned(value, decimals = 3) {
    const num = safeNumber(value);
    if (num === null) return "--";
    const sign = num > 0 ? "+" : "";
    return `${sign}${num.toFixed(decimals)}`;
  }

  function formatPlain(value, decimals = 3) {
    const num = safeNumber(value);
    if (num === null) return "--";
    return num.toFixed(decimals);
  }

  function setValueWithTone(elementId, value, decimals = 3) {
    const el = document.getElementById(elementId);
    if (!el) return;

    const num = safeNumber(value);
    if (num === null) {
      el.textContent = "--";
      el.classList.remove("positive", "negative");
      return;
    }

    el.textContent = formatSigned(num, decimals);
    el.classList.toggle("positive", num > 0);
    el.classList.toggle("negative", num < 0);
  }

  function cloneRows(rows) {
    return Array.isArray(rows) ? rows.map((row) => ({ ...row })) : [];
  }

  function getDatasetMap(possibleNames) {
    for (const name of possibleNames) {
      const candidate = window[name];
      if (candidate && typeof candidate === "object" && !Array.isArray(candidate)) {
        return candidate;
      }
    }
    return {};
  }

  function getDatasetArray(possibleNames) {
    for (const name of possibleNames) {
      const candidate = window[name];
      if (Array.isArray(candidate)) {
        return candidate;
      }
    }
    return [];
  }

  const fxPriceMap = getDatasetMap([
    "IV_FX_PRICE_DATA",
    "fxPriceData",
    "FX_PRICE_DATA",
    "fx_price_data"
  ]);

  const fxSpreadMap = getDatasetMap([
    "IV_FX_SPREADS_DATA",
    "fxSpreadData",
    "FX_SPREADS_DATA",
    "fx_spreads_data"
  ]);

  const fxSigmaMap = getDatasetMap([
    "IV_FX_SIGMA_DATA",
    "fxSigmaDataMap",
    "FX_SIGMA_DATA_MAP",
    "fx_sigma_data_map"
  ]);

  const fxSigmaArray = getDatasetArray([
    "IV_FX_SIGMA_DATA",
    "fxSigmaData",
    "FX_SIGMA_DATA",
    "fx_sigma_data",
    "sigmaData"
  ]);

  function getSelectedPriceRows() {
    const pairKey = normalizePairKey(fxPairSelect?.value || "AUD/USD");
    const rows = cloneRows(fxPriceMap[pairKey]);

    return rows
      .filter((row) => parseDateValue(row.date))
      .sort((a, b) => parseDateValue(a.date) - parseDateValue(b.date));
  }

  function getSelectedSpreadRows() {
    const pairKey = normalizePairKey(fxPairSelect?.value || "AUD/USD");
    const rows = cloneRows(fxSpreadMap[pairKey]);

    return rows
      .filter((row) => parseDateValue(row.as_of_date))
      .sort((a, b) => parseDateValue(a.as_of_date) - parseDateValue(b.as_of_date));
  }

  function getSelectedSigmaRows() {
    const pairKey = normalizePairKey(fxPairSelect?.value || "AUD/USD");

    if (Array.isArray(fxSigmaMap[pairKey])) {
      return cloneRows(fxSigmaMap[pairKey])
        .filter((row) => parseDateValue(row.date || row.as_of_date))
        .sort((a, b) => parseDateValue(a.date || a.as_of_date) - parseDateValue(b.date || b.as_of_date));
    }

    if (fxSigmaArray.length) {
      return cloneRows(fxSigmaArray)
        .filter((row) => normalizePairKey(row.symbol || row.pair) === pairKey)
        .filter((row) => parseDateValue(row.date || row.as_of_date))
        .sort((a, b) => parseDateValue(a.date || a.as_of_date) - parseDateValue(b.date || b.as_of_date));
    }

    return [];
  }

  function getAllSigmaLatestRowsFromSources(priceMap, sigmaMap, sigmaArray) {
    const latestRows = [];

    const sigmaMapKeys = Object.keys(sigmaMap || {});
    if (sigmaMapKeys.length) {
      sigmaMapKeys.forEach((pairKey) => {
        const rows = cloneRows(sigmaMap[pairKey])
          .filter((row) => parseDateValue(row.date || row.as_of_date))
          .sort((a, b) => parseDateValue(a.date || a.as_of_date) - parseDateValue(b.date || b.as_of_date));
        if (rows.length) latestRows.push(rows[rows.length - 1]);
      });
      return latestRows;
    }

    if (Array.isArray(sigmaArray) && sigmaArray.length) {
      const latestByPair = new Map();

      sigmaArray.forEach((row) => {
        const pairKey = normalizePairKey(row.symbol || row.pair);
        const dt = parseDateValue(row.date || row.as_of_date);
        if (!pairKey || !dt) return;

        const existing = latestByPair.get(pairKey);
        if (!existing || dt > existing._dt) {
          latestByPair.set(pairKey, { ...row, _dt: dt });
        }
      });

      latestByPair.forEach((row) => {
        const cleanRow = { ...row };
        delete cleanRow._dt;
        latestRows.push(cleanRow);
      });

      return latestRows;
    }

    const priceKeys = Object.keys(priceMap || {});
    priceKeys.forEach((pairKey) => {
      const rows = cloneRows(priceMap[pairKey])
        .filter((row) => parseDateValue(row.date))
        .sort((a, b) => parseDateValue(a.date) - parseDateValue(b.date));

      if (!rows.length) return;

      const subset = rows.slice(-20);
      const closes = subset.map((r) => safeNumber(r.close)).filter((v) => v !== null);
      if (closes.length < 2) return;

      const mean = closes.reduce((sum, v) => sum + v, 0) / closes.length;
      const variance = closes.reduce((sum, v) => sum + (v - mean) ** 2, 0) / closes.length;
      const std = Math.sqrt(variance);

      latestRows.push({
        pair: pairKey,
        symbol: pairKey,
        z_score: std,
        value: std,
        date: subset[subset.length - 1].date
      });
    });

    latestRows.sort((a, b) => {
      const av = safeNumber(a.z_score) ?? safeNumber(a.value) ?? 0;
      const bv = safeNumber(b.z_score) ?? safeNumber(b.value) ?? 0;
      return av - bv;
    });

    latestRows.forEach((row, idx) => {
      row.rank = idx + 1;
    });

    return latestRows;
  }

  function setFxPriceHeader() {
    const pairLabel = normalizePairLabel(fxPairSelect?.value || "AUD/USD");
    const horizon = fxHorizonSelect?.value || "30D";

    const badge = document.getElementById("fx-price-badge");
    if (badge) badge.textContent = `${pairLabel} • ${horizon}`;
  }

  function setFxSpreadHeader() {
    const mode = fxSpreadsSelect?.value || "Comparative Spread";

    const badge = document.getElementById("fx-spread-badge");
    if (badge) badge.textContent = mode;

    const modeValue = document.getElementById("fx-spread-mode");
    if (modeValue) {
      modeValue.textContent =
        mode === "Comparative Spread"
          ? "Comparative"
          : mode === "Rate Differential"
          ? "Differential"
          : "Bond Lens";
    }
  }

  function setFxPriceDate(rows) {
    const el = document.getElementById("fx-price-date");
    if (!el) return;
    el.textContent = rows.length ? formatUTC(rows[rows.length - 1].date) : "--";
  }

  function setFxSpreadDate(rows) {
    const el = document.getElementById("fx-spread-date");
    if (!el) return;
    el.textContent = rows.length ? formatUTC(rows[rows.length - 1].as_of_date) : "--";
  }

  function setFxSigmaDate(rows, fallbackRows) {
    const el = document.getElementById("fx-sigma-date");
    if (!el) return;

    if (rows.length) {
      const lastRow = rows[rows.length - 1];
      el.textContent = formatUTC(lastRow.date || lastRow.as_of_date);
      return;
    }

    el.textContent = fallbackRows.length ? formatUTC(fallbackRows[fallbackRows.length - 1].date) : "--";
  }

  function updateFxPriceStats(rows) {
    const lastEl = document.getElementById("fx-price-last");
    const changeEl = document.getElementById("fx-price-change");
    const rangeEl = document.getElementById("fx-price-range");

    if (!lastEl || !changeEl || !rangeEl) return;

    if (!rows.length) {
      lastEl.textContent = "--";
      changeEl.textContent = "--";
      rangeEl.textContent = "--";
      changeEl.classList.remove("positive", "negative");
      return;
    }

    const last = safeNumber(rows[rows.length - 1].close);
    const prev = rows.length > 1 ? safeNumber(rows[rows.length - 2].close) : null;
    const change = last !== null && prev !== null ? last - prev : null;

    const lows = rows.map((r) => safeNumber(r.low)).filter((v) => v !== null);
    const highs = rows.map((r) => safeNumber(r.high)).filter((v) => v !== null);

    lastEl.textContent = formatPlain(last, 3);
    setValueWithTone("fx-price-change", change, 3);

    rangeEl.textContent =
      lows.length && highs.length
        ? `${Math.min(...lows).toFixed(3)} – ${Math.max(...highs).toFixed(3)}`
        : "--";
  }

  function updateFxSpreadStats(rows) {
    const lastEl = document.getElementById("fx-spread-last");
    const avgEl = document.getElementById("fx-spread-avg");

    if (!lastEl || !avgEl) return;

    if (!rows.length) {
      lastEl.textContent = "--";
      avgEl.textContent = "--";
      return;
    }

    const values = rows
      .map((r) => safeNumber(r.yld_10y_diff))
      .filter((v) => v !== null);

    if (!values.length) {
      lastEl.textContent = "--";
      avgEl.textContent = "--";
      return;
    }

    const last = values[values.length - 1];
    const avg = values.reduce((sum, v) => sum + v, 0) / values.length;

    lastEl.textContent = formatSigned(last, 3);
    avgEl.textContent = formatSigned(avg, 3);
  }

  function updateFxSigmaStats(rows, priceRows) {
    const selectedLabel = document.getElementById("fx-sigma-selected");
    const zLabel = document.getElementById("fx-sigma-z");
    const rankLabel = document.getElementById("fx-sigma-rank");

    if (!selectedLabel || !zLabel || !rankLabel) return;

    if (rows.length) {
      const latest = rows[rows.length - 1];

      selectedLabel.textContent = normalizePairLabel(
        latest.symbol || latest.pair || fxPairSelect?.value || "AUD/USD"
      );

      const z =
        safeNumber(latest.z_score) ??
        safeNumber(latest.z) ??
        safeNumber(latest.sigma_z) ??
        safeNumber(latest.value);

      zLabel.textContent = z === null ? "--" : formatSigned(z, 2);
      zLabel.classList.toggle("positive", z !== null && z > 0);
      zLabel.classList.toggle("negative", z !== null && z < 0);

      const rank =
        latest.rank ??
        latest.sigma_rank ??
        latest.ease_rank ??
        null;

      rankLabel.textContent = rank === null ? "--" : String(rank);
      return;
    }

    selectedLabel.textContent = normalizePairLabel(fxPairSelect?.value || "AUD/USD");
    zLabel.textContent = "--";
    zLabel.classList.remove("positive", "negative");
    rankLabel.textContent = "--";

    if (priceRows.length) {
      selectedLabel.textContent = normalizePairLabel(fxPairSelect?.value || "AUD/USD");
    }
  }

  function renderFallbackMessage(containerId, message) {
    const el = document.getElementById(containerId);
    if (!el) return;
    el.innerHTML = `<div class="panel-placeholder">${message}</div>`;
  }

  function renderPriceChart(rows) {
    const el = document.getElementById("fx-price-chart");
    if (!el) return;

    if (!rows.length) {
      renderFallbackMessage("fx-price-chart", "No FX price data available");
      return;
    }

    const subset = rows.slice(-30);
    const closes = subset.map((r) => safeNumber(r.close)).filter((v) => v !== null);

    if (!closes.length) {
      renderFallbackMessage("fx-price-chart", "No FX price data available");
      return;
    }

    const min = Math.min(...closes);
    const max = Math.max(...closes);
    const range = max - min || 1;

    const width = 900;
    const height = 250;
    const padX = 42;
    const padY = 20;
    const innerW = width - padX * 2;
    const innerH = height - padY * 2;

    const points = subset.map((row, idx) => {
      const close = safeNumber(row.close);
      const x = padX + (innerW * idx) / Math.max(subset.length - 1, 1);
      const y =
        close === null
          ? padY + innerH / 2
          : padY + innerH - ((close - min) / range) * innerH;
      return { x, y, close, date: row.date };
    });

    const path = points.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ");
    const lastPoint = points[points.length - 1];

    const yTicks = 4;
    const yGrid = Array.from({ length: yTicks + 1 }, (_, i) => {
      const y = padY + (innerH * i) / yTicks;
      const value = max - (range * i) / yTicks;
      return { y, value };
    });

    const xLabels = [0, Math.floor((points.length - 1) / 2), points.length - 1]
      .filter((v, i, arr) => arr.indexOf(v) === i)
      .map((idx) => points[idx]);

    el.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX price chart">
        ${yGrid
          .map(
            (g) => `
              <line class="fx-grid-line" x1="${padX}" y1="${g.y}" x2="${width - padX}" y2="${g.y}"></line>
              <text class="fx-axis-label" x="${padX - 8}" y="${g.y + 3}" text-anchor="end">${g.value.toFixed(3)}</text>
            `
          )
          .join("")}
        <path class="fx-line-secondary" d="${path}"></path>
        <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
        ${xLabels
          .map(
            (p) => `
              <text class="fx-axis-label" x="${p.x}" y="${height - 6}" text-anchor="middle">${p.date}</text>
            `
          )
          .join("")}
      </svg>
    `;
  }

  function renderSpreadChart(rows) {
    const el = document.getElementById("fx-spread-chart");
    if (!el) return;

    if (!rows.length) {
      renderFallbackMessage("fx-spread-chart", "No spread data available");
      return;
    }

    const subset = rows.slice(-24);
    const values = subset.map((r) => safeNumber(r.yld_10y_diff)).filter((v) => v !== null);

    if (!values.length) {
      renderFallbackMessage("fx-spread-chart", "No spread data available");
      return;
    }

    const min = Math.min(...values);
    const max = Math.max(...values);
    const floor = Math.min(min, 0);
    const ceil = Math.max(max, 0);
    const range = ceil - floor || 1;

    const width = 520;
    const height = 250;
    const padX = 42;
    const padY = 20;
    const innerW = width - padX * 2;
    const innerH = height - padY * 2;

    const points = subset.map((row, idx) => {
      const value = safeNumber(row.yld_10y_diff);
      const x = padX + (innerW * idx) / Math.max(subset.length - 1, 1);
      const y =
        value === null
          ? padY + innerH / 2
          : padY + innerH - ((value - floor) / range) * innerH;
      return { x, y, value, date: row.as_of_date };
    });

    const zeroY = padY + innerH - ((0 - floor) / range) * innerH;
    const path = points.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ");
    const lastPoint = points[points.length - 1];

    const yTicks = 4;
    const yGrid = Array.from({ length: yTicks + 1 }, (_, i) => {
      const y = padY + (innerH * i) / yTicks;
      const value = ceil - (range * i) / yTicks;
      return { y, value };
    });

    el.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX spread chart">
        ${yGrid
          .map(
            (g) => `
              <line class="fx-grid-line" x1="${padX}" y1="${g.y}" x2="${width - padX}" y2="${g.y}"></line>
              <text class="fx-axis-label" x="${padX - 8}" y="${g.y + 3}" text-anchor="end">${g.value.toFixed(2)}</text>
            `
          )
          .join("")}
        <line class="fx-zero-line" x1="${padX}" y1="${zeroY}" x2="${width - padX}" y2="${zeroY}"></line>
        <path class="fx-line-secondary" d="${path}"></path>
        <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
        <text class="fx-axis-label" x="${padX}" y="${height - 6}" text-anchor="start">${points[0].date}</text>
        <text class="fx-axis-label" x="${width - padX}" y="${height - 6}" text-anchor="end">${lastPoint.date}</text>
      </svg>
    `;
  }

  function renderSigmaChart(selectedRows) {
    const el = document.getElementById("fx-sigma-chart");
    if (!el) return;

    const bars = getAllSigmaLatestRowsFromSources(fxPriceMap, fxSigmaMap, fxSigmaArray).slice(0, 10);

    if (!bars.length) {
      renderFallbackMessage("fx-sigma-chart", "No sigma data available");
      return;
    }

    const selectedKey = normalizePairKey(fxPairSelect?.value || "AUD/USD");
    const maxAbs = Math.max(
      ...bars.map((b) => Math.abs(safeNumber(b.z_score) ?? safeNumber(b.z) ?? safeNumber(b.sigma_z) ?? safeNumber(b.value) ?? 0)),
      1
    );

    const width = 520;
    const height = 220;
    const padX = 26;
    const padTop = 20;
    const padBottom = 34;
    const gap = 10;
    const barWidth = (width - padX * 2 - gap * (bars.length - 1)) / bars.length;
    const innerH = height - padTop - padBottom;

    el.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX sigma chart">
        ${bars
          .map((bar, idx) => {
            const value =
              safeNumber(bar.z_score) ??
              safeNumber(bar.z) ??
              safeNumber(bar.sigma_z) ??
              safeNumber(bar.value) ??
              0;

            const key = normalizePairKey(bar.symbol || bar.pair);
            const label = normalizePairLabel(bar.symbol || bar.pair);
            const x = padX + idx * (barWidth + gap);
            const h = (Math.abs(value) / maxAbs) * (innerH - 18);
            const y = padTop + innerH - h;
            const selectedClass = key === selectedKey ? " selected" : "";

            return `
              <rect class="fx-bar${selectedClass}" x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="4"></rect>
              <text class="fx-bar-value" x="${x + barWidth / 2}" y="${y - 6}" text-anchor="middle">${value.toFixed(2)}</text>
              <text class="fx-bar-label" x="${x + barWidth / 2}" y="${height - 10}" text-anchor="middle">${label}</text>
            `;
          })
          .join("")}
      </svg>
    `;
  }

  function setGeoScenLabel(mode) {
    const el = document.getElementById("geoscen-label");
    if (!el) return;
    el.textContent = mode === "Systemic"
      ? "GeoScen | OC | Systemic"
      : "GeoScen | OC | Home (Countries)";
  }

  function refreshFxPanel() {
    const priceRows = getSelectedPriceRows();
    const spreadRows = getSelectedSpreadRows();
    const sigmaRows = getSelectedSigmaRows();

    setFxPriceHeader();
    setFxSpreadHeader();
    setFxPriceDate(priceRows);
    setFxSpreadDate(spreadRows);
    setFxSigmaDate(sigmaRows, priceRows);

    updateFxPriceStats(priceRows);
    updateFxSpreadStats(spreadRows);
    updateFxSigmaStats(sigmaRows, priceRows);

    renderPriceChart(priceRows);
    renderSpreadChart(spreadRows);
    renderSigmaChart(sigmaRows);
  }

  navParents.forEach((parent) => {
    parent.addEventListener("click", () => {
      if (parent.dataset.locked === "true") return;

      const viewName = parent.dataset.view;
      const parentKey = parent.dataset.parent;

      if (parentKey) openSubnav(parentKey);
      if (viewName) showView(viewName);
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

  if (fxPairSelect) fxPairSelect.addEventListener("change", refreshFxPanel);

  if (fxSpreadsSelect) {
    fxSpreadsSelect.addEventListener("change", () => {
      setFxSpreadHeader();
      refreshFxPanel();
    });
  }

  if (fxHorizonSelect) {
    fxHorizonSelect.addEventListener("change", () => {
      setFxPriceHeader();
      refreshFxPanel();
    });
  }

  if (fxGeoScenModeSelect) {
    fxGeoScenModeSelect.addEventListener("change", (e) => {
      setGeoScenLabel(e.target.value);
    });
  }

  setGeoScenLabel(fxGeoScenModeSelect?.value || "Home");
  showView("what-is");
  refreshFxPanel();
});
