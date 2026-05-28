document.addEventListener("DOMContentLoaded", () => {
  const viewButtons = document.querySelectorAll("[data-view]");
  const contentViews = document.querySelectorAll(".content-view");
  const navParents = document.querySelectorAll(".nav-parent");
  const subnavs = document.querySelectorAll(".subnav");
  const moduleTabs = document.querySelectorAll(".module-tab[data-view]");

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
    if (viewName === "fx") {
      document.body.classList.add("view-fx");
    } else {
      document.body.classList.add("view-what-is");
    }
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
    contentViews.forEach((view) => {
      view.classList.remove("active");
    });

    viewButtons.forEach((button) => {
      button.classList.remove("active");
    });

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
      renderFX();
    }
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

  const fxControls = {
    pair: document.getElementById("fx-pair"),
    spreads: document.getElementById("fx-spreads"),
    horizon: document.getElementById("fx-horizon"),
    geoscen: document.getElementById("fx-geoscen-mode")
  };

  Object.values(fxControls).forEach((el) => {
    if (el) {
      el.addEventListener("change", () => {
        if (document.body.classList.contains("view-fx")) {
          renderFX();
        }
      });
    }
  });

  window.addEventListener("resize", () => {
    if (document.body.classList.contains("view-fx")) {
      renderFX();
    }
  });

  const FX_BASE = {
    "DXY": 104.2,
    "EUR/USD": 1.082,
    "USD/JPY": 151.4,
    "GBP/USD": 1.271,
    "AUD/USD": 0.664
  };

  const HORIZON_LENGTH = {
    "5D": 5,
    "15D": 15,
    "30D": 30,
    "45D": 45
  };

  const PAIRS = Object.keys(FX_BASE);

  function formatNumber(value, digits = 2) {
    return Number(value).toFixed(digits);
  }

  function getPairDigits(pair) {
    if (pair === "DXY") return 2;
    if (pair === "USD/JPY") return 2;
    return 4;
  }

  function hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i += 1) {
      hash = ((hash << 5) - hash) + str.charCodeAt(i);
      hash |= 0;
    }
    return Math.abs(hash);
  }

  function generateSeries(pair, horizon) {
    const len = HORIZON_LENGTH[horizon] || 30;
    const base = FX_BASE[pair] || 1;
    const seed = hashString(`${pair}-${horizon}`);
    const driftBase = ((seed % 9) - 4) * 0.0009;
    const amp1 = ((seed % 17) + 6) * 0.0015;
    const amp2 = ((seed % 11) + 4) * 0.0011;
    const cyc1 = ((seed % 5) + 3);
    const cyc2 = ((seed % 7) + 5);

    const series = [];

    for (let i = 0; i < len; i += 1) {
      const t = i / Math.max(len - 1, 1);
      const wave1 = Math.sin((Math.PI * cyc1 * t) + (seed % 13));
      const wave2 = Math.cos((Math.PI * cyc2 * t) + (seed % 19));
      const drift = driftBase * i;
      const pctMove = (wave1 * amp1) + (wave2 * amp2 * 0.7) + drift;
      const value = base * (1 + pctMove);
      series.push(Number(value.toFixed(6)));
    }

    return series;
  }

  function buildSpreadSeries(pair, spreadMode, horizon) {
    const main = generateSeries(pair, horizon);
    const benchmarkPair =
      pair === "DXY" ? "EUR/USD" :
      pair === "EUR/USD" ? "GBP/USD" :
      pair === "USD/JPY" ? "DXY" :
      pair === "GBP/USD" ? "EUR/USD" :
      "DXY";

    const benchmark = generateSeries(benchmarkPair, horizon);
    const len = Math.min(main.length, benchmark.length);

    return Array.from({ length: len }, (_, i) => {
      const mainRet = i === 0 ? 0 : ((main[i] / main[i - 1]) - 1);
      const benchRet = i === 0 ? 0 : ((benchmark[i] / benchmark[i - 1]) - 1);

      if (spreadMode === "Rate Differential") {
        return Number(((mainRet - benchRet) * 10000).toFixed(2));
      }

      if (spreadMode === "Bond Spread Lens") {
        return Number((((main[i] - benchmark[i]) / benchmark[i]) * 100).toFixed(2));
      }

      return Number(((mainRet - benchRet) * 100).toFixed(3));
    });
  }

  function getSigmaTable(horizon) {
    const values = PAIRS.map((pair) => {
      const series = generateSeries(pair, horizon);
      const returns = series.slice(1).map((v, i) => (v / series[i]) - 1);
      const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
      const variance = returns.reduce((a, b) => a + ((b - mean) ** 2), 0) / returns.length;
      const sigma = Math.sqrt(variance);
      return { pair, sigma };
    });

    const sigmaMean = values.reduce((a, b) => a + b.sigma, 0) / values.length;
    const sigmaStd = Math.sqrt(
      values.reduce((a, b) => a + ((b.sigma - sigmaMean) ** 2), 0) / values.length
    ) || 1;

    const ranked = values
      .map((row) => ({
        ...row,
        z: (row.sigma - sigmaMean) / sigmaStd
      }))
      .sort((a, b) => b.z - a.z)
      .map((row, idx) => ({
        ...row,
        rank: idx + 1
      }));

    return ranked;
  }

  function createLinePath(values, width, height, padding) {
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

  function createAreaPath(points, width, height, padding) {
    if (!points.length) return "";
    const first = points[0];
    const last = points[points.length - 1];
    const bottom = height - padding.bottom;

    return [
      `M ${first.x.toFixed(2)} ${bottom.toFixed(2)}`,
      ...points.map((p) => `L ${p.x.toFixed(2)} ${p.y.toFixed(2)}`),
      `L ${last.x.toFixed(2)} ${bottom.toFixed(2)}`,
      "Z"
    ].join(" ");
  }

  function renderPriceChart(container, values) {
    if (!container) return;

    const width = Math.max(container.clientWidth, 300);
    const height = Math.max(container.clientHeight, 230);
    const padding = { top: 18, right: 16, bottom: 28, left: 16 };

    const { points, path, min, max } = createLinePath(values, width, height, padding);
    const areaPath = createAreaPath(points, width, height, padding);

    const yTop = padding.top;
    const yBottom = height - padding.bottom;
    const labels = [max, (max + min) / 2, min]
      .map((v, idx) => {
        const y = yTop + ((idx / 2) * (yBottom - yTop));
        return `
          <line class="fx-grid-line" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
          <text class="fx-axis-label" x="${width - padding.right}" y="${y - 6}" text-anchor="end">${formatNumber(v, 4)}</text>
        `;
      })
      .join("");

    const xLabels = [0, Math.floor(values.length / 2), values.length - 1]
      .map((idx) => {
        const x = points[idx]?.x ?? padding.left;
        return `<text class="fx-axis-label" x="${x}" y="${height - 8}" text-anchor="middle">T-${values.length - 1 - idx}</text>`;
      })
      .join("");

    const lastPoint = points[points.length - 1];

    container.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="FX Price Chart">
        ${labels}
        <path class="fx-area-fill" d="${areaPath}"></path>
        <path class="fx-line-main" d="${path}"></path>
        <circle class="fx-point-last" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
        ${xLabels}
      </svg>
    `;
  }

  function renderSpreadChart(container, values) {
    if (!container) return;

    const width = Math.max(container.clientWidth, 300);
    const height = Math.max(container.clientHeight, 230);
    const padding = { top: 18, right: 16, bottom: 28, left: 16 };

    const { points, path, min, max } = createLinePath(values, width, height, padding);
    const zeroValue = 0;
    const valueRange = Math.max(max - min, 1e-9);
    const innerH = height - padding.top - padding.bottom;
    const zeroY = padding.top + ((max - zeroValue) / valueRange) * innerH;

    const lastPoint = points[points.length - 1];

    const xLabels = [0, Math.floor(values.length / 2), values.length - 1]
      .map((idx) => {
        const x = points[idx]?.x ?? padding.left;
        return `<text class="fx-axis-label" x="${x}" y="${height - 8}" text-anchor="middle">T-${values.length - 1 - idx}</text>`;
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

    const width = Math.max(container.clientWidth, 300);
    const height = Math.max(container.clientHeight, 220);
    const padding = { top: 20, right: 16, bottom: 32, left: 16 };
    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;
    const barGap = 14;
    const barCount = rows.length;
    const barWidth = (innerW - (barGap * (barCount - 1))) / barCount;
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

  function renderFX() {
    const pair = fxControls.pair?.value || "AUD/USD";
    const spreadMode = fxControls.spreads?.value || "Comparative Spread";
    const horizon = fxControls.horizon?.value || "30D";

    const priceSeries = generateSeries(pair, horizon);
    const spreadSeries = buildSpreadSeries(pair, spreadMode, horizon);
    const sigmaRows = getSigmaTable(horizon);

    const priceLast = priceSeries[priceSeries.length - 1];
    const priceFirst = priceSeries[0];
    const priceChangePct = ((priceLast / priceFirst) - 1) * 100;
    const priceMin = Math.min(...priceSeries);
    const priceMax = Math.max(...priceSeries);

    const spreadLast = spreadSeries[spreadSeries.length - 1];
    const spreadAvg = spreadSeries.reduce((a, b) => a + b, 0) / spreadSeries.length;

    const sigmaRow = sigmaRows.find((row) => row.pair === pair);

    const priceDigits = getPairDigits(pair);

    const priceBadge = document.getElementById("fx-price-badge");
    const spreadBadge = document.getElementById("fx-spread-badge");
    const sigmaBadge = document.getElementById("fx-sigma-badge");

    if (priceBadge) priceBadge.textContent = `${pair} • ${horizon}`;
    if (spreadBadge) spreadBadge.textContent = spreadMode;
    if (sigmaBadge) sigmaBadge.textContent = `${horizon} Cross-Pair Z Context`;

    updateStatValue(
      document.getElementById("fx-price-last"),
      formatNumber(priceLast, priceDigits)
    );

    updateStatValue(
      document.getElementById("fx-price-change"),
      `${priceChangePct >= 0 ? "+" : ""}${formatNumber(priceChangePct, 2)}%`,
      priceChangePct >= 0 ? "positive" : "negative"
    );

    updateStatValue(
      document.getElementById("fx-price-range"),
      `${formatNumber(priceMin, priceDigits)} - ${formatNumber(priceMax, priceDigits)}`
    );

    updateStatValue(
      document.getElementById("fx-spread-mode"),
      spreadMode === "Comparative Spread" ? "Comparative" : spreadMode
    );

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

    updateStatValue(document.getElementById("fx-sigma-selected"), pair);
    updateStatValue(
      document.getElementById("fx-sigma-z"),
      sigmaRow ? `${sigmaRow.z >= 0 ? "+" : ""}${sigmaRow.z.toFixed(2)}` : "--",
      sigmaRow && sigmaRow.z >= 0 ? "positive" : "negative"
    );
    updateStatValue(
      document.getElementById("fx-sigma-rank"),
      sigmaRow ? `${sigmaRow.rank}/${sigmaRows.length}` : "--"
    );

    renderPriceChart(document.getElementById("fx-price-chart"), priceSeries);
    renderSpreadChart(document.getElementById("fx-spread-chart"), spreadSeries);
    renderSigmaChart(document.getElementById("fx-sigma-chart"), sigmaRows, pair);
  }

  showView("what-is");
});
