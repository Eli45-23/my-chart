const chartEl = document.getElementById("chart");
const legendEl = document.getElementById("legend");
const statusEl = document.getElementById("status");
const errorEl = document.getElementById("error");
const countdownEl = document.getElementById("countdown");
const streamStatusEl = document.getElementById("streamStatus");
const chartEmptyEl = document.getElementById("chartEmpty");
const tfButtons = document.querySelectorAll(".tf-btn");
const toggleButtons = document.querySelectorAll(".toggle-btn[data-layer]");
const cleanModeToggle = document.getElementById("cleanModeToggle");
const symbolInput = document.getElementById("symbolInput");
const loadSymbolButton = document.getElementById("loadSymbolButton");
const chartTitleEl = document.getElementById("chartTitle");
const chartSubtitleEl = document.getElementById("chartSubtitle");
const lineAuditToggle = document.getElementById("lineAuditToggle");
const lineAuditPanel = document.getElementById("lineAuditPanel");
const lineAuditClose = document.getElementById("lineAuditClose");
const lineAuditList = document.getElementById("lineAuditList");
const lineAuditMeta = document.getElementById("lineAuditMeta");
const lineAuditDetail = document.getElementById("lineAuditDetail");

const CLEAN_MODE_STORAGE_KEY = "aaplChartCleanMode";
let cleanMode = true;

try {
  const savedCleanMode = localStorage.getItem(CLEAN_MODE_STORAGE_KEY);
  cleanMode = savedCleanMode === null ? true : savedCleanMode === "true";
} catch (_) {
  cleanMode = true;
}

const layerState = {
  premarket: true,
  previousDay: true,
  vwap: true,
  emas: true,
  sr: true,
  supplyDemand: true,
  weakZones: false,
  liquiditySweeps: true,
  clusters: true,
  reactionZones: true,
  confirmationSetups: true,
};

const COLORS = {
  pmhPml: "#c6a45e",
  previousHighLow: "#6f8eaf",
  previousClose: "#857a9d",
  resistance: "#b98662",
  support: "#5e8eae",
  weakResistance: "#695d50",
  weakSupport: "#465866",
  supply: "#b85f66",
  demand: "#4e9b88",
  weakSupply: "#67464b",
  weakDemand: "#3d6157",
  demandReaction: "#5d9f8f",
  supplyReaction: "#ad686d",
  failedZone: "#59626e",
  trigger: "#c3a35d",
  invalidation: "#727d8b",
  liquiditySweep: "#8a668f",
  liquiditySweepAlt: "#656b98",
  upsideCluster: "#ad8755",
  downsideCluster: "#568d95",
  supportWatch: "#668ca7",
  resistanceWatch: "#a88a67",
  confirmationWatch: "#88708e",
  confirmationConfirmed: "#c3a35d",
  confirmationInvalid: "#727d8b",
  aiEntryBullish: "#62ad91",
  aiEntryBearish: "#cf7373",
};

let activeTimeframe = "1Min";
let activeSymbol = "AAPL";
let eventSource = null;
let didInitialLoad = false;
let latestPayload = null;
let aiEntryPriceLine = null;
let labeledPrices = [];

const timeframeSeconds = {
  "1Min": 60,
  "5Min": 300,
  "15Min": 900,
};

const chartTimeEtFormatter = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const chartDateEtFormatter = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  month: "short",
  day: "numeric",
});

function chartTimeToDate(time) {
  if (typeof time === "number") return new Date(time * 1000);
  if (typeof time === "string") return new Date(time);
  if (time && typeof time === "object" && time.year && time.month && time.day) {
    return new Date(Date.UTC(time.year, time.month - 1, time.day, 12));
  }
  return new Date(NaN);
}

function formatChartTimeET(time, showDate = false) {
  const date = chartTimeToDate(time);
  if (Number.isNaN(date.getTime())) return "";
  return showDate ? chartDateEtFormatter.format(date) : chartTimeEtFormatter.format(date);
}

const chart = LightweightCharts.createChart(chartEl, {
  layout: {
    background: { color: "#090e15" },
    textColor: "#9ba8b8",
    attributionLogo: false,
  },
  grid: {
    vertLines: { color: "#141c27" },
    horzLines: { color: "#141c27" },
  },
  rightPriceScale: {
    borderColor: "#202c3b",
    scaleMargins: {
      top: 0.08,
      bottom: 0.08,
    },
  },
  timeScale: {
    borderColor: "#202c3b",
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 8,
    barSpacing: 8,
    minBarSpacing: 3,
    tickMarkFormatter: (time, tickMarkType) => formatChartTimeET(time, tickMarkType <= 2),
  },
  localization: {
    timeFormatter: (time) => formatChartTimeET(time),
  },
  crosshair: {
    mode: LightweightCharts.CrosshairMode.Normal,
    vertLine: {
      color: "#59677a",
      width: 1,
      style: LightweightCharts.LineStyle.Dashed,
      labelBackgroundColor: "#243143",
    },
    horzLine: {
      color: "#59677a",
      width: 1,
      style: LightweightCharts.LineStyle.Dashed,
      labelBackgroundColor: "#243143",
    },
  },
});

const candleSeries = chart.addCandlestickSeries({
  upColor: "#36a99a",
  downColor: "#d85c5c",
  borderVisible: true,
  borderUpColor: "#2b8d83",
  borderDownColor: "#b84f52",
  wickVisible: true,
  wickUpColor: "#64b9ae",
  wickDownColor: "#df7470",
  priceLineColor: "#67a99f",
  priceLineStyle: LightweightCharts.LineStyle.Dotted,
});

const vwapSeries = chart.addLineSeries({
  color: "#d2aa53",
  lineWidth: 2,
  priceLineVisible: false,
  title: "VWAP",
});

const ema9Series = chart.addLineSeries({
  color: "#5f91c1",
  lineWidth: 1,
  priceLineVisible: false,
  title: "EMA9",
});

const ema20Series = chart.addLineSeries({
  color: "#8a719f",
  lineWidth: 1,
  priceLineVisible: false,
  title: "EMA20",
});

let priceLines = [];

function focusRecentCandles(candles) {
  const count = Array.isArray(candles) ? candles.length : 0;
  if (!count) return;

  const visibleBars = activeTimeframe === "1Min" ? 190 : activeTimeframe === "5Min" ? 130 : 90;
  if (count <= visibleBars) {
    chart.timeScale().fitContent();
    return;
  }

  chart.timeScale().setVisibleLogicalRange({
    from: count - visibleBars,
    to: count + 8,
  });
}

function isLayerVisible(layer) {
  const cleanModeHiddenLayers = ["liquiditySweeps", "clusters", "reactionZones"];
  return layerState[layer] && !(cleanMode && cleanModeHiddenLayers.includes(layer));
}

function isWeakZone(zone) {
  return zone?.zone_quality_grade === "WEAK" || (zone?.label || "").includes("Weak Zone");
}

function lineAnchorPrice(line) {
  if (line?.price !== null && line?.price !== undefined && Number.isFinite(Number(line.price))) return Number(line.price);
  if (line?.top !== null && line?.top !== undefined && line?.bottom !== null && line?.bottom !== undefined &&
      Number.isFinite(Number(line.top)) && Number.isFinite(Number(line.bottom))) {
    return (Number(line.top) + Number(line.bottom)) / 2;
  }
  return null;
}

function isLineNearCurrentPrice(line, currentPrice, symbol = activeSymbol) {
  const anchor = lineAnchorPrice(line);
  const price = Number(currentPrice);
  if (!Number.isFinite(anchor) || !Number.isFinite(price) || price <= 0) return false;
  const etfs = new Set(["SPY", "QQQ", "IWM", "DIA"]);
  const maxPercent = etfs.has(String(symbol).toUpperCase()) ? 0.006 : 0.0075;
  return Math.abs(anchor - price) / price <= maxPercent;
}

function isValidAuditLine(line) {
  return Boolean(
    line?.id && line?.type && line?.label && line?.source && line?.reason && line?.status &&
    [1, 2, 3].includes(line?.priority) && Number.isFinite(lineAnchorPrice(line))
  );
}

function lineDisplayDecision(line) {
  if (!isValidAuditLine(line)) return { visible: false, hiddenReason: "invalid audit metadata" };
  if (!line.visible_in_full_mode) return { visible: false, hiddenReason: "not visible in full mode" };
  if (!cleanMode) return { visible: true, hiddenReason: null };

  const alwaysVisible = new Set(["VWAP", "EMA9", "EMA20"]);
  if (alwaysVisible.has(line.type)) return { visible: true, hiddenReason: null };
  if (!line.source || !line.reason) return { visible: false, hiddenReason: "no valid source" };
  if (line.status === "FAILED") return { visible: false, hiddenReason: "failed" };
  if (line.strength === "WEAK") return { visible: false, hiddenReason: "weak" };
  if (line.priority === 3) return { visible: false, hiddenReason: "low priority" };
  if (!line.visible_in_clean_mode) return { visible: false, hiddenReason: "not actionable in Clean Mode" };
  if (!isLineNearCurrentPrice(line, latestPayload?.current_price || latestPayload?.latest_trade?.price, activeSymbol)) {
    return { visible: false, hiddenReason: "distant" };
  }
  const anchor = lineAnchorPrice(line);
  const duplicateNearby = (latestPayload?.chart_lines || []).some(other =>
    other.id !== line.id && isValidAuditLine(other) && (other.priority || 3) < (line.priority || 3) &&
    Number.isFinite(lineAnchorPrice(other)) && Math.abs(lineAnchorPrice(other) - anchor) < 0.025
  );
  if (duplicateNearby) return { visible: false, hiddenReason: "duplicate nearby level" };
  return { visible: true, hiddenReason: null };
}

function chartSupplyDemandZones(zones) {
  const available = zones || [];

  if (cleanMode) {
    const currentPrice = latestPayload?.current_price || latestPayload?.latest_trade?.price;
    return available.filter(zone => {
      if (!["HOLD", "RECLAIM", "REJECTION"].includes(zone.reaction_status)) return false;
      return isLineNearCurrentPrice({ top: zone.high, bottom: zone.low }, currentPrice, activeSymbol);
    });
  }

  return available.filter(zone => !(isWeakZone(zone) && !layerState.weakZones));
}

function updateCleanModeControl() {
  cleanModeToggle.classList.toggle("active", cleanMode);
  cleanModeToggle.setAttribute("aria-pressed", String(cleanMode));
  cleanModeToggle.textContent = `Clean Mode: ${cleanMode ? "On" : "Off"}`;

  toggleButtons.forEach((btn) => {
    const suppressedLayers = ["weakZones", "liquiditySweeps", "clusters", "reactionZones"];
    const suppressed = cleanMode && suppressedLayers.includes(btn.dataset.layer);
    btn.classList.toggle("clean-mode-suppressed", suppressed);
    btn.title = suppressed ? "Hidden while Clean Mode is on" : "";
  });
}

function applyIndicatorVisibility() {
  const auditTypeVisible = type => (latestPayload?.chart_lines || []).some(line =>
    line.type === type && lineDisplayDecision(line).visible
  );
  vwapSeries.applyOptions({ visible: layerState.vwap && auditTypeVisible("VWAP"), lineWidth: 2 });
  ema9Series.applyOptions({ visible: layerState.emas && auditTypeVisible("EMA9"), lineWidth: cleanMode ? 2 : 1 });
  ema20Series.applyOptions({ visible: layerState.emas && auditTypeVisible("EMA20"), lineWidth: 1 });
}

function clearPriceLines() {
  for (const line of priceLines) {
    candleSeries.removePriceLine(line);
  }
  priceLines = [];
  labeledPrices = [];
}

function removeAiEntryMarker() {
  if (!aiEntryPriceLine) return;
  candleSeries.removePriceLine(aiEntryPriceLine);
  aiEntryPriceLine = null;
}

function isValidAiEntryReview(review) {
  const marker = review?.entry_marker;
  const label = marker?.label;
  return (
    review?.allow_entry_marker === true &&
    review?.read_only === true &&
    review?.not_an_order === true &&
    typeof marker?.price === "number" &&
    Number.isFinite(marker.price) &&
    ["bullish", "bearish"].includes(marker.direction) &&
    typeof label === "string" &&
    label.includes("ENTER TRADE SETUP") &&
    label.includes("POSSIBLE ENTRY — NOT AN ORDER")
  );
}

function renderAiEntryMarker(review) {
  removeAiEntryMarker();
  if (!isValidAiEntryReview(review)) return;

  const marker = review.entry_marker;
  const directionLabel = marker.direction === "bullish" ? "CALL" : "PUT";
  const title = [
    "ENTER TRADE SETUP",
    "POSSIBLE ENTRY — NOT AN ORDER",
    directionLabel,
  ].filter(Boolean).join(" | ");

  aiEntryPriceLine = candleSeries.createPriceLine({
    price: marker.price,
    color: marker.direction === "bullish" ? COLORS.aiEntryBullish : COLORS.aiEntryBearish,
    lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    axisLabelVisible: true,
    title,
  });
}

async function refreshAiEntryMarker() {
  try {
    const response = await fetch(`/api/ai/latest-review?symbol=${encodeURIComponent(activeSymbol)}`);
    if (!response.ok) {
      removeAiEntryMarker();
      return;
    }
    renderAiEntryMarker(await response.json());
  } catch (_) {
    removeAiEntryMarker();
  }
}

function visibleAuditLines() {
  return (latestPayload?.chart_lines || []).filter(line => lineDisplayDecision(line).visible);
}

function findAuditLineForPlot(label, price) {
  const numericPrice = Number(price);
  const words = String(label || "").toUpperCase().split(/[^A-Z0-9]+/).filter(word => word.length > 2);
  return (latestPayload?.chart_lines || [])
    .filter(line => {
      const values = [line.price, line.top, line.bottom].filter(value => value !== null && value !== undefined).map(Number);
      const anchor = lineAnchorPrice(line);
      return values.some(value => Number.isFinite(value) && Math.abs(value - numericPrice) < 0.011) ||
        (Number.isFinite(anchor) && Math.abs(anchor - numericPrice) < 0.011);
    })
    .sort((a, b) => {
      const aWords = `${a.type} ${a.label} ${a.short_label}`.toUpperCase();
      const bWords = `${b.type} ${b.label} ${b.short_label}`.toUpperCase();
      const aMatch = words.filter(word => aWords.includes(word)).length;
      const bMatch = words.filter(word => bWords.includes(word)).length;
      return bMatch - aMatch || (a.priority || 3) - (b.priority || 3);
    })[0] || null;
}

function addLevel(label, price, color, style = LightweightCharts.LineStyle.Solid, showLabel = true, lineWidth = 1) {
  if (price === null || price === undefined) return;
  const numericPrice = Number(price);
  const auditLine = findAuditLineForPlot(label, numericPrice);
  if (!auditLine) return;
  const decision = lineDisplayDecision(auditLine);
  if (!decision.visible) return;

  const priority = auditLine.priority || 3;
  const strongerNearby = visibleAuditLines().some(line =>
    line.id !== auditLine.id && (line.priority || 3) < priority &&
    Math.abs(lineAnchorPrice(line) - numericPrice) < 0.025
  );
  const overlapsLabel = labeledPrices.some(item =>
    Math.abs(item.price - numericPrice) < 0.022 && item.priority <= priority
  );
  const labelEligible = priority <= 2 && !["FAILED", "MUTED"].includes(auditLine.status) && auditLine.strength !== "WEAK";
  const labelVisible = showLabel && labelEligible && !strongerNearby && !overlapsLabel;
  if (labelVisible) labeledPrices.push({ price: numericPrice, priority });
  const muted = !cleanMode && (priority === 3 || ["FAILED", "MUTED"].includes(auditLine.status) || auditLine.strength === "WEAK");
  const smartLabel = auditLine.short_label || label;
  const line = candleSeries.createPriceLine({
    price: numericPrice,
    color: muted ? `${color}55` : color,
    lineWidth: priority === 1 && !muted ? Math.max(1, lineWidth) : 1,
    lineStyle: muted ? LightweightCharts.LineStyle.Dotted : style,
    axisLabelVisible: labelVisible,
    title: labelVisible ? smartLabel : "",
  });
  priceLines.push(line);
}


function addZoneBand(label, zone, colors) {
  if (!zone) return;

  const low = zone.low;
  const high = zone.high;

  if (low === null || low === undefined || high === null || high === undefined) return;

  const quality = zone.label || "Zone";
  const failed = zone.reaction_status === "FAILED";
  const weak = zone.zone_quality_grade === "WEAK" || failed;
  const zoneColor = failed ? COLORS.failedZone : colors.zone;
  const style = weak ? LightweightCharts.LineStyle.Dotted : LightweightCharts.LineStyle.Dashed;

  if (cleanMode) {
    if (zone.reaction_label && !failed) {
      const reactionColor = zone.type === "demand" ? COLORS.demandReaction : COLORS.supplyReaction;
      addLevel(zone.reaction_label, zone.defended_edge, reactionColor, LightweightCharts.LineStyle.Solid, true, 2);
    }
    return;
  }

  addLevel(`${label} High ${quality}`, high, zoneColor, LightweightCharts.LineStyle.Dotted, false);
  addLevel(`${label} Low ${quality}`, low, zoneColor, LightweightCharts.LineStyle.Dotted, false);

  if (zone.reaction_label && !failed) {
    const reactionColor = zone.type === "demand" ? COLORS.demandReaction : COLORS.supplyReaction;
    addLevel(zone.reaction_label, zone.defended_edge, reactionColor, LightweightCharts.LineStyle.Dashed, true);
  }

  if (zone.trigger !== null && zone.trigger !== undefined) {
    addLevel(`${label} T`, zone.trigger, weak ? zoneColor : COLORS.trigger, style, !weak && !zone.reaction_label);
  }

  if (zone.invalidation !== null && zone.invalidation !== undefined) {
    addLevel(`${label} Invalid`, zone.invalidation, COLORS.invalidation, LightweightCharts.LineStyle.Dotted, false);
  }
}


function addSweepZone(label, zone, color) {
  if (!zone) return;

  const low = zone.low;
  const high = zone.high;
  const price = zone.price;

  if (low !== null && low !== undefined && high !== null && high !== undefined) {
    const mid = (Number(low) + Number(high)) / 2;
    const rangeLabel = `${label}: ${Number(low).toFixed(2)}-${Number(high).toFixed(2)}`;

    // Draw high/low as quiet guide lines without long labels.
    addLevel(`${label} Upper`, high, color, LightweightCharts.LineStyle.Dotted, false);
    addLevel(`${label} Lower`, low, color, LightweightCharts.LineStyle.Dotted, false);

    // Draw one clear labeled center line so labels do not stack.
    addLevel(rangeLabel, mid, color, LightweightCharts.LineStyle.Dashed, true);
  } else if (price !== null && price !== undefined) {
    addLevel(label, price, color, LightweightCharts.LineStyle.Dashed, true);
  }
}function pill(label, value) {
  if (value === null || value === undefined) return "";
  return `<span class="pill">${label}: ${Number(value).toFixed(2)}</span>`;
}


function addClusterZone(label, cluster, color) {
  if (!cluster) return;

  const low = cluster.low;
  const high = cluster.high;

  if (low === null || low === undefined || high === null || high === undefined) return;

  const mid = (Number(low) + Number(high)) / 2;
  const rangeLabel = `${label}: ${Number(low).toFixed(2)}-${Number(high).toFixed(2)}`;

  addLevel(`${label} Upper`, high, color, LightweightCharts.LineStyle.Dotted, false);
  addLevel(`${label} Lower`, low, color, LightweightCharts.LineStyle.Dotted, false);
  addLevel(rangeLabel, mid, color, LightweightCharts.LineStyle.Solid, true);
}


function addReactionZone(label, zone, color) {
  if (!zone) return;

  const low = zone.low;
  const high = zone.high;

  if (low === null || low === undefined || high === null || high === undefined) return;

  const mid = (Number(low) + Number(high)) / 2;
  const score = zone.score !== undefined ? ` ${zone.score}` : "";
  const rangeLabel = `${label}${score}: ${Number(low).toFixed(2)}-${Number(high).toFixed(2)}`;

  addLevel(`${label} Watch Upper`, high, color, LightweightCharts.LineStyle.Dotted, false);
  addLevel(`${label} Watch Lower`, low, color, LightweightCharts.LineStyle.Dotted, false);
  addLevel(rangeLabel, mid, color, LightweightCharts.LineStyle.Dashed, true);
}

function textPill(text, tone = "") {
  return `<span class="pill ${tone}">${text}</span>`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, character => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;",
  }[character]));
}

function linePriceText(line) {
  if (line.price !== null && line.price !== undefined && Number.isFinite(Number(line.price))) return Number(line.price).toFixed(2);
  if (line.bottom !== null && line.bottom !== undefined && line.top !== null && line.top !== undefined &&
      Number.isFinite(Number(line.bottom)) && Number.isFinite(Number(line.top))) {
    return `${Number(line.bottom).toFixed(2)}-${Number(line.top).toFixed(2)}`;
  }
  return "n/a";
}

function renderLineAudit(selectedId = null) {
  if (!lineAuditList) return;
  const lines = (latestPayload?.chart_lines || []).sort((a, b) =>
    (a.priority || 3) - (b.priority || 3) || String(a.short_label).localeCompare(String(b.short_label))
  );
  const visibleCount = lines.filter(line => lineDisplayDecision(line).visible).length;
  lineAuditMeta.textContent = `${activeSymbol} · ${activeTimeframe} · ${visibleCount}/${lines.length} visible deterministic items`;
  lineAuditList.innerHTML = lines.map(line => {
    const decision = lineDisplayDecision(line);
    return `
    <div class="audit-item ${!decision.visible || line.status === "FAILED" || line.status === "MUTED" ? "muted" : ""} ${line.id === selectedId ? "selected" : ""}" data-line-id="${escapeHtml(line.id)}" title="${escapeHtml(line.reason)}">
      <div class="audit-row"><span class="audit-label">${escapeHtml(line.short_label)}</span><span>${escapeHtml(linePriceText(line))}</span></div>
      <div class="audit-row audit-sub"><span>${escapeHtml(line.type)}</span><span>${decision.visible ? "Visible" : `Hidden: ${escapeHtml(decision.hiddenReason)}`} · P${escapeHtml(line.priority)}</span></div>
    </div>
  `}).join("") || `<div class="audit-meta">No registered chart lines.</div>`;
  const selected = lines.find(line => line.id === selectedId);
  const selectedDecision = selected ? lineDisplayDecision(selected) : null;
  lineAuditDetail.classList.toggle("visible", Boolean(selected));
  lineAuditDetail.innerHTML = selected ? `
    <strong>${escapeHtml(selected.label)}</strong><br>
    Price / range: ${escapeHtml(linePriceText(selected))}<br>
    Visible in Clean Mode: ${selected.visible_in_clean_mode ? "Yes" : "No"}<br>
    Current display: ${selectedDecision.visible ? "Visible" : `Hidden - ${escapeHtml(selectedDecision.hiddenReason)}`}<br>
    Priority: ${escapeHtml(selected.priority)}<br>
    Source: ${escapeHtml(selected.source)}<br>
    Method: ${escapeHtml(selected.calculation_method)}<br>
    Reason: ${escapeHtml(selected.reason)}<br>
    Status: ${escapeHtml(selected.status)} · Strength: ${escapeHtml(selected.strength)} · Confidence: ${escapeHtml(selected.confidence ?? "n/a")}%<br>
    Defended edge: ${escapeHtml(selected.defended_edge ?? "n/a")} · Failure edge: ${escapeHtml(selected.failure_edge ?? "n/a")}<br>
    Timeframe: ${escapeHtml(selected.timeframe)}<br>
    Warnings: ${escapeHtml((selected.warnings || []).join(" | ") || "none")}<br>
    Read-only chart context.
  ` : "";
}

function legendGroup(title, items, className = "") {
  return `<section class="legend-group ${className}"><span class="legend-group-title">${title}</span><div class="legend-pills">${items.filter(Boolean).join("")}</div></section>`;
}

function warningTone(text) {
  const value = String(text || "").toUpperCase();
  if (value.includes("NO_TRADE") || value.includes("CHOP") || value.includes("FAILED") || value.includes("INVALIDATED")) return "alert";
  if (value.includes("WAIT") || value.includes("WARNING") || value.includes("MIXED")) return "caution";
  return "";
}

function updateChartEmptyState(candles) {
  if (!chartEmptyEl) return;
  chartEmptyEl.classList.toggle("visible", !Array.isArray(candles) || candles.length === 0);
}

function normalizeSymbolInput(value) {
  const symbol = String(value || "").trim().toUpperCase();
  return /^[A-Z][A-Z0-9.-]{0,9}$/.test(symbol) ? symbol : null;
}

function updateSymbolUi() {
  symbolInput.value = activeSymbol;
  chartTitleEl.textContent = `${activeSymbol} Live Trading Review`;
  chartSubtitleEl.textContent = `${activeSymbol} · Read-only AI-assisted chart review · Eastern Time`;
}

function loadSelectedSymbol() {
  const symbol = normalizeSymbolInput(symbolInput.value);
  if (!symbol) {
    errorEl.textContent = "Enter a valid stock or ETF symbol.";
    symbolInput.focus();
    return;
  }
  activeSymbol = symbol;
  updateSymbolUi();
  reloadForTimeframe();
}

function formatPrice(value) {
  return value === null || value === undefined ? "n/a" : Number(value).toFixed(2);
}

function formatRiskReward(setup, detailed = false) {
  const rr = setup?.risk_reward;
  if (!rr) return "RR: n/a";

  const summary = [
    `RR: ${rr.rr_grade || "n/a"}`,
    `R1 ${rr.rr_1 ?? "n/a"}`,
    `R2 ${rr.rr_2 ?? "n/a"}`,
    `Entry ${formatPrice(rr.suggested_entry)}`,
    `Stop ${formatPrice(rr.invalidation)}`,
    `T1 ${formatPrice(rr.target_1)}`,
    `T2 ${formatPrice(rr.target_2)}`,
  ];

  if (detailed) {
    summary.push(
      `R3 ${rr.rr_3 ?? "n/a"}`,
      `T3 ${formatPrice(rr.target_3)}`,
      `Opposing ${rr.nearest_opposing_level?.label || "n/a"} ${formatPrice(rr.nearest_opposing_level?.price)}`,
      `Room ${formatPrice(rr.room_to_opposing_level)}`,
      `RR Warnings ${(rr.rr_warnings || []).join(", ") || "none"}`
    );
  }

  return summary.join(" | ");
}

function secondsLeftInCandle() {
  const now = new Date();
  const nowEtString = now.toLocaleString("en-US", { timeZone: "America/New_York" });
  const nowEt = new Date(nowEtString);

  const secondsSinceMidnight =
    nowEt.getHours() * 3600 +
    nowEt.getMinutes() * 60 +
    nowEt.getSeconds();

  const tf = timeframeSeconds[activeTimeframe] || 60;
  const left = tf - (secondsSinceMidnight % tf);

  return left === tf ? 0 : left;
}

function updateCountdown() {
  const left = secondsLeftInCandle();
  const mm = String(Math.floor(left / 60)).padStart(2, "0");
  const ss = String(left % 60).padStart(2, "0");
  const label = activeTimeframe === "1Min" ? "1m" : activeTimeframe === "5Min" ? "5m" : "15m";
  countdownEl.textContent = `Next ${label} candle: ${mm}:${ss}`;
}

function updateLegend(data) {
  latestPayload = data || latestPayload;
  if (!latestPayload) return;
  renderLineAudit();

  const levels = latestPayload.levels || {};
  const indicators = latestPayload.indicators || {};
  const latestVWAP = indicators.vwap?.length ? indicators.vwap[indicators.vwap.length - 1].value : null;
  const latestEMA9 = indicators.ema9?.length ? indicators.ema9[indicators.ema9.length - 1].value : null;
  const latestEMA20 = indicators.ema20?.length ? indicators.ema20[indicators.ema20.length - 1].value : null;
  const stream = latestPayload.stream_status || {};
  const chartSession = latestPayload.chart_session || {};
  const trade = latestPayload.latest_trade;
  const setups = latestPayload.confirmation_setups?.setups || [];
  const demandZones = chartSupplyDemandZones(latestPayload.supply_demand?.demand);
  const supplyZones = chartSupplyDemandZones(latestPayload.supply_demand?.supply);
  const bestSetup = setups.reduce((best, setup) => {
    const score = setup.professional_score ?? setup.score ?? 0;
    const bestScore = best?.professional_score ?? best?.score ?? -1;
    return score > bestScore ? setup : best;
  }, null);

  const sessionItems = [
    textPill(`Timeframe ${activeTimeframe.replace("Min", "m")}`),
    chartSession.is_historical ? textPill(`${chartSession.label || "PREVIOUS SESSION"} · ${chartSession.date || "n/a"}`, "caution") : "",
    pill("Current", latestPayload.current_price || trade?.price),
    trade?.timestamp ? textPill(`Trade ${new Date(trade.timestamp).toLocaleTimeString("en-US", { timeZone: "America/New_York" })} ET`) : "",
    textPill(levels.premarket_window || "Premarket 04:00-09:30 ET"),
  ];
  const levelItems = [
    pill("PMH", levels.pmh), pill("PML", levels.pml), pill("PDH", levels.pdh), pill("PDL", levels.pdl), pill("PDC", levels.pdc),
    pill("VWAP", latestVWAP), pill("EMA9", latestEMA9), cleanMode ? "" : pill("EMA20", latestEMA20),
    cleanMode ? "" : textPill(`Support ${(latestPayload.support_resistance?.support || []).map(x => `${x.price.toFixed(2)} ${x.quality_grade || x.reliability_label || ""}`).join(", ") || "none"}`),
    cleanMode ? "" : textPill(`Resistance ${(latestPayload.support_resistance?.resistance || []).map(x => `${x.price.toFixed(2)} ${x.quality_grade || x.reliability_label || ""}`).join(", ") || "none"}`),
    textPill(`Demand ${demandZones.map(z => `${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.reaction_label || z.zone_quality_grade || ""}`).join(", ") || "none"}`),
    textPill(`Supply ${supplyZones.map(z => `${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.reaction_label || z.zone_quality_grade || ""}`).join(", ") || "none"}`),
  ];
  const researchItems = [
    cleanMode ? "" : textPill(`Reaction Zones ${[
      ...(latestPayload.structure_reactions?.resistance_watch || []).map(z => `Watch R ${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.score || ""}`),
      ...(latestPayload.structure_reactions?.support_watch || []).map(z => `Watch S ${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.score || ""}`),
    ].join(" | ") || "none"}`),
    cleanMode ? "" : textPill(`Sweeps ${[...(latestPayload.liquidity_sweeps?.upside || []), ...(latestPayload.liquidity_sweeps?.downside || [])].length || "none"}`),
    cleanMode ? "" : textPill(`Clusters ${(latestPayload.level_clusters?.clusters || []).length || "none"}`),
  ];
  const regime = latestPayload.professional_context?.aapl?.regime || {};
  const marketItems = [
    textPill(`Regime ${regime.regime || "n/a"} · ${regime.regime_confidence || "LOW"}`, warningTone(regime.regime)),
    textPill(`Action ${regime.action_label || "WAIT_FOR_BREAKOUT"}`, warningTone(regime.action_label)),
    textPill(`Market ${latestPayload.professional_context?.market_confirmation?.market_confirmation || latestPayload.professional_context?.market_alignment || "n/a"}`, warningTone(latestPayload.professional_context?.market_confirmation?.market_confirmation)),
    textPill(`${latestPayload.related_market_symbols?.primary_market || "SPY"} ${latestPayload.professional_context?.market_confirmation?.primary_market_bias || latestPayload.professional_context?.market_confirmation?.spy_bias || "n/a"} · ${latestPayload.related_market_symbols?.secondary_market || "QQQ"} ${latestPayload.professional_context?.market_confirmation?.secondary_market_bias || latestPayload.professional_context?.market_confirmation?.qqq_bias || "n/a"}`),
    textPill(`${activeSymbol} ${latestPayload.professional_context?.selected?.trend?.label || latestPayload.professional_context?.aapl?.trend?.label || "n/a"} · RVOL ${latestPayload.professional_context?.selected?.rvol ?? latestPayload.professional_context?.aapl?.rvol ?? "n/a"}x · ATR14 ${latestPayload.professional_context?.selected?.atr14 ?? latestPayload.professional_context?.aapl?.atr14 ?? "n/a"}`),
  ];
  const setupStatus = latestPayload.confirmation_setups?.status || "NO_SETUP";
  const setupItems = [
    textPill(`Pro Grade ${latestPayload.professional_context?.professional_grade || "n/a"}`, warningTone(latestPayload.professional_context?.professional_grade)),
    textPill(`Setup ${setupStatus}`, warningTone(setupStatus)),
    textPill(`Best ${latestPayload.confirmation_setups?.best_grade || bestSetup?.professional_grade || "NO_TRADE"} · ${bestSetup?.professional_score ?? bestSetup?.score ?? 0}`, warningTone(latestPayload.confirmation_setups?.best_grade || bestSetup?.professional_grade)),
    textPill(`Trend Filter ${latestPayload.confirmation_setups?.trend?.label || "n/a"}`),
    textPill(`Setups ${setups.map(s => `${s.professional_grade || ""} ${s.confirmation_stage === "EARLY_CONFIRM" ? "EARLY" : (s.confirmation_stage || s.status)} · ${formatRiskReward(s, !cleanMode)}`).join(" || ") || "none"}`, warningTone(setupStatus)),
  ];
  const warningText = (latestPayload.professional_context?.warnings || []).join(" | ") || "No active warnings";
  const riskItems = [
    textPill(warningText, warningText === "No active warnings" ? "" : "alert"),
    textPill(`Chop ${regime.chop_score ?? "n/a"}`, Number(regime.chop_score || 0) >= 60 ? "alert" : ""),
    textPill(`Logger setups +${latestPayload.setup_logging?.logged_setups ?? 0} · outcomes +${latestPayload.setup_logging?.outcomes_evaluated ?? 0}`),
    textPill("Read-only · manual confirmation required"),
  ];

  legendEl.innerHTML = [
    legendGroup("Session / Timeframe", sessionItems),
    legendGroup("Key Levels", [...levelItems, ...researchItems]),
    legendGroup("Market Context", marketItems),
    legendGroup("Setup Quality", setupItems, "setup"),
    legendGroup("Risk / Warnings", riskItems, "warnings"),
  ].join("");

  streamStatusEl.textContent = stream.connected ? "Stream connected" : `Stream ${stream.error || "waiting"}`;
  streamStatusEl.classList.toggle("connected", Boolean(stream.connected));
  streamStatusEl.classList.toggle("reconnecting", !stream.connected);
}


function addConfirmationSetup(label, setup) {
  if (!setup) return;

  const price = setup.level_price;
  if (price === null || price === undefined) return;

  let color = COLORS.confirmationWatch;
  let style = LightweightCharts.LineStyle.Dashed;
  const stage = setup.confirmation_stage || setup.status || "WATCH";
  const stageLabel = stage === "EARLY_CONFIRM" ? "EARLY" : stage;
  if (cleanMode && stage !== "CONFIRMED") return;

  if (stage === "CONFIRMED") {
    color = COLORS.confirmationConfirmed;
    style = LightweightCharts.LineStyle.Solid;
  }

  if (stage === "FAILED" || setup.status === "INVALIDATED") {
    color = COLORS.confirmationInvalid;
    style = LightweightCharts.LineStyle.Dotted;
  }

  addLevel(
    `${stageLabel} ${String(setup.direction || "").toUpperCase()} ${setup.source || label} RR ${setup.risk_reward?.rr_grade || "n/a"} R1 ${setup.risk_reward?.rr_1 ?? "n/a"} R2 ${setup.risk_reward?.rr_2 ?? "n/a"}`,
    setup.trigger ?? price,
    color,
    style,
    true
  );

  if (setup.trigger !== null && setup.trigger !== undefined) {
    addLevel(`${label} Trigger`, setup.trigger, color, LightweightCharts.LineStyle.Dashed, false);
  }

  if (setup.invalidation !== null && setup.invalidation !== undefined) {
    addLevel(`${label} Invalid`, setup.invalidation, COLORS.confirmationInvalid, LightweightCharts.LineStyle.Dotted, false);
  }
}

function drawStaticLevels(data) {
  clearPriceLines();

  const levels = data.levels || {};

  if (layerState.premarket) {
    addLevel("PMH", levels.pmh, COLORS.pmhPml);
    addLevel("PML", levels.pml, COLORS.pmhPml);
  }

  if (layerState.previousDay) {
    addLevel("PDH", levels.pdh, COLORS.previousHighLow, LightweightCharts.LineStyle.Dashed);
    addLevel("PDL", levels.pdl, COLORS.previousHighLow, LightweightCharts.LineStyle.Dashed);
    addLevel("PDC", levels.pdc, COLORS.previousClose, LightweightCharts.LineStyle.Dotted);
  }

  if (isLayerVisible("sr")) {
    const sr = data.support_resistance || {};
    const visibleLevels = (levels) => cleanMode
      ? (levels || []).filter(level => ["A", "B"].includes(level.quality_grade))
      : (levels || []);

    visibleLevels(sr.resistance).forEach((level, index) => {
      const weak = level.quality_grade === "WEAK";
      addLevel(
        `R${index + 1} ${level.quality_grade || level.reliability_label || ""} ${level.quality_score ?? level.reliability_score ?? ""}`,
        level.price,
        weak ? COLORS.weakResistance : COLORS.resistance,
        weak ? LightweightCharts.LineStyle.Dotted : LightweightCharts.LineStyle.Dashed,
        !weak
      );
    });

    visibleLevels(sr.support).forEach((level, index) => {
      const weak = level.quality_grade === "WEAK";
      addLevel(
        `S${index + 1} ${level.quality_grade || level.reliability_label || ""} ${level.quality_score ?? level.reliability_score ?? ""}`,
        level.price,
        weak ? COLORS.weakSupport : COLORS.support,
        weak ? LightweightCharts.LineStyle.Dotted : LightweightCharts.LineStyle.Dashed,
        !weak
      );
    });
  }

  if (layerState.supplyDemand) {
    const sd = data.supply_demand || {};

    chartSupplyDemandZones(sd.supply).forEach((zone, index) => {
      addZoneBand(`Supply ${index + 1}`, zone, { zone: isWeakZone(zone) ? COLORS.weakSupply : COLORS.supply });
    });

    chartSupplyDemandZones(sd.demand).forEach((zone, index) => {
      addZoneBand(`Demand ${index + 1}`, zone, { zone: isWeakZone(zone) ? COLORS.weakDemand : COLORS.demand });
    });
  }

  if (isLayerVisible("liquiditySweeps")) {
    const sweeps = data.liquidity_sweeps || {};

    (sweeps.upside || []).forEach((zone, index) => {
      addSweepZone(`Upside Sweep ${zone.source || index + 1}`, zone, COLORS.liquiditySweep);
    });

    (sweeps.downside || []).forEach((zone, index) => {
      addSweepZone(`Downside Sweep ${zone.source || index + 1}`, zone, COLORS.liquiditySweepAlt);
    });
  }

  if (isLayerVisible("clusters")) {
    const clusters = data.level_clusters?.clusters || [];

    clusters.forEach((cluster, index) => {
      const color = cluster.kind === "upside" ? COLORS.upsideCluster : COLORS.downsideCluster;
      const label = cluster.kind === "upside" ? `Upside Cluster ${index + 1}` : `Downside Cluster ${index + 1}`;
      addClusterZone(label, cluster, color);
    });
  }

  if (isLayerVisible("reactionZones")) {
    const reactions = data.structure_reactions || {};

    (reactions.resistance_watch || []).forEach((zone, index) => {
      addReactionZone(`Watch R${index + 1}`, zone, COLORS.resistanceWatch);
    });

    (reactions.support_watch || []).forEach((zone, index) => {
      addReactionZone(`Watch S${index + 1}`, zone, COLORS.supportWatch);
    });
  }

  if (layerState.confirmationSetups) {
    const confirmation = data.confirmation_setups || {};
    (confirmation.setups || []).slice(0, 4).forEach((setup, index) => {
      addConfirmationSetup(`Setup ${index + 1}`, setup);
    });
  }

  applyIndicatorVisibility();
}

async function loadInitialChart() {
  errorEl.textContent = "";
  statusEl.textContent = "Loading chart...";

  const res = await fetch(`/api/chart?symbol=${encodeURIComponent(activeSymbol)}&timeframe=${encodeURIComponent(activeTimeframe)}`);
  const data = await res.json();

  if (!res.ok || data.data_status !== "ok") {
    throw new Error((data.errors || ["Unknown error"]).join(", "));
  }

  latestPayload = data;
  candleSeries.setData(data.candles || []);
  updateChartEmptyState(data.candles);

  const indicators = data.indicators || {};
  vwapSeries.setData(indicators.vwap || []);
  ema9Series.setData(indicators.ema9 || []);
  ema20Series.setData(indicators.ema20 || []);

  drawStaticLevels(data);
  updateLegend(data);

  didInitialLoad = true;

  statusEl.textContent = data.chart_session?.is_historical
    ? `Previous session: ${data.chart_session.date}`
    : `Initial load: ${new Date(data.timestamp).toLocaleString("en-US", {
        timeZone: "America/New_York",
      })} ET`;

  focusRecentCandles(data.candles);
}

function connectStream() {
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource(`/api/stream?symbol=${encodeURIComponent(activeSymbol)}&timeframe=${encodeURIComponent(activeTimeframe)}`);

  eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);

    if (data.type === "live_candle" && data.candle) {
      candleSeries.update(data.candle);
      updateChartEmptyState([data.candle]);

      latestPayload = {
        ...(latestPayload || {}),
        current_price: data.latest_trade?.price,
        latest_trade: data.latest_trade,
        stream_status: data.stream_status,
      };

      updateLegend(latestPayload);
    }

    if (data.type === "heartbeat") {
      latestPayload = {
        ...(latestPayload || {}),
        current_price: data.latest_trade?.price || latestPayload?.current_price,
        latest_trade: data.latest_trade || latestPayload?.latest_trade,
        stream_status: data.stream_status,
      };

      updateLegend(latestPayload);
    }
  };

  eventSource.onerror = () => {
    streamStatusEl.textContent = "Stream: reconnecting...";
  };
}

async function reloadForTimeframe() {
  try {
    didInitialLoad = false;
    candleSeries.setData([]);
    updateChartEmptyState([]);
    vwapSeries.setData([]);
    ema9Series.setData([]);
    ema20Series.setData([]);
    clearPriceLines();

    await loadInitialChart();
    connectStream();
  } catch (err) {
    errorEl.textContent = `Chart error: ${err.message}`;
    statusEl.textContent = "Error loading chart";
    updateChartEmptyState([]);
  }
}

tfButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    activeTimeframe = btn.dataset.tf;

    tfButtons.forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");

    reloadForTimeframe();
  });
});

loadSymbolButton.addEventListener("click", loadSelectedSymbol);
symbolInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") loadSelectedSymbol();
});

toggleButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    const layer = btn.dataset.layer;
    layerState[layer] = !layerState[layer];

    btn.classList.toggle("active", layerState[layer]);

    if (latestPayload) {
      drawStaticLevels(latestPayload);
      updateLegend(latestPayload);
    }

    applyIndicatorVisibility();
  });
});

cleanModeToggle.addEventListener("click", () => {
  cleanMode = !cleanMode;

  try {
    localStorage.setItem(CLEAN_MODE_STORAGE_KEY, String(cleanMode));
  } catch (_) {
    // Clean Mode still works for this session when storage is unavailable.
  }

  updateCleanModeControl();

  if (latestPayload) {
    drawStaticLevels(latestPayload);
    updateLegend(latestPayload);
  }

  applyIndicatorVisibility();
});

lineAuditToggle.addEventListener("click", () => {
  const visible = !lineAuditPanel.classList.contains("visible");
  lineAuditPanel.classList.toggle("visible", visible);
  lineAuditToggle.classList.toggle("active", visible);
  lineAuditToggle.setAttribute("aria-pressed", String(visible));
  if (visible) renderLineAudit();
});

lineAuditClose.addEventListener("click", () => {
  lineAuditPanel.classList.remove("visible");
  lineAuditToggle.classList.remove("active");
  lineAuditToggle.setAttribute("aria-pressed", "false");
});

lineAuditList.addEventListener("click", event => {
  const item = event.target.closest("[data-line-id]");
  if (item) renderLineAudit(item.dataset.lineId);
});

window.addEventListener("resize", () => {
  chart.applyOptions({ width: chartEl.clientWidth });
});

setInterval(updateCountdown, 1000);
setInterval(refreshAiEntryMarker, 30000);

// Refresh static levels and indicators every 30 seconds.
// Live candle movement comes from the stream.
setInterval(() => {
  if (didInitialLoad) {
    fetch(`/api/chart?symbol=${encodeURIComponent(activeSymbol)}&timeframe=${encodeURIComponent(activeTimeframe)}`)
      .then(r => r.json())
      .then(data => {
        if (data.data_status === "ok") {
          latestPayload = data;
          const indicators = data.indicators || {};
          vwapSeries.setData(indicators.vwap || []);
          ema9Series.setData(indicators.ema9 || []);
          ema20Series.setData(indicators.ema20 || []);
          drawStaticLevels(data);
          updateLegend(data);
        }
      })
      .catch(() => {});
  }
}, 30000);

updateCleanModeControl();
updateSymbolUi();
reloadForTimeframe();
refreshAiEntryMarker();
