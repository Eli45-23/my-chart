const chartEl = document.getElementById("chart");
const legendEl = document.getElementById("legend");
const statusEl = document.getElementById("status");
const errorEl = document.getElementById("error");
const countdownEl = document.getElementById("countdown");
const streamStatusEl = document.getElementById("streamStatus");
const dataQualityStatusEl = document.getElementById("dataQualityStatus");
const chartEmptyEl = document.getElementById("chartEmpty");
const fvgOverlay = document.getElementById("fvgOverlay");
const paperTradeOverlay = document.getElementById("paperTradeOverlay");
const keyLevelEdgeMarkers = document.getElementById("keyLevelEdgeMarkers");
const tfButtons = document.querySelectorAll(".tf-btn");
const toggleButtons = document.querySelectorAll(".toggle-btn[data-layer]");
const cleanModeToggle = document.getElementById("cleanModeToggle");
const symbolInput = document.getElementById("symbolInput");
const loadSymbolButton = document.getElementById("loadSymbolButton");
const rebuildChartButton = document.getElementById("rebuildChartButton");
const paperTradeToggle = document.getElementById("paperTradeToggle");
const marketGridToggle = document.getElementById("marketGridToggle");
const marketGridPanel = document.getElementById("marketGridPanel");
const marketGridCardsEl = document.getElementById("marketGridCards");
const marketGridRefreshButton = document.getElementById("marketGridRefresh");
const marketGridCloseButton = document.getElementById("marketGridClose");
const marketGridCleanModeToggle = document.getElementById("marketGridCleanModeToggle");
const marketGridLayerButtons = document.querySelectorAll(".toggle-btn[data-market-grid-layer]");
const rebuildBanner = document.getElementById("rebuildBanner");
const chartTitleEl = document.getElementById("chartTitle");
const chartSubtitleEl = document.getElementById("chartSubtitle");
const lineAuditToggle = document.getElementById("lineAuditToggle");
const lineAuditPanel = document.getElementById("lineAuditPanel");
const lineAuditClose = document.getElementById("lineAuditClose");
const lineAuditList = document.getElementById("lineAuditList");
const lineAuditMeta = document.getElementById("lineAuditMeta");
const lineAuditDetail = document.getElementById("lineAuditDetail");
const candleCompareToggle = document.getElementById("candleCompareToggle");
const candleComparePanel = document.getElementById("candleComparePanel");
const candleCompareClose = document.getElementById("candleCompareClose");
const candleCompareMeta = document.getElementById("candleCompareMeta");
const candleCompareSummary = document.getElementById("candleCompareSummary");
const candleCompareList = document.getElementById("candleCompareList");
const paperTradePanel = document.getElementById("paperTradePanel");
const paperTradeClose = document.getElementById("paperTradeClose");
const paperTradeMeta = document.getElementById("paperTradeMeta");
const paperTradeForm = document.getElementById("paperTradeForm");
const paperSymbolInput = document.getElementById("paperSymbol");
const paperTimeframeInput = document.getElementById("paperTimeframe");
const paperTradeTypeInput = document.getElementById("paperTradeType");
const paperEntryLabel = document.getElementById("paperEntryLabel");
const paperStopLabel = document.getElementById("paperStopLabel");
const paperTargetLabel = document.getElementById("paperTargetLabel");
const paperQuantityLabel = document.getElementById("paperQuantityLabel");
const paperOptionNotice = document.getElementById("paperOptionNotice");
const paperEntryInput = document.getElementById("paperEntry");
const paperStopInput = document.getElementById("paperStop");
const paperTargetInput = document.getElementById("paperTarget");
const paperQuantityInput = document.getElementById("paperQuantity");
const paperNotesInput = document.getElementById("paperNotes");
const paperCloseTradeButton = document.getElementById("paperCloseTrade");
const paperClearTradeButton = document.getElementById("paperClearTrade");
const paperTradeMessage = document.getElementById("paperTradeMessage");
const paperTradeSummary = document.getElementById("paperTradeSummary");
const paperTradeList = document.getElementById("paperTradeList");

const CLEAN_MODE_STORAGE_KEY = "aaplChartCleanMode";
const PAPER_TRADE_STORAGE_KEY = "paperTradePlannerTrades";
const MARKET_GRID_STORAGE_KEY = "marketGridLayout";
const MARKET_GRID_SETTINGS_STORAGE_KEY = "marketGridLayerSettings";
const MARKET_GRID_DEFAULTS = [
  { symbol: "SPY", timeframe: "5Min" },
  { symbol: "AAPL", timeframe: "5Min" },
  { symbol: "QQQ", timeframe: "5Min" },
  { symbol: "AMZN", timeframe: "5Min" },
];
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
  fvg: true,
  weakZones: false,
  liquiditySweeps: true,
  clusters: true,
  reactionZones: true,
  confirmationSetups: true,
};

const MARKET_GRID_LAYER_DEFAULTS = {
  premarket: true,
  previousDay: true,
  vwap: true,
  emas: true,
  sr: true,
  supplyDemand: true,
  fvg: true,
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
  paperEntry: "#d8c27a",
  paperStop: "#d77979",
  paperTarget: "#65b9a6",
  bullFvg: "#5da68f",
  bearFvg: "#c87575",
  weakFvg: "#5f6875",
  hodLod: "#9fb5cf",
  openingRange: "#d1b56d",
};

let activeTimeframe = "1Min";
let activeSymbol = "AAPL";
let eventSource = null;
let didInitialLoad = false;
let latestPayload = null;
let aiEntryPriceLine = null;
let labeledPrices = [];
let paperTrades = [];
let paperTradePriceLines = [];
let marketGridLayout = MARKET_GRID_DEFAULTS.map(card => ({ ...card }));
let marketGridCleanMode = true;
let marketGridLayerState = { ...MARKET_GRID_LAYER_DEFAULTS };
const marketGridCharts = new Map();

const CORE_CLEAN_KEY_LEVEL_TYPES = new Set([
  "PMH", "PML", "PDH", "PDL", "HOD", "LOD", "OPEN 5M HIGH", "OPEN 5M LOW",
]);
const GRID_CORE_LEVEL_TYPES = new Set([
  "PMH", "PML", "PDH", "PDL", "HOD", "LOD", "OPEN 5M HIGH", "OPEN 5M LOW",
]);

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

function loadMarketGridLayout() {
  try {
    const saved = JSON.parse(localStorage.getItem(MARKET_GRID_STORAGE_KEY) || "null");
    if (!Array.isArray(saved) || saved.length !== MARKET_GRID_DEFAULTS.length) return;
    marketGridLayout = saved.map((card, index) => ({
      symbol: normalizeSymbolInput(card?.symbol) || MARKET_GRID_DEFAULTS[index].symbol,
      timeframe: timeframeSeconds[card?.timeframe] ? card.timeframe : MARKET_GRID_DEFAULTS[index].timeframe,
    }));
  } catch (_) {
    marketGridLayout = MARKET_GRID_DEFAULTS.map(card => ({ ...card }));
  }
}

function saveMarketGridLayout() {
  try {
    localStorage.setItem(MARKET_GRID_STORAGE_KEY, JSON.stringify(marketGridLayout));
  } catch (_) {
    // The grid remains usable for this page session if browser storage is unavailable.
  }
}

function loadMarketGridSettings() {
  try {
    const saved = JSON.parse(localStorage.getItem(MARKET_GRID_SETTINGS_STORAGE_KEY) || "null");
    if (!saved || typeof saved !== "object") return;
    marketGridCleanMode = saved.cleanMode !== false;
    marketGridLayerState = {
      ...MARKET_GRID_LAYER_DEFAULTS,
      ...(saved.layers && typeof saved.layers === "object" ? saved.layers : {}),
    };
  } catch (_) {
    marketGridCleanMode = true;
    marketGridLayerState = { ...MARKET_GRID_LAYER_DEFAULTS };
  }
}

function saveMarketGridSettings() {
  try {
    localStorage.setItem(MARKET_GRID_SETTINGS_STORAGE_KEY, JSON.stringify({
      cleanMode: marketGridCleanMode,
      layers: marketGridLayerState,
    }));
  } catch (_) {
    // The grid remains usable for this page session if browser storage is unavailable.
  }
}

function gridLayerVisible(layer) {
  if (!marketGridLayerState[layer]) return false;
  return !marketGridCleanMode || ![
    "vwap", "emas", "weakZones", "liquiditySweeps", "clusters", "reactionZones", "confirmationSetups",
  ].includes(layer);
}

function marketGridModeLabel() {
  return marketGridCleanMode ? "CLEAN MODE" : "FULL MODE";
}

function updateMarketGridControls() {
  if (marketGridCleanModeToggle) {
    marketGridCleanModeToggle.textContent = `Clean Mode: ${marketGridCleanMode ? "On" : "Off"}`;
    marketGridCleanModeToggle.classList.toggle("active", marketGridCleanMode);
    marketGridCleanModeToggle.setAttribute("aria-pressed", String(marketGridCleanMode));
  }
  marketGridLayerButtons.forEach(button => {
    const layer = button.dataset.marketGridLayer;
    const enabled = Boolean(marketGridLayerState[layer]);
    button.classList.toggle("active", enabled);
    button.classList.toggle("clean-mode-suppressed", enabled && !gridLayerVisible(layer));
    button.setAttribute("aria-pressed", String(enabled));
  });
}

function applyMarketGridIndicatorVisibility(card) {
  if (!card) return;
  card.vwap.applyOptions({ visible: gridLayerVisible("vwap") });
  card.ema9.applyOptions({ visible: gridLayerVisible("emas") });
  card.ema20.applyOptions({ visible: gridLayerVisible("emas") });
}

function redrawMarketGridLayers() {
  updateMarketGridControls();
  marketGridCharts.forEach((card, index) => {
    applyMarketGridIndicatorVisibility(card);
    if (card.data) renderMarketGridStructure(index, card.data);
  });
}

function marketGridOpen() {
  return Boolean(marketGridPanel && !marketGridPanel.hidden);
}

function disposeMarketGridCharts() {
  marketGridCharts.forEach(card => {
    card.eventSource?.close();
    card.chart.remove();
  });
  marketGridCharts.clear();
}

function marketGridCardMarkup(card, index) {
  const timeframeOptions = ["1Min", "5Min", "15Min"].map(timeframe =>
    `<option value="${timeframe}" ${card.timeframe === timeframe ? "selected" : ""}>${timeframe.replace("Min", "m")}</option>`
  ).join("");
  return `
    <article class="market-grid-card" data-market-grid-card="${index}">
      <div class="market-grid-card-head">
        <div class="market-grid-controls">
          <input class="market-grid-symbol" data-market-grid-symbol="${index}" value="${escapeHtml(card.symbol)}" maxlength="10" aria-label="Grid chart ${index + 1} symbol" />
          <select class="market-grid-timeframe" data-market-grid-timeframe="${index}" aria-label="Grid chart ${index + 1} timeframe">${timeframeOptions}</select>
          <button class="market-grid-load" data-market-grid-load="${index}" type="button">Load</button>
        </div>
        <button class="market-grid-focus" data-market-grid-focus="${index}" type="button">Focus</button>
      </div>
      <div class="market-grid-meta"><span data-market-grid-status="${index}">Loading validated candles...</span><span data-market-grid-price="${index}">--</span></div>
      <div class="market-grid-chart" data-market-grid-chart="${index}"></div>
    </article>
  `;
}

function createMarketGridChart(index) {
  const host = marketGridCardsEl?.querySelector(`[data-market-grid-chart="${index}"]`);
  if (!host || marketGridCharts.has(index)) return;
  const miniChart = LightweightCharts.createChart(host, {
    width: host.clientWidth,
    height: host.clientHeight,
    layout: { background: { color: "#0b1119" }, textColor: "#8392a4", attributionLogo: false },
    grid: { vertLines: { color: "#131c28" }, horzLines: { color: "#131c28" } },
    rightPriceScale: { borderColor: "#28384b", scaleMargins: { top: 0.1, bottom: 0.1 } },
    timeScale: {
      borderColor: "#28384b",
      timeVisible: true,
      secondsVisible: false,
      rightOffset: 4,
      barSpacing: 6,
      minBarSpacing: 2,
      tickMarkFormatter: (time, tickMarkType) => formatChartTimeET(time, tickMarkType <= 2),
    },
    localization: { timeFormatter: time => formatChartTimeET(time) },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
  });
  const candles = miniChart.addCandlestickSeries({
    upColor: "#36a99a", downColor: "#d85c5c", borderVisible: true,
    borderUpColor: "#2b8d83", borderDownColor: "#b84f52",
    wickUpColor: "#64b9ae", wickDownColor: "#df7470",
  });
  const vwap = miniChart.addLineSeries({ color: "#d2aa53", lineWidth: 1, priceLineVisible: false, title: "VWAP", visible: false });
  const ema9 = miniChart.addLineSeries({ color: "#5f91c1", lineWidth: 1, priceLineVisible: false, title: "EMA9", visible: false });
  const ema20 = miniChart.addLineSeries({ color: "#8a719f", lineWidth: 1, priceLineVisible: false, title: "EMA20", visible: false });
  const card = { chart: miniChart, candles, vwap, ema9, ema20, host, priceLines: [], eventSource: null, streamKey: null, quality: null, data: null };
  marketGridCharts.set(index, card);
  applyMarketGridIndicatorVisibility(card);
}

function resizeMarketGridCharts() {
  marketGridCharts.forEach(card => {
    if (card.host.clientWidth && card.host.clientHeight) {
      card.chart.applyOptions({ width: card.host.clientWidth, height: card.host.clientHeight });
    }
  });
}

function updateMarketGridCard(index, data, errorMessage = "") {
  const status = marketGridCardsEl?.querySelector(`[data-market-grid-status="${index}"]`);
  const price = marketGridCardsEl?.querySelector(`[data-market-grid-price="${index}"]`);
  const card = marketGridCharts.get(index);
  if (!status || !price || !card) return;
  if (errorMessage) {
    status.textContent = errorMessage;
    status.classList.add("market-grid-error");
    price.textContent = "--";
    card.candles.setData([]);
    card.vwap.setData([]);
    card.ema9.setData([]);
    return;
  }
  const candles = data?.candles || [];
  card.data = data;
  card.candles.setData(candles);
  card.vwap.setData(data?.indicators?.vwap || []);
  card.ema9.setData(data?.indicators?.ema9 || []);
  card.ema20.setData(data?.indicators?.ema20 || []);
  renderMarketGridStructure(index, data);
  card.chart.timeScale().fitContent();
  const current = Number(data?.current_price ?? data?.latest_trade?.price ?? candles.at(-1)?.close);
  price.textContent = Number.isFinite(current) ? current.toFixed(2) : "--";
  status.textContent = `${marketGridModeLabel()} · ${data?.data_quality_status || "DEGRADED"} · ${candles.length} candles · ${marketGridLayout[index].timeframe.replace("Min", "m")}`;
  status.classList.toggle("market-grid-error", data?.data_quality_status === "DEGRADED");
  card.quality = data?.data_quality_status || "DEGRADED";
}

function clearMarketGridPriceLines(card) {
  for (const line of card?.priceLines || []) card.candles.removePriceLine(line);
  if (card) card.priceLines = [];
}

function addMarketGridPriceLine(card, label, price, color, style = LightweightCharts.LineStyle.Dashed, showLabel = true) {
  const numeric = Number(price);
  if (!card || !Number.isFinite(numeric)) return;
  card.priceLines.push(card.candles.createPriceLine({
    price: numeric,
    color,
    lineWidth: 1,
    lineStyle: style,
    axisLabelVisible: showLabel,
    title: showLabel ? label : "",
  }));
}

function addMarketGridRange(card, label, item, color, style = LightweightCharts.LineStyle.Dashed) {
  const low = Number(item?.low);
  const high = Number(item?.high);
  if (Number.isFinite(low) && Number.isFinite(high)) {
    addMarketGridPriceLine(card, "", high, color, LightweightCharts.LineStyle.Dotted, false);
    addMarketGridPriceLine(card, "", low, color, LightweightCharts.LineStyle.Dotted, false);
    addMarketGridPriceLine(card, label, (low + high) / 2, color, style);
    return;
  }
  addMarketGridPriceLine(card, label, item?.price, color, style);
}

function selectGridNearestLevels(levels, currentPrice, side) {
  const current = Number(currentPrice);
  const gradeRank = grade => ({ A: 0, B: 1, C: 2, WEAK: 3 }[grade] ?? 2);
  return (levels || [])
    .filter(level => Number.isFinite(Number(level?.price)))
    .filter(level => !Number.isFinite(current) || (side === "support" ? Number(level.price) <= current : Number(level.price) >= current))
    .sort((left, right) =>
      Math.abs(Number(left.price) - current) - Math.abs(Number(right.price) - current) ||
      gradeRank(left.quality_grade) - gradeRank(right.quality_grade) ||
      Number(right.quality_score || 0) - Number(left.quality_score || 0)
    )
    .slice(0, 2);
}

function selectGridNearestZone(zones, currentPrice, side) {
  const current = Number(currentPrice);
  const gradeRank = grade => ({ A: 0, B: 1, C: 2, WEAK: 3 }[grade] ?? 2);
  return (zones || [])
    .filter(zone => Number.isFinite(Number(zone?.low)) && Number.isFinite(Number(zone?.high)))
    .filter(zone => zone.reaction_status !== "FAILED" || (Number.isFinite(current) && Math.abs((side === "demand" ? Number(zone.high) : Number(zone.low)) - current) / current < 0.003))
    .sort((left, right) => {
      const leftAnchor = side === "demand" ? Number(left.high) : Number(left.low);
      const rightAnchor = side === "demand" ? Number(right.high) : Number(right.low);
      return Math.abs(leftAnchor - current) - Math.abs(rightAnchor - current) ||
        gradeRank(left.zone_quality_grade) - gradeRank(right.zone_quality_grade) ||
        Number(right.zone_quality_score || 0) - Number(left.zone_quality_score || 0);
    })[0] || null;
}

function renderMarketGridStructure(index, data) {
  const card = marketGridCharts.get(index);
  if (!card) return;
  clearMarketGridPriceLines(card);
  if (data?.display_only_index) return;

  const levels = data?.levels || {};
  const current = Number(data?.current_price ?? data?.latest_trade?.price ?? data?.candles?.at(-1)?.close);
  const lineColors = {
    PMH: COLORS.pmhPml, PML: COLORS.pmhPml,
    PDH: COLORS.previousHighLow, PDL: COLORS.previousHighLow,
    HOD: COLORS.hodLod, LOD: COLORS.hodLod,
    "OPEN 5M HIGH": COLORS.openingRange, "OPEN 5M LOW": COLORS.openingRange,
  };
  const levelMap = {
    PMH: levels.pmh, PML: levels.pml, PDH: levels.pdh, PDL: levels.pdl,
    HOD: levels.hod, LOD: levels.lod,
    "OPEN 5M HIGH": levels.opening_5m_high, "OPEN 5M LOW": levels.opening_5m_low,
  };
  const showCore = type => (
    ["PMH", "PML"].includes(type) ? gridLayerVisible("premarket") : gridLayerVisible("previousDay")
  );
  GRID_CORE_LEVEL_TYPES.forEach(type => {
    if (showCore(type)) addMarketGridPriceLine(card, type, levelMap[type], lineColors[type], LightweightCharts.LineStyle.Dashed);
  });
  if (gridLayerVisible("previousDay")) {
    addMarketGridPriceLine(card, "PDC", levels.pdc, COLORS.previousClose, LightweightCharts.LineStyle.Dotted);
  }

  const supportResistance = data?.support_resistance || {};
  const cleanLevels = side => selectGridNearestLevels(supportResistance[side], current, side);
  const fullLevels = side => (supportResistance[side] || []).filter(level =>
    marketGridLayerState.weakZones || level.quality_grade !== "WEAK"
  ).slice(0, 8);
  const selectedResistance = marketGridCleanMode ? cleanLevels("resistance") : fullLevels("resistance");
  const selectedSupport = marketGridCleanMode ? cleanLevels("support") : fullLevels("support");
  if (gridLayerVisible("sr")) selectedResistance.forEach(level => {
    addMarketGridPriceLine(card, level.quality_grade === "WEAK" ? "WEAK RESISTANCE" : "RESISTANCE", level.price, level.quality_grade === "WEAK" ? COLORS.weakResistance : COLORS.resistance);
  });
  if (gridLayerVisible("sr")) selectedSupport.forEach(level => {
    addMarketGridPriceLine(card, level.quality_grade === "WEAK" ? "WEAK SUPPORT" : "SUPPORT", level.price, level.quality_grade === "WEAK" ? COLORS.weakSupport : COLORS.support);
  });

  const supplyDemand = data?.supply_demand || {};
  [["demand", "DEMAND", COLORS.demand], ["supply", "SUPPLY", COLORS.supply]].forEach(([side, label, color]) => {
    const zones = marketGridCleanMode
      ? [selectGridNearestZone(supplyDemand[side], current, side)].filter(Boolean)
      : (supplyDemand[side] || []).filter(zone => marketGridLayerState.weakZones || (!isWeakZone(zone) && zone.reaction_status !== "FAILED")).slice(0, 4);
    if (!gridLayerVisible("supplyDemand")) return;
    zones.forEach(zone => {
    const weak = isWeakZone(zone);
    const zoneLabel = `${weak ? "WEAK " : ""}${label}`;
    const zoneColor = weak ? (side === "demand" ? COLORS.weakDemand : COLORS.weakSupply) : color;
    addMarketGridPriceLine(card, zoneLabel, (Number(zone.low) + Number(zone.high)) / 2, zoneColor, LightweightCharts.LineStyle.Solid);
    addMarketGridPriceLine(card, "", zone.low, zoneColor, LightweightCharts.LineStyle.Dotted, false);
    addMarketGridPriceLine(card, "", zone.high, zoneColor, LightweightCharts.LineStyle.Dotted, false);
    });
  });

  const gaps = [
    ...((data?.fair_value_gaps || {}).bullish || []),
    ...((data?.fair_value_gaps || {}).bearish || []),
  ].filter(gap => marketGridCleanMode
    ? gap.visible_in_clean_mode && gap.worth_showing && ["ACTIVE", "PARTIALLY_FILLED"].includes(gap.status) && gap.quality_grade !== "WEAK" && (gap.quality_grade !== "C" || (gap.price_interacting_now && gap.has_key_confluence))
    : marketGridLayerState.weakZones || (gap.quality_grade !== "WEAK" && !["FILLED", "INVALID"].includes(gap.status))
  );
  if (gridLayerVisible("fvg")) gaps.slice(0, marketGridCleanMode ? 2 : 6).forEach(gap => {
    const bearish = gap.type === "BEARISH_FVG";
    const color = bearish ? COLORS.bearFvg : COLORS.bullFvg;
    addMarketGridPriceLine(card, bearish ? "BEAR FVG" : "BULL FVG", gap.midpoint, color, LightweightCharts.LineStyle.Dashed);
    addMarketGridPriceLine(card, "", gap.bottom, color, LightweightCharts.LineStyle.Dotted, false);
    addMarketGridPriceLine(card, "", gap.top, color, LightweightCharts.LineStyle.Dotted, false);
  });

  if (gridLayerVisible("liquiditySweeps")) {
    const sweeps = data?.liquidity_sweeps || {};
    (sweeps.upside || []).slice(0, 4).forEach((zone, index) => {
      addMarketGridRange(card, `UP SWEEP ${index + 1}`, zone, COLORS.liquiditySweep);
    });
    (sweeps.downside || []).slice(0, 4).forEach((zone, index) => {
      addMarketGridRange(card, `DOWN SWEEP ${index + 1}`, zone, COLORS.liquiditySweepAlt);
    });
  }

  if (gridLayerVisible("clusters")) {
    (data?.level_clusters?.clusters || []).slice(0, 5).forEach((cluster, index) => {
      const color = cluster.kind === "upside" ? COLORS.upsideCluster : COLORS.downsideCluster;
      addMarketGridRange(card, `${cluster.kind === "upside" ? "UP" : "DOWN"} CLUSTER ${index + 1}`, cluster, color, LightweightCharts.LineStyle.Solid);
    });
  }

  if (gridLayerVisible("reactionZones")) {
    const reactions = data?.structure_reactions || {};
    (reactions.resistance_watch || []).slice(0, 3).forEach((zone, index) => {
      addMarketGridRange(card, `WATCH R${index + 1}`, zone, COLORS.resistanceWatch);
    });
    (reactions.support_watch || []).slice(0, 3).forEach((zone, index) => {
      addMarketGridRange(card, `WATCH S${index + 1}`, zone, COLORS.supportWatch);
    });
  }

  if (gridLayerVisible("confirmationSetups")) {
    (data?.confirmation_setups?.setups || []).slice(0, 4).forEach(setup => {
      const stage = setup.confirmation_stage || setup.status || "WATCH";
      const direction = String(setup.direction || "").toUpperCase();
      const color = stage === "CONFIRMED" ? COLORS.confirmationConfirmed
        : (stage === "FAILED" || setup.status === "INVALIDATED") ? COLORS.confirmationInvalid
        : COLORS.confirmationWatch;
      addMarketGridPriceLine(card, `${stage === "EARLY_CONFIRM" ? "EARLY" : stage} ${direction}`.trim(), setup.trigger ?? setup.level_price, color,
        stage === "CONFIRMED" ? LightweightCharts.LineStyle.Solid : LightweightCharts.LineStyle.Dashed);
    });
  }
}

function closeMarketGridCardStream(card) {
  card?.eventSource?.close();
  if (card) {
    card.eventSource = null;
    card.streamKey = null;
  }
}

function updateMarketGridLiveStatus(index, message, error = false) {
  const status = marketGridCardsEl?.querySelector(`[data-market-grid-status="${index}"]`);
  if (!status) return;
  status.textContent = message;
  status.classList.toggle("market-grid-error", error);
}

function connectMarketGridCardStream(index, requested) {
  const card = marketGridCharts.get(index);
  if (!card) return;
  const streamKey = `${requested.symbol}:${requested.timeframe}`;
  if (card.eventSource && card.streamKey === streamKey) return;
  closeMarketGridCardStream(card);

  const source = new EventSource(`/api/stream?symbol=${encodeURIComponent(requested.symbol)}&timeframe=${encodeURIComponent(requested.timeframe)}`);
  card.eventSource = source;
  card.streamKey = streamKey;

  source.onopen = () => {
    if (marketGridCharts.get(index)?.eventSource !== source) return;
    updateMarketGridLiveStatus(index, `${marketGridModeLabel()} · ${card.quality || "VALIDATED"} · LIVE ${requested.timeframe.replace("Min", "m")}`);
  };
  source.onmessage = event => {
    let data;
    try {
      data = JSON.parse(event.data);
    } catch (_) {
      return;
    }
    const current = marketGridLayout[index];
    if (!current || current.symbol !== requested.symbol || current.timeframe !== requested.timeframe || marketGridCharts.get(index)?.eventSource !== source) return;
    const price = marketGridCardsEl?.querySelector(`[data-market-grid-price="${index}"]`);
    if (data.type === "live_candle" && data.candle) {
      card.candles.update(data.candle);
      const latest = Number(data.latest_trade?.price ?? data.candle.close);
      if (price && Number.isFinite(latest)) price.textContent = latest.toFixed(2);
      updateMarketGridLiveStatus(index, `${marketGridModeLabel()} · ${card.quality || "VALIDATED"} · LIVE ${requested.timeframe.replace("Min", "m")}`);
    } else if (data.type === "heartbeat") {
      const latest = Number(data.latest_trade?.price);
      if (price && Number.isFinite(latest)) price.textContent = latest.toFixed(2);
    } else if (data.type === "data_quality_warning") {
      card.quality = data.data_quality_status || "WARNING";
      updateMarketGridLiveStatus(index, `${marketGridModeLabel()} · ${card.quality} · live candle withheld`, card.quality === "DEGRADED");
    }
  };
  source.onerror = () => {
    if (marketGridCharts.get(index)?.eventSource === source) {
      updateMarketGridLiveStatus(index, `${requested.symbol} stream reconnecting...`);
    }
  };
}

async function loadMarketGridCard(index) {
  const requested = { ...marketGridLayout[index] };
  const card = marketGridCharts.get(index);
  if (card?.streamKey && card.streamKey !== `${requested.symbol}:${requested.timeframe}`) closeMarketGridCardStream(card);
  const status = marketGridCardsEl?.querySelector(`[data-market-grid-status="${index}"]`);
  if (status) {
    status.textContent = `Loading ${requested.symbol}...`;
    status.classList.remove("market-grid-error");
  }
  try {
    const response = await fetch(`/api/chart?symbol=${encodeURIComponent(requested.symbol)}&timeframe=${encodeURIComponent(requested.timeframe)}`);
    const data = await response.json();
    const current = marketGridLayout[index];
    if (!current || current.symbol !== requested.symbol || current.timeframe !== requested.timeframe) return;
    if (!response.ok || data.data_status !== "ok") {
      throw new Error((data.errors || ["No chart data available."]).join(", "));
    }
    updateMarketGridCard(index, data);
    if (data.stream_supported === false) {
      closeMarketGridCardStream(marketGridCharts.get(index));
      updateMarketGridLiveStatus(index, `${data.data_source || "External index"} · refreshes every 30s`);
      return;
    }
    connectMarketGridCardStream(index, requested);
  } catch (error) {
    updateMarketGridCard(index, null, error.message || "Chart unavailable.");
  }
}

function refreshMarketGrid() {
  if (!marketGridOpen()) return;
  marketGridLayout.forEach((_, index) => loadMarketGridCard(index));
}

function focusMarketGridCard(index) {
  const card = marketGridLayout[index];
  if (!card) return;
  activeTimeframe = card.timeframe;
  tfButtons.forEach(button => button.classList.toggle("active", button.dataset.tf === activeTimeframe));
  symbolInput.value = card.symbol;
  loadSelectedSymbol();
}

function renderMarketGrid() {
  if (!marketGridOpen() || !marketGridCardsEl) return;
  disposeMarketGridCharts();
  marketGridCardsEl.innerHTML = marketGridLayout.map(marketGridCardMarkup).join("");
  marketGridLayout.forEach((_, index) => {
    createMarketGridChart(index);
    const symbolInputEl = marketGridCardsEl.querySelector(`[data-market-grid-symbol="${index}"]`);
    const timeframeInput = marketGridCardsEl.querySelector(`[data-market-grid-timeframe="${index}"]`);
    const loadButton = marketGridCardsEl.querySelector(`[data-market-grid-load="${index}"]`);
    const focusButton = marketGridCardsEl.querySelector(`[data-market-grid-focus="${index}"]`);
    const applySymbol = () => {
      const symbol = normalizeSymbolInput(symbolInputEl.value);
      if (!symbol) {
        updateMarketGridCard(index, null, "Enter a valid symbol.");
        symbolInputEl.value = marketGridLayout[index].symbol;
        return;
      }
      closeMarketGridCardStream(marketGridCharts.get(index));
      marketGridLayout[index].symbol = symbol;
      symbolInputEl.value = symbol;
      saveMarketGridLayout();
      loadMarketGridCard(index);
    };
    symbolInputEl.addEventListener("change", applySymbol);
    symbolInputEl.addEventListener("keydown", event => {
      if (event.key === "Enter") applySymbol();
    });
    loadButton.addEventListener("click", applySymbol);
    timeframeInput.addEventListener("change", () => {
      closeMarketGridCardStream(marketGridCharts.get(index));
      marketGridLayout[index].timeframe = timeframeInput.value;
      saveMarketGridLayout();
      loadMarketGridCard(index);
    });
    focusButton.addEventListener("click", () => focusMarketGridCard(index));
  });
  requestAnimationFrame(() => {
    resizeMarketGridCharts();
    refreshMarketGrid();
  });
}

function setMarketGridVisible(visible) {
  if (!marketGridPanel || !marketGridToggle) return;
  marketGridPanel.hidden = !visible;
  marketGridToggle.classList.toggle("active", visible);
  marketGridToggle.setAttribute("aria-pressed", String(visible));
  if (visible) renderMarketGrid();
  else disposeMarketGridCharts();
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

window.ChartRuntime = {
  chart,
  candleSeries,
  chartEl,
  get activeSymbol() {
    return activeSymbol;
  },
  get activeTimeframe() {
    return activeTimeframe;
  },
  get latestPayload() {
    return latestPayload;
  },
  requestDrawingRedraw() {
    window.dispatchEvent(new CustomEvent("chart-runtime-redraw"));
  },
};

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

function isCoreCleanKeyLevel(line) {
  return CORE_CLEAN_KEY_LEVEL_TYPES.has(line?.type);
}

function currentChartPrice(data = latestPayload) {
  const latestCandle = data?.candles?.slice(-1)?.[0];
  return data?.current_price || data?.latest_trade?.price || latestCandle?.close || null;
}

function loadPaperTrades() {
  try {
    const parsed = JSON.parse(localStorage.getItem(PAPER_TRADE_STORAGE_KEY) || "[]");
    paperTrades = Array.isArray(parsed) ? parsed.filter(trade => trade?.read_only === true) : [];
  } catch (_) {
    paperTrades = [];
  }
}

function savePaperTrades() {
  try {
    localStorage.setItem(PAPER_TRADE_STORAGE_KEY, JSON.stringify(paperTrades.slice(-80)));
  } catch (_) {
    setPaperTradeMessage("Unable to save paper trades in this browser session.", "warning");
  }
}

function paperTradeScopeMatches(trade, symbol = activeSymbol, timeframe = activeTimeframe) {
  return trade?.symbol === symbol && trade?.timeframe === timeframe;
}

function activePaperTrade(symbol = activeSymbol, timeframe = activeTimeframe) {
  return paperTrades
    .filter(trade => paperTradeScopeMatches(trade, symbol, timeframe) && trade.status === "ACTIVE")
    .sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)))[0] || null;
}

function isOptionPaperTrade(type) {
  return type === "CALL_OPTION" || type === "PUT_OPTION";
}

function paperTradeDirection(type) {
  return type === "SHORT_STOCK" || type === "PUT_OPTION" ? "short" : "long";
}

function paperTradeTypeLabel(type) {
  return ({
    LONG_STOCK: "Long Stock",
    SHORT_STOCK: "Short Stock",
    CALL_OPTION: "Call Option",
    PUT_OPTION: "Put Option",
  })[type] || "Paper Trade";
}

function paperStatusLabel(status) {
  return String(status || "ACTIVE").replace(/_/g, " ");
}

function paperNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function paperMoney(value) {
  return Number.isFinite(Number(value)) ? `$${Number(value).toFixed(2)}` : "n/a";
}

function calculatePaperTrade(trade) {
  const entry = paperNumber(trade?.entry);
  const stop = paperNumber(trade?.stop);
  const target = paperNumber(trade?.target);
  const quantity = Math.max(1, Math.floor(paperNumber(trade?.quantity) || 1));
  const multiplier = isOptionPaperTrade(trade?.type) ? 100 : 1;
  const riskPerUnit = Math.abs(entry - stop) * multiplier;
  const rewardPerUnit = Math.abs(target - entry) * multiplier;
  const totalRisk = riskPerUnit * quantity;
  const totalReward = rewardPerUnit * quantity;
  const rr = riskPerUnit > 0 ? rewardPerUnit / riskPerUnit : null;
  return { entry, stop, target, quantity, multiplier, riskPerUnit, rewardPerUnit, totalRisk, totalReward, rr };
}

function setPaperTradeMessage(message = "", tone = "") {
  if (!paperTradeMessage) return;
  paperTradeMessage.textContent = message;
  paperTradeMessage.className = `paper-message ${tone}`.trim();
}

function setPaperTradeFormDefaults() {
  if (!paperSymbolInput || !paperTimeframeInput) return;
  paperSymbolInput.value = activeSymbol;
  paperTimeframeInput.value = activeTimeframe;
  paperTradeMeta.textContent = `${activeSymbol} · ${activeTimeframe} · browser-local only`;
  if (!paperQuantityInput.value) paperQuantityInput.value = "1";
  updatePaperTradeInputLanguage();
}

function updatePaperTradeInputLanguage() {
  const optionMode = isOptionPaperTrade(paperTradeTypeInput?.value);
  if (paperEntryLabel) paperEntryLabel.textContent = optionMode ? "Entry Premium" : "Entry Price";
  if (paperStopLabel) paperStopLabel.textContent = optionMode ? "Stop Premium" : "Stop Loss";
  if (paperTargetLabel) paperTargetLabel.textContent = optionMode ? "Target Premium" : "Take Profit";
  if (paperQuantityLabel) paperQuantityLabel.textContent = optionMode ? "Contracts (x100)" : "Shares";
  if (paperOptionNotice) {
    paperOptionNotice.hidden = !optionMode;
    paperOptionNotice.textContent = optionMode
      ? "Option planner is premium-based. Stock-chart levels are not exact option TP/SL levels; manage on the option premium chart or estimate with delta. Option premium tracking is manual."
      : "";
  }
}

function resetPaperTradeForm(keepSymbol = true) {
  if (!paperTradeForm) return;
  paperTradeForm.reset();
  paperTradeTypeInput.value = "LONG_STOCK";
  paperQuantityInput.value = "1";
  if (keepSymbol) setPaperTradeFormDefaults();
  setPaperTradeMessage("");
}

function readPaperTradeForm() {
  const symbol = normalizeSymbolInput(paperSymbolInput.value) || activeSymbol;
  const trade = {
    id: `paper-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    symbol,
    timeframe: activeTimeframe,
    type: paperTradeTypeInput.value,
    entry: paperNumber(paperEntryInput.value),
    stop: paperNumber(paperStopInput.value),
    target: paperNumber(paperTargetInput.value),
    quantity: Math.max(1, Math.floor(paperNumber(paperQuantityInput.value) || 0)),
    notes: String(paperNotesInput.value || "").trim(),
    status: "ACTIVE",
    created_at: new Date().toISOString(),
    closed_at: null,
    close_reason: null,
    read_only: true,
    safety: "Paper trade only — no real order.",
  };
  const calc = calculatePaperTrade(trade);
  if (![trade.entry, trade.stop, trade.target].every(Number.isFinite)) {
    throw new Error("Entry, stop loss, and take profit must be valid numbers.");
  }
  if (trade.entry <= 0 || trade.stop <= 0 || trade.target <= 0) {
    throw new Error("Entry, stop loss, and take profit must be greater than zero.");
  }
  if (!Number.isFinite(trade.quantity) || trade.quantity < 1) {
    throw new Error("Quantity/contracts must be at least 1.");
  }
  if (!Number.isFinite(calc.rr) || calc.rr <= 0) {
    throw new Error("Risk/reward needs a non-zero risk distance.");
  }
  return { ...trade, calculations: calc };
}

function updatePaperTradeStatus(data = latestPayload) {
  const trade = activePaperTrade();
  if (!trade || isOptionPaperTrade(trade.type)) return false;
  const price = paperNumber(currentChartPrice(data));
  if (!Number.isFinite(price)) return false;
  const direction = paperTradeDirection(trade.type);
  let newStatus = null;
  if (direction === "long") {
    if (price >= Number(trade.target)) newStatus = "TP_HIT";
    if (price <= Number(trade.stop)) newStatus = "SL_HIT";
  } else {
    if (price <= Number(trade.target)) newStatus = "TP_HIT";
    if (price >= Number(trade.stop)) newStatus = "SL_HIT";
  }
  if (!newStatus) return false;
  trade.status = newStatus;
  trade.closed_at = new Date().toISOString();
  trade.close_reason = `Auto-marked from chart price ${price.toFixed(2)}. Paper trade only — no real order.`;
  savePaperTrades();
  return true;
}

function clearPaperTradeLines() {
  for (const line of paperTradePriceLines) {
    candleSeries.removePriceLine(line);
  }
  paperTradePriceLines = [];
  if (paperTradeOverlay) paperTradeOverlay.innerHTML = "";
}

function renderPaperTradeOverlay() {
  if (!paperTradeOverlay) return;
  paperTradeOverlay.innerHTML = "";
  const trade = activePaperTrade();
  if (!trade) return;
  if (isOptionPaperTrade(trade.type)) return;
  const { entry, stop, target } = calculatePaperTrade(trade);
  const coordinates = [entry, stop, target].map(price => candleSeries.priceToCoordinate(price));
  if (!coordinates.every(Number.isFinite)) return;
  const [entryY, stopY, targetY] = coordinates;
  const riskTop = Math.min(entryY, stopY);
  const riskHeight = Math.max(3, Math.abs(entryY - stopY));
  const rewardTop = Math.min(entryY, targetY);
  const rewardHeight = Math.max(3, Math.abs(entryY - targetY));
  paperTradeOverlay.innerHTML = `
    <div class="paper-zone risk" style="top:${riskTop}px;height:${riskHeight}px"><span class="paper-zone-label">RISK · Paper Trade Only</span></div>
    <div class="paper-zone reward" style="top:${rewardTop}px;height:${rewardHeight}px"><span class="paper-zone-label">REWARD · Paper Trade Only</span></div>
  `;
}

function renderPaperTradeLines() {
  clearPaperTradeLines();
  const trade = activePaperTrade();
  if (!trade) return;
  if (isOptionPaperTrade(trade.type)) return;
  const { entry, stop, target } = calculatePaperTrade(trade);
  [
    { price: entry, color: COLORS.paperEntry, title: "ENTRY · Paper Trade Only" },
    { price: stop, color: COLORS.paperStop, title: "SL · Paper Trade Only" },
    { price: target, color: COLORS.paperTarget, title: "TP · Paper Trade Only" },
  ].forEach(item => {
    paperTradePriceLines.push(candleSeries.createPriceLine({
      price: item.price,
      color: item.color,
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      axisLabelVisible: true,
      title: item.title,
    }));
  });
  renderPaperTradeOverlay();
}

function paperTradeSummaryHtml(trade) {
  if (!trade) return "No active paper trade for this symbol/timeframe.";
  const calc = calculatePaperTrade(trade);
  const optionNote = isOptionPaperTrade(trade.type)
    ? `<br><span class="compare-warning">Premium-based plan only. Stock-chart levels are not exact option TP/SL levels; manage the option premium chart or estimate with delta. Option premium tracking is manual.</span>`
    : "";
  return `
    <strong>${escapeHtml(paperTradeTypeLabel(trade.type))} · ${escapeHtml(paperStatusLabel(trade.status))}</strong><br>
    Entry ${paperMoney(calc.entry)} · SL ${paperMoney(calc.stop)} · TP ${paperMoney(calc.target)} · Qty ${calc.quantity}<br>
    Risk ${paperMoney(calc.totalRisk)} · Reward ${paperMoney(calc.totalReward)} · R:R ${Number.isFinite(calc.rr) ? calc.rr.toFixed(2) : "n/a"}${optionNote}<br>
    Paper trade only — no real order.
  `;
}

function renderPaperTradePanel() {
  setPaperTradeFormDefaults();
  const active = activePaperTrade();
  if (paperTradeSummary) paperTradeSummary.innerHTML = paperTradeSummaryHtml(active);
  if (!paperTradeList) return;
  const scoped = paperTrades
    .filter(trade => paperTradeScopeMatches(trade))
    .sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)))
    .slice(0, 8);
  paperTradeList.innerHTML = scoped.map(trade => {
    const calc = calculatePaperTrade(trade);
    const statusClass = trade.status === "ACTIVE" ? "active" : "closed";
    return `
      <div class="paper-trade-item ${statusClass}">
        <div class="paper-row"><span class="paper-label">${escapeHtml(paperTradeTypeLabel(trade.type))}</span><span>${escapeHtml(paperStatusLabel(trade.status))}</span></div>
        <div class="paper-sub">${escapeHtml(trade.symbol)} · ${escapeHtml(trade.timeframe)} · Entry ${paperMoney(calc.entry)} · SL ${paperMoney(calc.stop)} · TP ${paperMoney(calc.target)}</div>
        <div class="paper-sub">Risk ${paperMoney(calc.totalRisk)} · Reward ${paperMoney(calc.totalReward)} · R:R ${Number.isFinite(calc.rr) ? calc.rr.toFixed(2) : "n/a"} · ${escapeHtml(new Date(trade.created_at).toLocaleString("en-US", { timeZone: "America/New_York" }))} ET</div>
        ${isOptionPaperTrade(trade.type) ? `<div class="paper-sub compare-warning">Premium-based option plan; stock-chart levels are not exact option TP/SL levels. Manual premium tracking only.</div>` : ""}
        ${trade.notes ? `<div class="paper-sub">Notes: ${escapeHtml(trade.notes)}</div>` : ""}
        ${trade.close_reason ? `<div class="paper-sub">${escapeHtml(trade.close_reason)}</div>` : ""}
      </div>
    `;
  }).join("") || `<div class="paper-meta">No paper trades saved for ${escapeHtml(activeSymbol)} ${escapeHtml(activeTimeframe)}.</div>`;
}

function renderPaperTradePlanner() {
  updatePaperTradeStatus();
  renderPaperTradeLines();
  renderPaperTradePanel();
}

function levelDistanceFromPrice(level, currentPrice) {
  const price = Number(level?.price);
  const current = Number(currentPrice);
  return Number.isFinite(price) && Number.isFinite(current) ? Math.abs(price - current) : Number.POSITIVE_INFINITY;
}

function zoneAnchorForSide(zone, side, currentPrice) {
  const low = Number(zone?.low);
  const high = Number(zone?.high);
  const current = Number(currentPrice);
  if (!Number.isFinite(low) || !Number.isFinite(high)) return null;
  if (Number.isFinite(current) && current >= low && current <= high) return current;
  return side === "demand" ? high : low;
}

function zoneDistanceFromPrice(zone, side, currentPrice) {
  const anchor = zoneAnchorForSide(zone, side, currentPrice);
  const current = Number(currentPrice);
  return Number.isFinite(anchor) && Number.isFinite(current) ? Math.abs(anchor - current) : Number.POSITIVE_INFINITY;
}

function selectCleanModeSupportResistance(sr, currentPrice) {
  const current = Number(currentPrice);
  const selectSide = (levels, side) => (levels || [])
    .filter(level => Number.isFinite(Number(level?.price)))
    .filter(level => !Number.isFinite(current) || (side === "support" ? Number(level.price) <= current : Number(level.price) >= current))
    .sort((a, b) => {
      const gradeRank = grade => ({ A: 0, B: 1, C: 2, WEAK: 3 }[grade] ?? 2);
      return levelDistanceFromPrice(a, current) - levelDistanceFromPrice(b, current) ||
        gradeRank(a.quality_grade) - gradeRank(b.quality_grade) ||
        (b.quality_score || 0) - (a.quality_score || 0);
    })
    .slice(0, 2);

  return {
    support: selectSide(sr?.support, "support"),
    resistance: selectSide(sr?.resistance, "resistance"),
  };
}

function selectCleanModeSupplyDemand(sd, currentPrice) {
  const current = Number(currentPrice);
  const selectSide = (zones, side) => (zones || [])
    .filter(zone => Number.isFinite(Number(zone?.low)) && Number.isFinite(Number(zone?.high)))
    .filter(zone => {
      if (zone.reaction_status !== "FAILED") return true;
      const retestEdge = side === "demand" ? zone.high : zone.low;
      return isLineNearCurrentPrice({ price: retestEdge }, current, activeSymbol);
    })
    .filter(zone => !Number.isFinite(current) || (side === "demand" ? Number(zone.high) <= current || isLineNearCurrentPrice({ top: zone.high, bottom: zone.low }, current, activeSymbol) : Number(zone.low) >= current || isLineNearCurrentPrice({ top: zone.high, bottom: zone.low }, current, activeSymbol)))
    .sort((a, b) => {
      const gradeRank = grade => ({ A: 0, B: 1, C: 2, WEAK: 3 }[grade] ?? 2);
      return zoneDistanceFromPrice(a, side, current) - zoneDistanceFromPrice(b, side, current) ||
        gradeRank(a.zone_quality_grade) - gradeRank(b.zone_quality_grade) ||
        (b.zone_quality_score || 0) - (a.zone_quality_score || 0);
    })
    .slice(0, 1);

  return {
    demand: selectSide(sd?.demand, "demand"),
    supply: selectSide(sd?.supply, "supply"),
  };
}

function selectedCleanStructureIds(data = latestPayload) {
  const currentPrice = currentChartPrice(data);
  const selected = new Set();
  const sr = selectCleanModeSupportResistance(data?.support_resistance || {}, currentPrice);
  const sd = selectCleanModeSupplyDemand(data?.supply_demand || {}, currentPrice);
  (data?.chart_lines || []).forEach(line => {
    const anchor = lineAnchorPrice(line);
    if (!Number.isFinite(anchor)) return;
    if (line.type === "SUPPORT" && sr.support.some(level => Math.abs(Number(level.price) - anchor) < 0.011)) selected.add(line.id);
    if (line.type === "RESISTANCE" && sr.resistance.some(level => Math.abs(Number(level.price) - anchor) < 0.011)) selected.add(line.id);
    if (line.type === "DEMAND_ZONE" && sd.demand.some(zone => Math.abs(((Number(zone.low) + Number(zone.high)) / 2) - anchor) < 0.011)) selected.add(line.id);
    if (line.type === "SUPPLY_ZONE" && sd.supply.some(zone => Math.abs(((Number(zone.low) + Number(zone.high)) / 2) - anchor) < 0.011)) selected.add(line.id);
  });
  return selected;
}

function selectedCleanStructureDetails(line, data = latestPayload) {
  const current = Number(currentChartPrice(data));
  const anchor = lineAnchorPrice(line);
  const selected = selectedCleanStructureIds(data).has(line?.id);
  let side = line?.extra_details?.side || "unknown";
  if (Number.isFinite(current) && Number.isFinite(anchor)) {
    if (line?.top !== null && line?.top !== undefined && line?.bottom !== null && line?.bottom !== undefined &&
        Number(line.bottom) <= current && current <= Number(line.top)) {
      side = "overlapping";
    } else if (anchor > current) {
      side = "above";
    } else if (anchor < current) {
      side = "below";
    } else {
      side = "overlapping";
    }
  }
  return {
    selected_as_nearest_clean_mode: selected,
    distance_from_current_price: Number.isFinite(current) && Number.isFinite(anchor) ? Math.abs(anchor - current) : null,
    side,
  };
}

function isCleanModeSelectedStructure(line) {
  return selectedCleanStructureIds().has(line?.id);
}

function cleanModeStructureHiddenReason(line) {
  if (!["SUPPORT", "RESISTANCE", "DEMAND_ZONE", "SUPPLY_ZONE"].includes(line?.type)) return null;
  if (line.status === "FAILED") return "failed zone not retesting";
  if (line.strength === "WEAK") return "weak and not nearest";
  if (["SUPPORT", "RESISTANCE"].includes(line.type)) return "extra Clean Mode support/resistance";
  return "extra Clean Mode zone";
}

function lineDisplayDecision(line) {
  if (!isValidAuditLine(line)) return { visible: false, hiddenReason: "invalid audit metadata" };
  if (!line.visible_in_full_mode) return { visible: false, hiddenReason: "not visible in full mode" };
  if (!cleanMode) return { visible: true, hiddenReason: null };

  const alwaysVisible = new Set(["VWAP", "EMA9", "EMA20"]);
  if (alwaysVisible.has(line.type)) return { visible: !cleanMode, hiddenReason: cleanMode ? "hidden in trader Clean Mode" : null };
  const cleanTypes = new Set([
    "PMH", "PML", "PDH", "PDL", "HOD", "LOD", "OPEN 5M HIGH", "OPEN 5M LOW",
    "BULLISH_FVG", "BEARISH_FVG", "DEMAND_ZONE", "SUPPLY_ZONE", "SUPPORT", "RESISTANCE",
  ]);
  if (!cleanTypes.has(line.type)) return { visible: false, hiddenReason: "hidden in trader Clean Mode" };
  if (!line.source || !line.reason) return { visible: false, hiddenReason: "no valid source" };
  if (isCoreCleanKeyLevel(line)) {
    return line.visible_in_clean_mode
      ? { visible: true, hiddenReason: null }
      : { visible: false, hiddenReason: "not marked visible in Clean Mode" };
  }
  if (["BULLISH_FVG", "BEARISH_FVG"].includes(line.type) && line.status === "FILLED") return { visible: false, hiddenReason: "filled FVG" };
  if (["BULLISH_FVG", "BEARISH_FVG"].includes(line.type) && line.status === "INVALID") return { visible: false, hiddenReason: "invalid FVG" };
  if (["BULLISH_FVG", "BEARISH_FVG"].includes(line.type) && !line.visible_in_clean_mode) {
    return { visible: false, hiddenReason: line.extra_details?.clean_mode_hidden_reason || line.extra_details?.hidden_reason || "not actionable in Clean Mode" };
  }
  if (["SUPPORT", "RESISTANCE", "DEMAND_ZONE", "SUPPLY_ZONE"].includes(line.type)) {
    if (isCleanModeSelectedStructure(line)) {
      const typeLabel = line.type === "DEMAND_ZONE" ? "demand" : line.type === "SUPPLY_ZONE" ? "supply" : line.type.toLowerCase();
      return { visible: true, hiddenReason: `selected nearest ${typeLabel}` };
    }
    return { visible: false, hiddenReason: cleanModeStructureHiddenReason(line) };
  }
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
      if (zone.reaction_status === "FAILED" || zone.zone_quality_grade === "WEAK") return false;
      if (!["A", "B", "C"].includes(zone.zone_quality_grade)) return false;
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
    const suppressedLayers = ["vwap", "emas", "weakZones", "liquiditySweeps", "clusters", "reactionZones", "confirmationSetups"];
    const suppressed = cleanMode && suppressedLayers.includes(btn.dataset.layer);
    btn.classList.toggle("clean-mode-suppressed", suppressed);
    btn.title = suppressed ? "Hidden while Clean Mode is on" : "";
  });
}

function applyIndicatorVisibility() {
  const auditTypeVisible = type => (latestPayload?.chart_lines || []).some(line =>
    line.type === type && lineDisplayDecision(line).visible
  );
  vwapSeries.applyOptions({ visible: !cleanMode && layerState.vwap && auditTypeVisible("VWAP"), lineWidth: 2 });
  ema9Series.applyOptions({ visible: !cleanMode && layerState.emas && auditTypeVisible("EMA9"), lineWidth: 1 });
  ema20Series.applyOptions({ visible: !cleanMode && layerState.emas && auditTypeVisible("EMA20"), lineWidth: 1 });
}

function clearPriceLines() {
  for (const line of priceLines) {
    candleSeries.removePriceLine(line);
  }
  priceLines = [];
  labeledPrices = [];
  if (fvgOverlay) fvgOverlay.innerHTML = "";
  if (keyLevelEdgeMarkers) keyLevelEdgeMarkers.innerHTML = "";
}

function closePaperTradePanel() {
  paperTradePanel?.classList.remove("visible");
  paperTradeToggle?.classList.remove("active");
  paperTradeToggle?.setAttribute("aria-pressed", "false");
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
  const requestedSymbol = activeSymbol;
  try {
    const response = await fetch(`/api/ai/latest-review?symbol=${encodeURIComponent(requestedSymbol)}`);
    if (requestedSymbol !== activeSymbol) return;
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

function findSelectedStructureAuditLine(label, price) {
  if (!cleanMode) return null;
  const numericPrice = Number(price);
  const labelText = String(label || "").toUpperCase();
  const type = labelText.includes("SUPPORT") ? "SUPPORT" :
    labelText.includes("RESISTANCE") ? "RESISTANCE" :
    labelText.includes("DEMAND") ? "DEMAND_ZONE" :
    labelText.includes("SUPPLY") ? "SUPPLY_ZONE" : null;
  if (!type || !Number.isFinite(numericPrice)) return null;
  return (latestPayload?.chart_lines || []).find(line =>
    line.type === type && isCleanModeSelectedStructure(line) &&
    Math.abs(lineAnchorPrice(line) - numericPrice) < 0.35
  ) || null;
}

function addLevel(label, price, color, style = LightweightCharts.LineStyle.Solid, showLabel = true, lineWidth = 1) {
  if (price === null || price === undefined) return;
  const numericPrice = Number(price);
  const auditLine = findAuditLineForPlot(label, numericPrice) || findSelectedStructureAuditLine(label, numericPrice);
  if (!auditLine) return;
  const decision = lineDisplayDecision(auditLine);
  if (!decision.visible) return;

  const isCoreKey = cleanMode && isCoreCleanKeyLevel(auditLine);
  const priority = auditLine.priority || 3;
  const strongerNearby = !isCoreKey && visibleAuditLines().some(line =>
    line.id !== auditLine.id && (line.priority || 3) < priority &&
    Math.abs(lineAnchorPrice(line) - numericPrice) < 0.025
  );
  const overlapsLabel = !isCoreKey && labeledPrices.some(item =>
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

function coreKeyLevelsFromPayload(data = latestPayload) {
  return (data?.chart_lines || [])
    .filter(line => isCoreCleanKeyLevel(line) && lineDisplayDecision(line).visible)
    .map(line => ({
      label: line.short_label || line.label || line.type,
      price: lineAnchorPrice(line),
    }))
    .filter(item => Number.isFinite(item.price))
    .sort((a, b) => b.price - a.price);
}

function renderCoreKeyLevelEdgeMarkers() {
  if (!keyLevelEdgeMarkers) return;
  keyLevelEdgeMarkers.innerHTML = "";
  if (!cleanMode || !latestPayload) return;

  const chartHeight = chartEl.clientHeight;
  if (!chartHeight) return;

  const top = [];
  const bottom = [];
  coreKeyLevelsFromPayload().forEach(level => {
    const coordinate = candleSeries.priceToCoordinate(level.price);
    if (!Number.isFinite(coordinate)) return;
    if (coordinate < 8) top.push(level);
    if (coordinate > chartHeight - 8) bottom.push(level);
  });

  const markerHtml = [
    ...top.slice(0, 4).map((level, index) =>
      `<div class="key-level-edge-marker top" style="top:${8 + index * 24}px">${escapeHtml(level.label)} ${level.price.toFixed(2)} ↑</div>`
    ),
    ...bottom.slice(-4).map((level, index) =>
      `<div class="key-level-edge-marker bottom" style="bottom:${8 + index * 24}px">${escapeHtml(level.label)} ${level.price.toFixed(2)} ↓</div>`
    ),
  ].join("");
  keyLevelEdgeMarkers.innerHTML = markerHtml;
}

function scheduleCoreKeyLevelEdgeMarkers() {
  requestAnimationFrame(renderCoreKeyLevelEdgeMarkers);
}


function addZoneBand(label, zone, colors, options = {}) {
  if (!zone) return;

  const low = zone.low;
  const high = zone.high;

  if (low === null || low === undefined || high === null || high === undefined) return;

  const quality = zone.label || "Zone";
  const failed = zone.reaction_status === "FAILED";
  const weak = zone.zone_quality_grade === "WEAK" || failed;
  const zoneColor = options.muted ? colors.zone : failed ? COLORS.failedZone : colors.zone;
  const style = weak || options.muted ? LightweightCharts.LineStyle.Dotted : LightweightCharts.LineStyle.Dashed;

  if (cleanMode) {
    if (options.cleanLabel) {
      addLevel(options.cleanLabel, (Number(low) + Number(high)) / 2, zoneColor, style, true, options.muted ? 1 : 2);
    } else if (zone.reaction_label && !failed) {
      const reactionColor = zone.type === "demand" ? COLORS.demandReaction : COLORS.supplyReaction;
      addLevel(zone.reaction_label, zone.defended_edge, reactionColor, LightweightCharts.LineStyle.Solid, true, 2);
    } else if (!failed && !weak) {
      const cleanLabel = zone.type === "demand" ? "DEMAND" : "SUPPLY";
      addLevel(cleanLabel, (Number(low) + Number(high)) / 2, zoneColor, LightweightCharts.LineStyle.Dashed, true, 2);
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

function shouldRenderFvg(gap) {
  if (!gap) return false;
  if (cleanMode && !gap.visible_in_clean_mode) return false;
  if (cleanMode && !(gap.worth_showing && ["ACTIVE", "PARTIALLY_FILLED"].includes(gap.status))) return false;
  if (cleanMode && ["WEAK"].includes(gap.quality_grade)) return false;
  if (cleanMode && gap.quality_grade === "C" && !(gap.price_interacting_now && gap.has_key_confluence)) return false;
  const currentPrice = latestPayload?.current_price || latestPayload?.latest_trade?.price || latestPayload?.candles?.slice(-1)?.[0]?.close;
  if (cleanMode && !isLineNearCurrentPrice({ top: gap.top, bottom: gap.bottom }, currentPrice, activeSymbol)) return false;
  return true;
}

function fvgChartTime(gap) {
  return gap?.candle3_time ?? gap?.created_at ?? null;
}

function renderFvgOverlay(data = latestPayload) {
  if (!fvgOverlay) return;
  fvgOverlay.innerHTML = "";
  if (!data || !isLayerVisible("fvg")) return;

  const chartHeight = chartEl.clientHeight;
  const chartWidth = chartEl.clientWidth;
  if (!chartHeight || !chartWidth) return;

  const gaps = [
    ...((data.fair_value_gaps || {}).bullish || []),
    ...((data.fair_value_gaps || {}).bearish || []),
  ].filter(shouldRenderFvg);

  fvgOverlay.innerHTML = gaps.map(gap => {
    const topPrice = Number(gap.top);
    const bottomPrice = Number(gap.bottom);
    const midpoint = Number(gap.midpoint ?? ((topPrice + bottomPrice) / 2));
    if (!Number.isFinite(topPrice) || !Number.isFinite(bottomPrice) || topPrice <= bottomPrice) return "";

    const topCoordinate = candleSeries.priceToCoordinate(topPrice);
    const bottomCoordinate = candleSeries.priceToCoordinate(bottomPrice);
    const midpointCoordinate = candleSeries.priceToCoordinate(midpoint);
    if (![topCoordinate, bottomCoordinate, midpointCoordinate].every(Number.isFinite)) return "";

    const upper = Math.min(topCoordinate, bottomCoordinate);
    const lower = Math.max(topCoordinate, bottomCoordinate);
    if (lower < 0 || upper > chartHeight) return "";

    const timeCoordinate = chart.timeScale().timeToCoordinate(fvgChartTime(gap));
    const left = Number.isFinite(timeCoordinate) ? Math.max(0, timeCoordinate) : 0;
    const rightPadding = 82;
    const width = Math.max(34, chartWidth - left - rightPadding);
    const height = Math.max(4, lower - upper);
    const midpointTop = Math.max(0, Math.min(height, midpointCoordinate - upper));
    const bearish = gap.type === "BEARISH_FVG";
    const muted = gap.quality_grade === "WEAK" || ["FILLED", "INVALID"].includes(gap.status);
    const label = bearish ? "BEAR FVG" : "BULL FVG";
    const classes = ["fvg-box", bearish ? "bearish" : "bullish", muted ? "muted" : ""].filter(Boolean).join(" ");

    return `
      <div class="${classes}" style="left:${left}px; top:${Math.max(0, upper)}px; width:${width}px; height:${height}px" title="${escapeHtml(label)} ${escapeHtml(gap.status || "")} ${escapeHtml(gap.quality_grade || "")}">
        <div class="fvg-midpoint" style="top:${midpointTop}px"></div>
        <div class="fvg-label">${escapeHtml(label)}</div>
      </div>
    `;
  }).join("");
}

function scheduleChartOverlays() {
  requestAnimationFrame(() => {
    renderFvgOverlay();
    renderCoreKeyLevelEdgeMarkers();
    renderPaperTradeOverlay();
    window.ChartRuntime?.requestDrawingRedraw?.();
  });
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

function fvgAuditProofHtml(line) {
  if (!["BULLISH_FVG", "BEARISH_FVG"].includes(line?.type)) return "";
  const details = line.extra_details || {};
  const bullishProof = `${details.candle1_high ?? "n/a"} < ${details.candle3_low ?? "n/a"}`;
  const bearishProof = `${details.candle1_low ?? "n/a"} > ${details.candle3_high ?? "n/a"}`;
  const hiddenReason = details.clean_mode_hidden_reason || details.hidden_reason;
  return `
    <br><strong>FVG validation proof</strong><br>
    Type / timeframe: ${escapeHtml(line.type)} / ${escapeHtml(line.timeframe || details.timeframe || "n/a")}<br>
    Direction: ${escapeHtml(details.direction || "n/a")}<br>
    Candle times: C1 ${escapeHtml(details.candle1_time ?? "n/a")} · C2 ${escapeHtml(details.candle2_time ?? "n/a")} · C3 ${escapeHtml(details.candle3_time ?? "n/a")}<br>
    Bullish rule: candle1.high &lt; candle3.low (${escapeHtml(bullishProof)})<br>
    Bearish rule: candle1.low &gt; candle3.high (${escapeHtml(bearishProof)})<br>
    Rule passed: ${details.rule_passed === true ? "true" : "false"}<br>
    C1 high/low: ${escapeHtml(details.candle1_high ?? "n/a")} / ${escapeHtml(details.candle1_low ?? "n/a")}<br>
    C2 open/high/low/close: ${escapeHtml(details.candle2_open ?? "n/a")} / ${escapeHtml(details.candle2_high ?? "n/a")} / ${escapeHtml(details.candle2_low ?? "n/a")} / ${escapeHtml(details.candle2_close ?? "n/a")}<br>
    C3 high/low: ${escapeHtml(details.candle3_high ?? "n/a")} / ${escapeHtml(details.candle3_low ?? "n/a")}<br>
    Top / bottom / midpoint / gap size: ${escapeHtml(details.top ?? line.top ?? "n/a")} / ${escapeHtml(details.bottom ?? line.bottom ?? "n/a")} / ${escapeHtml(details.midpoint ?? line.price ?? "n/a")} / ${escapeHtml(details.gap_size ?? "n/a")}<br>
    Displacement / engulfing: ${escapeHtml(details.displacement_score ?? "n/a")} / ${escapeHtml(details.engulfing_score ?? "n/a")}<br>
    C2 engulfed C1: ${details.middle_candle_engulfed_c1 === true ? "true" : "false"} · Closed beyond C1: ${details.middle_candle_closed_beyond_c1 === true ? "true" : "false"}<br>
    Quality: ${escapeHtml(details.quality_grade ?? line.strength ?? "n/a")} · ${escapeHtml(details.quality_reason ?? "strict imbalance audit")}<br>
    Price interacting now: ${details.price_interacting_now === true ? "true" : "false"} · Key confluence: ${details.has_key_confluence === true ? "true" : "false"}<br>
    Primary Clean Mode FVG: ${details.selected_as_primary_clean_mode_fvg === true ? "true" : "false"}<br>
    Visible in Clean Mode: ${details.visible_in_clean_mode === true ? "true" : "false"}${hiddenReason ? ` · Hidden reason: ${escapeHtml(hiddenReason)}` : ""}<br>
    Fill: ${escapeHtml(details.fill_percentage ?? "n/a")}% · Status: ${escapeHtml(line.status)}<br>
    FVG = strict 3-candle imbalance only. Supply/demand/base zones are separate chart context.
  `;
}

function structureAuditHtml(line, decision) {
  if (!["SUPPORT", "RESISTANCE", "DEMAND_ZONE", "SUPPLY_ZONE"].includes(line?.type)) return "";
  const details = selectedCleanStructureDetails(line);
  const distance = Number.isFinite(details.distance_from_current_price) ? details.distance_from_current_price.toFixed(2) : "n/a";
  return `
    <br><strong>Clean Mode structure selection</strong><br>
    Selected as nearest Clean Mode structure: ${details.selected_as_nearest_clean_mode ? "true" : "false"}<br>
    Distance from current price: ${escapeHtml(distance)}<br>
    Side: ${escapeHtml(details.side)}<br>
    Hidden reason: ${decision?.visible ? "none" : escapeHtml(decision?.hiddenReason || line.extra_details?.hidden_reason || "n/a")}<br>
  `;
}

function renderLineAudit(selectedId = null) {
  if (!lineAuditList) return;
  const lines = (latestPayload?.chart_lines || []).sort((a, b) =>
    (a.priority || 3) - (b.priority || 3) || String(a.short_label).localeCompare(String(b.short_label))
  );
  const visibleCount = lines.filter(line => lineDisplayDecision(line).visible).length;
  const quality = latestPayload?.data_quality_status || "DEGRADED";
  const qualityWarning = (latestPayload?.candle_data_warnings || []).join(" | ");
  lineAuditMeta.textContent = `${activeSymbol} · ${activeTimeframe} · ${visibleCount}/${lines.length} visible deterministic items · Data ${quality}${qualityWarning ? `: ${qualityWarning}` : ""}`;
  const supportResistanceCount = lines.filter(line => ["SUPPORT", "RESISTANCE"].includes(line.type)).length;
  const supplyDemandCount = lines.filter(line => ["DEMAND_ZONE", "SUPPLY_ZONE"].includes(line.type)).length;
  const emptyStructureNotes = [
    supportResistanceCount ? "" : `<div class="audit-meta">No support/resistance detected.</div>`,
    supplyDemandCount ? "" : `<div class="audit-meta">No supply/demand zones detected.</div>`,
  ].join("");
  lineAuditList.innerHTML = emptyStructureNotes + (lines.map(line => {
    const decision = lineDisplayDecision(line);
    return `
    <div class="audit-item ${!decision.visible || line.status === "FAILED" || line.status === "MUTED" ? "muted" : ""} ${line.id === selectedId ? "selected" : ""}" data-line-id="${escapeHtml(line.id)}" title="${escapeHtml(line.reason)}">
      <div class="audit-row"><span class="audit-label">${escapeHtml(line.short_label)}</span><span>${escapeHtml(linePriceText(line))}</span></div>
      <div class="audit-row audit-sub"><span>${escapeHtml(line.type)}</span><span>${decision.visible ? "Visible" : `Hidden: ${escapeHtml(decision.hiddenReason)}`} · P${escapeHtml(line.priority)}</span></div>
    </div>
  `}).join("") || `<div class="audit-meta">No registered chart lines.</div>`);
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
    ${structureAuditHtml(selected, selectedDecision)}
    ${fvgAuditProofHtml(selected)}
    Timeframe: ${escapeHtml(selected.timeframe)}<br>
    Warnings: ${escapeHtml((selected.warnings || []).join(" | ") || "none")}<br>
    Read-only chart context.
  ` : "";
}

function closeLineAudit() {
  lineAuditPanel.classList.remove("visible");
  lineAuditToggle.classList.remove("active");
  lineAuditToggle.setAttribute("aria-pressed", "false");
}

function closeCandleCompare() {
  candleComparePanel.classList.remove("visible");
  candleCompareToggle.classList.remove("active");
  candleCompareToggle.setAttribute("aria-pressed", "false");
}

function compactOhlcv(values) {
  if (!values) return "not available";
  const price = value => Number.isFinite(Number(value)) ? Number(value).toFixed(4).replace(/0+$/, "").replace(/\.$/, "") : "n/a";
  return `O ${price(values.open)} · H ${price(values.high)} · L ${price(values.low)} · C ${price(values.close)} · V ${values.volume ?? "n/a"}`;
}

async function loadCandleCompare() {
  const requestedSymbol = activeSymbol;
  const requestedTimeframe = activeTimeframe;
  candleCompareMeta.textContent = `${requestedSymbol} · ${requestedTimeframe} · loading preserved audit data`;
  candleCompareList.innerHTML = `<div class="audit-meta">Loading raw and validated candle comparison...</div>`;
  try {
    const response = await fetch(`/api/debug/candle-compare?symbol=${encodeURIComponent(requestedSymbol)}&timeframe=${encodeURIComponent(requestedTimeframe)}`);
    const data = await response.json();
    if (requestedSymbol !== activeSymbol || requestedTimeframe !== activeTimeframe) return;
    if (!response.ok) throw new Error((data.errors || ["Candle comparison unavailable."]).join(", "));

    const rejected = data.rejected_candles || [];
    const mismatches = data.mismatches || [];
    candleCompareMeta.textContent = `${data.symbol} · ${data.timeframe} · ${data.feed || "provider"} · ${data.data_quality_status} · ${data.candle_accuracy_mode}`;
    candleCompareSummary.innerHTML = `
      <strong>Candle Data Status: ${escapeHtml(data.data_quality_status)}</strong><br>
      Displayed source: ${escapeHtml(data.candle_accuracy_mode)}<br>
      ${rejected.length
        ? `<span class="compare-warning">Raw Alpaca bad print filtered; chart displays rebuilt validated candle.</span>`
        : "No rejected raw Alpaca prints in the current audit window."}<br>
      Warnings: ${escapeHtml((data.comparison_warnings || []).join(" | ") || "none")}<br>
      Read-only comparison. Raw provider candles are audit data only.
    `;
    candleCompareList.innerHTML = mismatches.slice(-80).reverse().map(row => {
      const rejectedSources = row.rejected_source_candles || [];
      return `
        <div class="audit-item ${rejectedSources.length ? "compare-bad" : ""}">
          <div class="audit-row"><span class="audit-label">${escapeHtml(row.timestamp_et || "Unknown time")}</span><span>${escapeHtml(row.validation_status)}</span></div>
          <div class="audit-row audit-sub"><span>Display: ${row.used_for_display ? "Yes" : "No"}</span><span>Calculations: ${row.used_for_calculations ? "Yes" : "No"}</span></div>
          <div class="compare-values">Raw provider: ${escapeHtml(compactOhlcv(row.raw_provider_ohlcv))}<br>Rebuilt: ${escapeHtml(compactOhlcv(row.rebuilt_ohlcv))}<br>Displayed: ${escapeHtml(compactOhlcv(row.displayed_ohlcv))}</div>
          ${rejectedSources.map(source => `<div class="compare-values compare-warning">Rejected source ${escapeHtml(source.timestamp_et)}: ${escapeHtml(compactOhlcv(source.raw_ohlcv))} · Display No · Calculations No</div>`).join("")}
          <div class="compare-values">${escapeHtml(row.mismatch_reason || "")}</div>
        </div>
      `;
    }).join("") || `<div class="audit-meta">No raw-vs-rebuilt mismatches. ${rejected.length} rejected raw print(s) remain available in debug data.</div>`;
  } catch (error) {
    candleCompareMeta.textContent = `${activeSymbol} · ${activeTimeframe} · unavailable`;
    candleCompareList.innerHTML = `<div class="compare-summary compare-warning">${escapeHtml(error.message)}</div>`;
  }
}

function legendGroup(title, items, className = "") {
  return `<section class="legend-group ${className}"><span class="legend-group-title">${title}</span><div class="legend-pills">${items.filter(Boolean).join("")}</div></section>`;
}

function warningTone(text) {
  const value = String(text || "").toUpperCase();
  if (value.includes("NO_TRADE") || value.includes("CHOP") || value.includes("FAILED") || value.includes("INVALIDATED") || value.includes("DEGRADED")) return "alert";
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
  setPaperTradeFormDefaults();
  renderPaperTradePanel();
}

function resetSymbolScopedUi() {
  latestPayload = null;
  clearPaperTradeLines();
  removeAiEntryMarker();
  closeLineAudit();
  closeCandleCompare();
  lineAuditList.innerHTML = "";
  lineAuditMeta.textContent = `${activeSymbol} · ${activeTimeframe} · waiting for chart data`;
  lineAuditDetail.innerHTML = "";
  candleCompareMeta.textContent = `${activeSymbol} · ${activeTimeframe} · not loaded`;
  candleCompareSummary.innerHTML = "";
  candleCompareList.innerHTML = "";
  streamStatusEl.textContent = `${activeSymbol} stream waiting`;
  streamStatusEl.classList.remove("connected");
  streamStatusEl.classList.add("reconnecting");
  renderPaperTradePanel();
}

function loadSelectedSymbol() {
  const symbol = normalizeSymbolInput(symbolInput.value);
  if (!symbol) {
    errorEl.textContent = "Enter a valid stock or ETF symbol.";
    symbolInput.focus();
    return;
  }
  if (symbol === activeSymbol) {
    reloadForTimeframe();
    return;
  }
  activeSymbol = symbol;
  updateSymbolUi();
  resetSymbolScopedUi();
  reloadForTimeframe();
}

let rebuildBannerTimer = null;

function showRebuildBanner(message, tone = "") {
  if (!rebuildBanner) return;
  if (rebuildBannerTimer) clearTimeout(rebuildBannerTimer);
  rebuildBanner.textContent = message;
  rebuildBanner.className = `rebuild-banner visible ${tone}`.trim();
  rebuildBannerTimer = setTimeout(() => {
    rebuildBanner.className = "rebuild-banner";
  }, 7000);
}

async function rebuildChartData() {
  if (!rebuildChartButton) return;
  rebuildChartButton.disabled = true;
  rebuildChartButton.textContent = "Rebuilding...";
  statusEl.textContent = `Rebuilding ${activeSymbol} validated candles...`;
  errorEl.textContent = "";

  try {
    const response = await fetch(`/api/chart/rebuild?symbol=${encodeURIComponent(activeSymbol)}`, {
      method: "POST",
      headers: { "Accept": "application/json" },
    });
    const result = await response.json();
    if (!response.ok || result.data_status !== "ok") {
      throw new Error((result.errors || ["Chart rebuild failed."]).join(", "));
    }
    await reloadForTimeframe();
    const warnings = result.candle_data_warnings || [];
    showRebuildBanner(
      warnings.length
        ? `Chart data rebuilt from validated candles · ${warnings.join(" | ")}`
        : "Chart data rebuilt from validated candles",
      result.data_quality_status === "CLEAN" ? "" : "warning"
    );
  } catch (error) {
    errorEl.textContent = `Chart rebuild error: ${error.message}`;
    showRebuildBanner(`Chart rebuild failed: ${error.message}`, "error");
  } finally {
    rebuildChartButton.disabled = false;
    rebuildChartButton.textContent = "Rebuild Chart Data";
  }
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
    pill("PMH", levels.pmh), pill("PML", levels.pml), pill("PDH", levels.pdh), pill("PDL", levels.pdl),
    pill("HOD", levels.hod), pill("LOD", levels.lod), pill("OPEN 5M HIGH", levels.opening_5m_high), pill("OPEN 5M LOW", levels.opening_5m_low),
    cleanMode ? "" : pill("PDC", levels.pdc), cleanMode ? "" : pill("VWAP", latestVWAP), cleanMode ? "" : pill("EMA9", latestEMA9), cleanMode ? "" : pill("EMA20", latestEMA20),
    cleanMode ? "" : textPill(`Support ${(latestPayload.support_resistance?.support || []).map(x => `${x.price.toFixed(2)} ${x.quality_grade || x.reliability_label || ""}`).join(", ") || "none"}`),
    cleanMode ? "" : textPill(`Resistance ${(latestPayload.support_resistance?.resistance || []).map(x => `${x.price.toFixed(2)} ${x.quality_grade || x.reliability_label || ""}`).join(", ") || "none"}`),
    textPill(`Demand ${demandZones.map(z => `${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.reaction_label || z.zone_quality_grade || ""}`).join(", ") || "none"}`),
    textPill(`Supply ${supplyZones.map(z => `${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.reaction_label || z.zone_quality_grade || ""}`).join(", ") || "none"}`),
    textPill(`FVG ${(latestPayload.fair_value_gaps?.all || []).filter(g => ["ACTIVE", "PARTIALLY_FILLED"].includes(g.status)).slice(0, 4).map(g => `${g.type === "BULLISH_FVG" ? "Bull" : "Bear"} ${g.bottom.toFixed(2)}-${g.top.toFixed(2)} ${g.quality_grade}`).join(", ") || "none"}`),
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
    textPill(`Setups ${setups.map(s => `${s.setup_intent || s.setup_label || ""} ${s.professional_grade || ""} ${s.confirmation_stage === "EARLY_CONFIRM" ? "EARLY" : (s.confirmation_stage || s.status)} · ${formatRiskReward(s, !cleanMode)}`).join(" || ") || "none"}`, warningTone(setupStatus)),
  ];
  const warningText = (latestPayload.professional_context?.warnings || []).join(" | ") || "No active warnings";
  const riskItems = [
    textPill(
      `Candle Data ${latestPayload.data_quality_status || "DEGRADED"} · ${latestPayload.candle_accuracy_mode || "RAW_PROVIDER"}`,
      warningTone(latestPayload.data_quality_status)
    ),
    ...(latestPayload.candle_data_warnings || []).map(warning => textPill(warning, "caution")),
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

  streamStatusEl.textContent = stream.connected
    ? `${activeSymbol} stream connected`
    : `${activeSymbol} stream ${stream.error || "waiting"}`;
  streamStatusEl.classList.toggle("connected", Boolean(stream.connected));
  streamStatusEl.classList.toggle("reconnecting", !stream.connected);
  if (dataQualityStatusEl) {
    const quality = latestPayload.data_quality_status || "DEGRADED";
    const rejected = Number(latestPayload.rejected_candle_count || 0);
    const suspicious = Number(latestPayload.suspicious_candle_count || 0);
    dataQualityStatusEl.textContent = rejected
      ? "Candle Data WARNING — raw Alpaca bad print filtered"
      : quality === "CLEAN"
      ? `Data clean · ${latestPayload.candle_accuracy_mode || "VALIDATED"}`
      : `Data ${quality.toLowerCase()}${suspicious ? ` · ${suspicious} suspicious retained` : ""}`;
    dataQualityStatusEl.classList.toggle("data-warning", quality === "WARNING");
    dataQualityStatusEl.classList.toggle("data-degraded", quality === "DEGRADED");
  }
}


function addConfirmationSetup(label, setup) {
  if (!setup) return;

  const price = setup.level_price;
  if (price === null || price === undefined) return;

  let color = COLORS.confirmationWatch;
  let style = LightweightCharts.LineStyle.Dashed;
  const stage = setup.confirmation_stage || setup.status || "WATCH";
  const stageLabel = stage === "EARLY_CONFIRM" ? "EARLY" : stage;
  const intent = setup.setup_intent || setup.setup_label || "";
  const blocked = ["NO TRADE", "RESEARCH CONTEXT"].includes(intent);
  if (cleanMode && stage !== "CONFIRMED") return;
  if (cleanMode && blocked) return;

  if (stage === "CONFIRMED") {
    color = COLORS.confirmationConfirmed;
    style = LightweightCharts.LineStyle.Solid;
  }

  if (stage === "FAILED" || setup.status === "INVALIDATED") {
    color = COLORS.confirmationInvalid;
    style = LightweightCharts.LineStyle.Dotted;
  }

  addLevel(
    `${intent ? `${intent} · ` : ""}${stageLabel} ${String(setup.direction || "").toUpperCase()} ${setup.source || label} RR ${setup.risk_reward?.rr_grade || "n/a"} R1 ${setup.risk_reward?.rr_1 ?? "n/a"} R2 ${setup.risk_reward?.rr_2 ?? "n/a"}`,
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
  const showPremarketLevels = cleanMode || layerState.premarket;
  const showSessionKeyLevels = cleanMode || layerState.previousDay;

  if (showPremarketLevels) {
    addLevel("PMH", levels.pmh, COLORS.pmhPml);
    addLevel("PML", levels.pml, COLORS.pmhPml);
  }

  if (showSessionKeyLevels) {
    addLevel("PDH", levels.pdh, COLORS.previousHighLow, LightweightCharts.LineStyle.Dashed);
    addLevel("PDL", levels.pdl, COLORS.previousHighLow, LightweightCharts.LineStyle.Dashed);
    if (!cleanMode) addLevel("PDC", levels.pdc, COLORS.previousClose, LightweightCharts.LineStyle.Dotted);
    addLevel("HOD", levels.hod, COLORS.hodLod, LightweightCharts.LineStyle.Solid);
    addLevel("LOD", levels.lod, COLORS.hodLod, LightweightCharts.LineStyle.Solid);
    addLevel("OPEN 5M HIGH", levels.opening_5m_high, COLORS.openingRange, LightweightCharts.LineStyle.Dashed);
    addLevel("OPEN 5M LOW", levels.opening_5m_low, COLORS.openingRange, LightweightCharts.LineStyle.Dashed);
  }

  if (isLayerVisible("sr")) {
    const sr = data.support_resistance || {};
    const cleanSr = selectCleanModeSupportResistance(sr, currentChartPrice(data));
    const visibleLevels = (levels, side) => cleanMode ? cleanSr[side] : (levels || []);

    visibleLevels(sr.resistance, "resistance").forEach((level, index) => {
      const weak = level.quality_grade === "WEAK";
      addLevel(
        cleanMode ? `${weak ? "WEAK " : ""}RESISTANCE` : `R${index + 1} ${level.quality_grade || level.reliability_label || ""} ${level.quality_score ?? level.reliability_score ?? ""}`,
        level.price,
        weak ? COLORS.weakResistance : COLORS.resistance,
        weak ? LightweightCharts.LineStyle.Dotted : LightweightCharts.LineStyle.Dashed,
        true
      );
    });

    visibleLevels(sr.support, "support").forEach((level, index) => {
      const weak = level.quality_grade === "WEAK";
      addLevel(
        cleanMode ? `${weak ? "WEAK " : ""}SUPPORT` : `S${index + 1} ${level.quality_grade || level.reliability_label || ""} ${level.quality_score ?? level.reliability_score ?? ""}`,
        level.price,
        weak ? COLORS.weakSupport : COLORS.support,
        weak ? LightweightCharts.LineStyle.Dotted : LightweightCharts.LineStyle.Dashed,
        true
      );
    });
  }

  if (layerState.supplyDemand) {
    const sd = data.supply_demand || {};
    const cleanSd = selectCleanModeSupplyDemand(sd, currentChartPrice(data));

    (cleanMode ? cleanSd.supply : chartSupplyDemandZones(sd.supply)).forEach((zone, index) => {
      const weak = isWeakZone(zone);
      addZoneBand(`Supply ${index + 1}`, zone, { zone: weak ? COLORS.weakSupply : COLORS.supply }, {
        cleanLabel: cleanMode ? `${weak ? "WEAK " : ""}SUPPLY` : null,
        muted: cleanMode && weak,
      });
    });

    (cleanMode ? cleanSd.demand : chartSupplyDemandZones(sd.demand)).forEach((zone, index) => {
      const weak = isWeakZone(zone);
      addZoneBand(`Demand ${index + 1}`, zone, { zone: weak ? COLORS.weakDemand : COLORS.demand }, {
        cleanLabel: cleanMode ? `${weak ? "WEAK " : ""}DEMAND` : null,
        muted: cleanMode && weak,
      });
    });
  }

  if (isLayerVisible("fvg")) {
    renderFvgOverlay(data);
  } else if (fvgOverlay) {
    fvgOverlay.innerHTML = "";
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
  scheduleChartOverlays();
}

async function loadInitialChart() {
  errorEl.textContent = "";
  statusEl.textContent = "Loading chart...";
  const requestedSymbol = activeSymbol;
  const requestedTimeframe = activeTimeframe;

  const res = await fetch(`/api/chart?symbol=${encodeURIComponent(requestedSymbol)}&timeframe=${encodeURIComponent(requestedTimeframe)}`);
  const data = await res.json();
  if (requestedSymbol !== activeSymbol || requestedTimeframe !== activeTimeframe) return;

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
  renderPaperTradePlanner();

  didInitialLoad = true;

  statusEl.textContent = data.chart_session?.is_historical
    ? `Previous session: ${data.chart_session.date}`
    : `Initial load: ${new Date(data.timestamp).toLocaleString("en-US", {
        timeZone: "America/New_York",
      })} ET`;

  focusRecentCandles(data.candles);
  scheduleChartOverlays();
}

function connectStream() {
  if (eventSource) {
    eventSource.close();
  }

  if (latestPayload?.stream_supported === false) {
    eventSource = null;
    streamStatusEl.textContent = `${activeSymbol} external index refreshes every 30s`;
    streamStatusEl.classList.remove("connected");
    streamStatusEl.classList.add("reconnecting");
    return;
  }

  const streamSymbol = activeSymbol;
  const streamTimeframe = activeTimeframe;
  eventSource = new EventSource(`/api/stream?symbol=${encodeURIComponent(streamSymbol)}&timeframe=${encodeURIComponent(streamTimeframe)}`);

  eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.symbol && data.symbol !== activeSymbol) return;
    if (data.timeframe && data.timeframe !== activeTimeframe) return;
    if (streamSymbol !== activeSymbol || streamTimeframe !== activeTimeframe) return;

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
      renderPaperTradePlanner();
    }

    if (data.type === "data_quality_warning") {
      latestPayload = {
        ...(latestPayload || {}),
        data_quality_status: data.data_quality_status || "WARNING",
        candle_data_warnings: data.candle_data_warnings || [],
        bad_print_filter_enabled: true,
        stream_status: data.stream_status,
      };
      updateLegend(latestPayload);
      renderPaperTradePlanner();
    }

    if (data.type === "heartbeat") {
      latestPayload = {
        ...(latestPayload || {}),
        latest_trade: data.latest_trade || latestPayload?.latest_trade,
        stream_status: data.stream_status,
      };

      updateLegend(latestPayload);
      renderPaperTradePlanner();
    }
  };

  eventSource.onerror = () => {
    if (streamSymbol === activeSymbol && streamTimeframe === activeTimeframe) {
      streamStatusEl.textContent = `${activeSymbol} stream reconnecting...`;
    }
  };
}

async function reloadForTimeframe() {
  const requestedSymbol = activeSymbol;
  const requestedTimeframe = activeTimeframe;
  try {
    didInitialLoad = false;
    latestPayload = null;
    candleSeries.setData([]);
    updateChartEmptyState([]);
    vwapSeries.setData([]);
    ema9Series.setData([]);
    ema20Series.setData([]);
    clearPriceLines();
    clearPaperTradeLines();
    renderPaperTradePanel();

    await loadInitialChart();
    if (requestedSymbol !== activeSymbol || requestedTimeframe !== activeTimeframe) return;
    connectStream();
  } catch (err) {
    if (requestedSymbol !== activeSymbol || requestedTimeframe !== activeTimeframe) return;
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
rebuildChartButton?.addEventListener("click", rebuildChartData);
marketGridToggle?.addEventListener("click", () => setMarketGridVisible(!marketGridOpen()));
marketGridCloseButton?.addEventListener("click", () => setMarketGridVisible(false));
marketGridRefreshButton?.addEventListener("click", refreshMarketGrid);
marketGridCleanModeToggle?.addEventListener("click", () => {
  marketGridCleanMode = !marketGridCleanMode;
  saveMarketGridSettings();
  redrawMarketGridLayers();
});
marketGridLayerButtons.forEach(button => {
  button.addEventListener("click", () => {
    const layer = button.dataset.marketGridLayer;
    if (!layer) return;
    marketGridLayerState[layer] = !marketGridLayerState[layer];
    saveMarketGridSettings();
    redrawMarketGridLayers();
  });
});
paperTradeToggle?.addEventListener("click", () => {
  const visible = !paperTradePanel?.classList.contains("visible");
  paperTradePanel?.classList.toggle("visible", visible);
  paperTradeToggle.classList.toggle("active", visible);
  paperTradeToggle.setAttribute("aria-pressed", String(visible));
  if (visible) {
    setPaperTradeFormDefaults();
    renderPaperTradePanel();
  }
});
paperTradeClose?.addEventListener("click", closePaperTradePanel);
paperTradeTypeInput?.addEventListener("change", updatePaperTradeInputLanguage);
paperTradeForm?.addEventListener("submit", event => {
  event.preventDefault();
  try {
    const trade = readPaperTradeForm();
    paperTrades.forEach(existing => {
      if (paperTradeScopeMatches(existing, trade.symbol, trade.timeframe) && existing.status === "ACTIVE") {
        existing.status = "CANCELLED";
        existing.closed_at = new Date().toISOString();
        existing.close_reason = "Replaced by a new local paper trade. Paper trade only — no real order.";
      }
    });
    paperTrades.push(trade);
    savePaperTrades();
    setPaperTradeMessage("Paper trade added locally. Paper trade only — no real order.");
    renderPaperTradePlanner();
  } catch (error) {
    setPaperTradeMessage(error.message, "warning");
  }
});
paperCloseTradeButton?.addEventListener("click", () => {
  const trade = activePaperTrade();
  if (!trade) {
    setPaperTradeMessage("No active paper trade to close for this symbol/timeframe.", "warning");
    return;
  }
  trade.status = "MANUALLY_CLOSED";
  trade.closed_at = new Date().toISOString();
  trade.close_reason = "Manually closed locally. Paper trade only — no real order.";
  savePaperTrades();
  setPaperTradeMessage("Paper trade manually closed. No real order was sent.");
  renderPaperTradePlanner();
});
paperClearTradeButton?.addEventListener("click", () => {
  const trade = activePaperTrade();
  if (trade) {
    trade.status = "CANCELLED";
    trade.closed_at = new Date().toISOString();
    trade.close_reason = "Cleared from chart locally. Paper trade only — no real order.";
    savePaperTrades();
  }
  resetPaperTradeForm();
  setPaperTradeMessage("Cleared local paper trade drawing. No real order was sent.");
  renderPaperTradePlanner();
});
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
  if (visible) closeCandleCompare();
  lineAuditPanel.classList.toggle("visible", visible);
  lineAuditToggle.classList.toggle("active", visible);
  lineAuditToggle.setAttribute("aria-pressed", String(visible));
  if (visible) renderLineAudit();
});

lineAuditClose.addEventListener("click", () => {
  closeLineAudit();
});

lineAuditList.addEventListener("click", event => {
  const item = event.target.closest("[data-line-id]");
  if (item) renderLineAudit(item.dataset.lineId);
});

candleCompareToggle.addEventListener("click", () => {
  const visible = !candleComparePanel.classList.contains("visible");
  if (visible) {
    closeLineAudit();
    loadCandleCompare();
  }
  candleComparePanel.classList.toggle("visible", visible);
  candleCompareToggle.classList.toggle("active", visible);
  candleCompareToggle.setAttribute("aria-pressed", String(visible));
});

candleCompareClose.addEventListener("click", closeCandleCompare);

window.addEventListener("resize", () => {
  chart.applyOptions({ width: chartEl.clientWidth });
  resizeMarketGridCharts();
  scheduleChartOverlays();
});

chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
  scheduleChartOverlays();
});

setInterval(updateCountdown, 1000);
setInterval(refreshAiEntryMarker, 30000);
setInterval(refreshMarketGrid, 30000);

// Refresh static levels and indicators every 30 seconds.
// Live candle movement comes from the stream.
setInterval(() => {
  if (didInitialLoad) {
    const requestedSymbol = activeSymbol;
    const requestedTimeframe = activeTimeframe;
    fetch(`/api/chart?symbol=${encodeURIComponent(requestedSymbol)}&timeframe=${encodeURIComponent(requestedTimeframe)}`)
      .then(r => r.json())
      .then(data => {
        if (requestedSymbol !== activeSymbol || requestedTimeframe !== activeTimeframe) return;
        if (data.data_status === "ok") {
          latestPayload = data;
          const indicators = data.indicators || {};
          vwapSeries.setData(indicators.vwap || []);
          ema9Series.setData(indicators.ema9 || []);
          ema20Series.setData(indicators.ema20 || []);
          drawStaticLevels(data);
          updateLegend(data);
          renderPaperTradePlanner();
        }
      })
      .catch(() => {});
  }
}, 30000);

loadPaperTrades();
loadMarketGridLayout();
loadMarketGridSettings();
updateCleanModeControl();
updateMarketGridControls();
updateSymbolUi();
renderPaperTradePlanner();
reloadForTimeframe();
refreshAiEntryMarker();
