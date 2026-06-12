const chartEl = document.getElementById("chart");
const legendEl = document.getElementById("legend");
const statusEl = document.getElementById("status");
const errorEl = document.getElementById("error");
const countdownEl = document.getElementById("countdown");
const streamStatusEl = document.getElementById("streamStatus");
const tfButtons = document.querySelectorAll(".tf-btn");
const toggleButtons = document.querySelectorAll(".toggle-btn");

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
};

const COLORS = {
  pmhPml: "#f6c85f",
  previousHighLow: "#8ab4f8",
  previousClose: "#b39ddb",
  resistance: "#ff9800",
  support: "#2196f3",
  supply: "#ff4d6d",
  demand: "#00c853",
  trigger: "#ffd600",
  invalidation: "#9e9e9e",
  liquiditySweep: "#d500f9",
  liquiditySweepAlt: "#7c4dff",
  upsideCluster: "#ffb300",
  downsideCluster: "#00e5ff",
  supportWatch: "#64b5f6",
  resistanceWatch: "#ffcc80",
};

let activeTimeframe = "1Min";
let eventSource = null;
let didInitialLoad = false;
let latestPayload = null;

const timeframeSeconds = {
  "1Min": 60,
  "5Min": 300,
  "15Min": 900,
};

const chart = LightweightCharts.createChart(chartEl, {
  layout: {
    background: { color: "#0f1115" },
    textColor: "#d6d9df",
  },
  grid: {
    vertLines: { color: "#1d2430" },
    horzLines: { color: "#1d2430" },
  },
  rightPriceScale: {
    borderColor: "#2d3545",
  },
  timeScale: {
    borderColor: "#2d3545",
    timeVisible: true,
    secondsVisible: false,
  },
  localization: {
    timeFormatter: (time) => {
      const date = new Date(time * 1000);
      return date.toLocaleTimeString("en-US", {
        timeZone: "America/New_York",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
    },
  },
  crosshair: {
    mode: LightweightCharts.CrosshairMode.Normal,
  },
});

const candleSeries = chart.addCandlestickSeries({
  upColor: "#26a69a",
  downColor: "#ef5350",
  borderUpColor: "#26a69a",
  borderDownColor: "#ef5350",
  wickUpColor: "#26a69a",
  wickDownColor: "#ef5350",
});

const vwapSeries = chart.addLineSeries({
  color: "#fbc02d",
  lineWidth: 2,
  priceLineVisible: false,
  title: "VWAP",
});

const ema9Series = chart.addLineSeries({
  color: "#42a5f5",
  lineWidth: 1,
  priceLineVisible: false,
  title: "EMA9",
});

const ema20Series = chart.addLineSeries({
  color: "#ab47bc",
  lineWidth: 1,
  priceLineVisible: false,
  title: "EMA20",
});

let priceLines = [];

function applyIndicatorVisibility() {
  vwapSeries.applyOptions({ visible: layerState.vwap });
  ema9Series.applyOptions({ visible: layerState.emas });
  ema20Series.applyOptions({ visible: layerState.emas });
}

function clearPriceLines() {
  for (const line of priceLines) {
    candleSeries.removePriceLine(line);
  }
  priceLines = [];
}

function addLevel(label, price, color, style = LightweightCharts.LineStyle.Solid, showLabel = true) {
  if (price === null || price === undefined) return;
  const line = candleSeries.createPriceLine({
    price,
    color,
    lineWidth: 1,
    lineStyle: style,
    axisLabelVisible: showLabel,
    title: showLabel ? `${label} ${Number(price).toFixed(2)}` : "",
  });
  priceLines.push(line);
}


function addZoneBand(label, zone, colors) {
  if (!zone) return;

  const low = zone.low;
  const high = zone.high;

  if (low === null || low === undefined || high === null || high === undefined) return;

  const quality = zone.label || "Zone";

  addLevel(`${label} High ${quality}`, high, colors.zone, LightweightCharts.LineStyle.Dotted);
  addLevel(`${label} Low ${quality}`, low, colors.zone, LightweightCharts.LineStyle.Dotted);

  if (zone.trigger !== null && zone.trigger !== undefined) {
    addLevel(`${label} Trigger`, zone.trigger, COLORS.trigger, LightweightCharts.LineStyle.Dashed);
  }

  if (zone.invalidation !== null && zone.invalidation !== undefined) {
    addLevel(`${label} Invalid`, zone.invalidation, COLORS.invalidation, LightweightCharts.LineStyle.Dotted);
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

function textPill(text) {
  return `<span class="pill">${text}</span>`;
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

  const levels = latestPayload.levels || {};
  const indicators = latestPayload.indicators || {};
  const latestVWAP = indicators.vwap?.length ? indicators.vwap[indicators.vwap.length - 1].value : null;
  const latestEMA9 = indicators.ema9?.length ? indicators.ema9[indicators.ema9.length - 1].value : null;
  const latestEMA20 = indicators.ema20?.length ? indicators.ema20[indicators.ema20.length - 1].value : null;
  const stream = latestPayload.stream_status || {};
  const trade = latestPayload.latest_trade;

  legendEl.innerHTML = [
    textPill(`Timeframe: ${activeTimeframe.replace("Min", "m")}`),
    pill("Current", latestPayload.current_price || trade?.price),
    trade?.timestamp ? textPill(`Latest trade: ${new Date(trade.timestamp).toLocaleTimeString("en-US", { timeZone: "America/New_York" })} ET`) : "",
    pill("PMH", levels.pmh),
    pill("PML", levels.pml),
    pill("PDH", levels.pdh),
    pill("PDL", levels.pdl),
    pill("PDC", levels.pdc),
    pill("VWAP", latestVWAP),
    pill("EMA9", latestEMA9),
    pill("EMA20", latestEMA20),
    textPill(`Support: ${(latestPayload.support_resistance?.support || []).map(x => `${x.price.toFixed(2)} ${x.reliability_label || ""} ${x.reliability_score || ""}`).join(", ") || "none"}`),
    textPill(`Resistance: ${(latestPayload.support_resistance?.resistance || []).map(x => `${x.price.toFixed(2)} ${x.reliability_label || ""} ${x.reliability_score || ""}`).join(", ") || "none"}`),
    textPill(`Reaction Zones: ${[
      ...(latestPayload.structure_reactions?.resistance_watch || []).map(z => `Watch R ${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.score || ""}`),
      ...(latestPayload.structure_reactions?.support_watch || []).map(z => `Watch S ${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.score || ""}`),
    ].join(" | ") || "none"}`),
    textPill(`Demand: ${(latestPayload.supply_demand?.demand || []).map(z => `${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.label || ""} ${z.reliability_score || ""} T:${z.trigger?.toFixed(2) || "n/a"}`).join(", ") || "none"}`),
    textPill(`Supply: ${(latestPayload.supply_demand?.supply || []).map(z => `${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.label || ""} ${z.reliability_score || ""} T:${z.trigger?.toFixed(2) || "n/a"}`).join(", ") || "none"}`),
    textPill(`Upside Sweep: ${(latestPayload.liquidity_sweeps?.upside || []).map(z => `${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.source || ""}`).join(", ") || "none"}`),
    textPill(`Downside Sweep: ${(latestPayload.liquidity_sweeps?.downside || []).map(z => `${z.low.toFixed(2)}-${z.high.toFixed(2)} ${z.source || ""}`).join(", ") || "none"}`),
    textPill(`Clusters: ${(latestPayload.level_clusters?.clusters || []).map(c => `${c.low.toFixed(2)}-${c.high.toFixed(2)} ${c.label || ""}`).join(" | ") || "none"}`),
    textPill(levels.premarket_window || "Premarket: 04:00-09:30 ET"),
  ].join("");

  streamStatusEl.textContent = stream.connected ? "Stream: connected" : `Stream: ${stream.error || "waiting"}`;
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

  if (layerState.sr) {
    const sr = data.support_resistance || {};

    (sr.resistance || []).forEach((level, index) => {
      addLevel(`R${index + 1} ${level.reliability_label || ""} ${level.reliability_score || ""}`, level.price, COLORS.resistance, LightweightCharts.LineStyle.Dashed);
    });

    (sr.support || []).forEach((level, index) => {
      addLevel(`S${index + 1} ${level.reliability_label || ""} ${level.reliability_score || ""}`, level.price, COLORS.support, LightweightCharts.LineStyle.Dashed);
    });
  }

  if (layerState.supplyDemand) {
    const sd = data.supply_demand || {};

    const shouldShowZone = (zone) => {
      const label = zone.label || "";
      if (label.includes("Weak Zone") && !layerState.weakZones) return false;
      return true;
    };

    (sd.supply || []).filter(shouldShowZone).forEach((zone, index) => {
      addZoneBand(`Supply ${index + 1}`, zone, { zone: COLORS.supply });
    });

    (sd.demand || []).filter(shouldShowZone).forEach((zone, index) => {
      addZoneBand(`Demand ${index + 1}`, zone, { zone: COLORS.demand });
    });
  }

  if (layerState.liquiditySweeps) {
    const sweeps = data.liquidity_sweeps || {};

    (sweeps.upside || []).forEach((zone, index) => {
      addSweepZone(`Upside Sweep ${zone.source || index + 1}`, zone, COLORS.liquiditySweep);
    });

    (sweeps.downside || []).forEach((zone, index) => {
      addSweepZone(`Downside Sweep ${zone.source || index + 1}`, zone, COLORS.liquiditySweepAlt);
    });
  }

  if (layerState.clusters) {
    const clusters = data.level_clusters?.clusters || [];

    clusters.forEach((cluster, index) => {
      const color = cluster.kind === "upside" ? COLORS.upsideCluster : COLORS.downsideCluster;
      const label = cluster.kind === "upside" ? `Upside Cluster ${index + 1}` : `Downside Cluster ${index + 1}`;
      addClusterZone(label, cluster, color);
    });
  }

  if (layerState.reactionZones) {
    const reactions = data.structure_reactions || {};

    (reactions.resistance_watch || []).forEach((zone, index) => {
      addReactionZone(`Watch R${index + 1}`, zone, COLORS.resistanceWatch);
    });

    (reactions.support_watch || []).forEach((zone, index) => {
      addReactionZone(`Watch S${index + 1}`, zone, COLORS.supportWatch);
    });
  }

  applyIndicatorVisibility();
}

async function loadInitialChart() {
  errorEl.textContent = "";
  statusEl.textContent = "Loading chart...";

  const res = await fetch(`/api/chart/aapl?timeframe=${encodeURIComponent(activeTimeframe)}`);
  const data = await res.json();

  if (!res.ok || data.data_status !== "ok") {
    throw new Error((data.errors || ["Unknown error"]).join(", "));
  }

  candleSeries.setData(data.candles || []);

  const indicators = data.indicators || {};
  vwapSeries.setData(indicators.vwap || []);
  ema9Series.setData(indicators.ema9 || []);
  ema20Series.setData(indicators.ema20 || []);

  drawStaticLevels(data);
  updateLegend(data);

  didInitialLoad = true;

  statusEl.textContent = `Initial load: ${new Date(data.timestamp).toLocaleString("en-US", {
    timeZone: "America/New_York",
  })} ET`;

  chart.timeScale().fitContent();
}

function connectStream() {
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource(`/api/stream/aapl?timeframe=${encodeURIComponent(activeTimeframe)}`);

  eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);

    if (data.type === "live_candle" && data.candle) {
      candleSeries.update(data.candle);

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
    vwapSeries.setData([]);
    ema9Series.setData([]);
    ema20Series.setData([]);
    clearPriceLines();

    await loadInitialChart();
    connectStream();
  } catch (err) {
    errorEl.textContent = `Chart error: ${err.message}`;
    statusEl.textContent = "Error loading chart";
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


toggleButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    const layer = btn.dataset.layer;
    layerState[layer] = !layerState[layer];

    btn.classList.toggle("active", layerState[layer]);

    if (latestPayload) {
      drawStaticLevels(latestPayload);
    }

    applyIndicatorVisibility();
  });
});

window.addEventListener("resize", () => {
  chart.applyOptions({ width: chartEl.clientWidth });
});

setInterval(updateCountdown, 1000);

// Refresh static levels and indicators every 30 seconds.
// Live candle movement comes from the stream.
setInterval(() => {
  if (didInitialLoad) {
    fetch(`/api/chart/aapl?timeframe=${encodeURIComponent(activeTimeframe)}`)
      .then(r => r.json())
      .then(data => {
        if (data.data_status === "ok") {
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

reloadForTimeframe();
