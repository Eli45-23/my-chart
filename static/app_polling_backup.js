const chartEl = document.getElementById("chart");
const legendEl = document.getElementById("legend");
const statusEl = document.getElementById("status");
const errorEl = document.getElementById("error");
const countdownEl = document.getElementById("countdown");
const tfButtons = document.querySelectorAll(".tf-btn");

let activeTimeframe = "1Min";
let refreshMs = 2000;

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
let refreshTimer = null;
let didInitialLoad = false;
let lastTimeframeLoaded = null;

function clearPriceLines() {
  for (const line of priceLines) {
    candleSeries.removePriceLine(line);
  }
  priceLines = [];
}

function addLevel(label, price, color, style = LightweightCharts.LineStyle.Solid) {
  if (price === null || price === undefined) return;
  const line = candleSeries.createPriceLine({
    price: price,
    color: color,
    lineWidth: 1,
    lineStyle: style,
    axisLabelVisible: true,
    title: `${label} ${Number(price).toFixed(2)}`,
  });
  priceLines.push(line);
}

function pill(label, value) {
  if (value === null || value === undefined) return "";
  return `<span class="pill">${label}: ${Number(value).toFixed(2)}</span>`;
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
  if (!countdownEl) return;

  const left = secondsLeftInCandle();
  const mm = String(Math.floor(left / 60)).padStart(2, "0");
  const ss = String(left % 60).padStart(2, "0");
  const label = activeTimeframe === "1Min" ? "1m" : activeTimeframe === "5Min" ? "5m" : "15m";
  countdownEl.textContent = `Next ${label} candle: ${mm}:${ss}`;
}

async function loadChart() {
  try {
    errorEl.textContent = "";
    const res = await fetch(`/api/chart/aapl?timeframe=${encodeURIComponent(activeTimeframe)}`);
    const data = await res.json();

    if (!res.ok || data.data_status !== "ok") {
      throw new Error((data.errors || ["Unknown error"]).join(", "));
    }

    const candles = data.candles || [];
    const indicators = data.indicators || {};

    const shouldFullReload =
      !didInitialLoad ||
      lastTimeframeLoaded !== activeTimeframe ||
      candles.length < 2;

    if (shouldFullReload) {
      candleSeries.setData(candles);
      vwapSeries.setData(indicators.vwap || []);
      ema9Series.setData(indicators.ema9 || []);
      ema20Series.setData(indicators.ema20 || []);
      didInitialLoad = true;
      lastTimeframeLoaded = activeTimeframe;
      chart.timeScale().fitContent();
    } else {
      const latestCandle = candles[candles.length - 1];
      if (latestCandle) {
        candleSeries.update(latestCandle);
      }

      const latestVWAP = indicators.vwap?.length ? indicators.vwap[indicators.vwap.length - 1] : null;
      const latestEMA9 = indicators.ema9?.length ? indicators.ema9[indicators.ema9.length - 1] : null;
      const latestEMA20 = indicators.ema20?.length ? indicators.ema20[indicators.ema20.length - 1] : null;

      if (latestVWAP) vwapSeries.update(latestVWAP);
      if (latestEMA9) ema9Series.update(latestEMA9);
      if (latestEMA20) ema20Series.update(latestEMA20);
    }

    clearPriceLines();

    const levels = data.levels || {};

    addLevel("PMH", levels.pmh, "#f6c85f");
    addLevel("PML", levels.pml, "#f6c85f");
    addLevel("PDH", levels.pdh, "#8ab4f8", LightweightCharts.LineStyle.Dashed);
    addLevel("PDL", levels.pdl, "#8ab4f8", LightweightCharts.LineStyle.Dashed);
    addLevel("PDC", levels.pdc, "#b39ddb", LightweightCharts.LineStyle.Dotted);

    const sr = data.support_resistance || {};
    (sr.resistance || []).forEach((level, index) => {
      addLevel(`R${index + 1}`, level.price, "#ff7043", LightweightCharts.LineStyle.Dashed);
    });
    (sr.support || []).forEach((level, index) => {
      addLevel(`S${index + 1}`, level.price, "#66bb6a", LightweightCharts.LineStyle.Dashed);
    });

    const latestVWAP = indicators.vwap?.length ? indicators.vwap[indicators.vwap.length - 1].value : null;
    const latestEMA9 = indicators.ema9?.length ? indicators.ema9[indicators.ema9.length - 1].value : null;
    const latestEMA20 = indicators.ema20?.length ? indicators.ema20[indicators.ema20.length - 1].value : null;

    legendEl.innerHTML = [
      textPill(`Timeframe: ${activeTimeframe.replace("Min", "m")}`),
      pill("Current", data.current_price),
      data.latest_trade?.timestamp ? textPill(`Latest trade: ${new Date(data.latest_trade.timestamp).toLocaleTimeString("en-US", { timeZone: "America/New_York" })} ET`) : "",
      pill("PMH", levels.pmh),
      pill("PML", levels.pml),
      pill("PDH", levels.pdh),
      pill("PDL", levels.pdl),
      pill("PDC", levels.pdc),
      pill("VWAP", latestVWAP),
      pill("EMA9", latestEMA9),
      pill("EMA20", latestEMA20),
      textPill(`Support: ${(data.support_resistance?.support || []).map(x => x.price.toFixed(2)).join(", ") || "none"}`),
      textPill(`Resistance: ${(data.support_resistance?.resistance || []).map(x => x.price.toFixed(2)).join(", ") || "none"}`),
      textPill(levels.premarket_window || "Premarket: 04:00-09:30 ET"),
    ].join("");

    statusEl.textContent = `Last update: ${new Date(data.timestamp).toLocaleString("en-US", {
      timeZone: "America/New_York",
    })} ET`;

    updateCountdown();
  } catch (err) {
    errorEl.textContent = `Chart error: ${err.message}`;
    statusEl.textContent = "Error loading chart";
  }
}

function startRefreshLoop() {
  if (refreshTimer) clearInterval(refreshTimer);

  if (activeTimeframe === "1Min") refreshMs = 1000;
  else if (activeTimeframe === "5Min") refreshMs = 5000;
  else refreshMs = 10000;

  loadChart();
  refreshTimer = setInterval(loadChart, refreshMs);
}

tfButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    activeTimeframe = btn.dataset.tf;

    tfButtons.forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");

    clearPriceLines();
    candleSeries.setData([]);
    vwapSeries.setData([]);
    ema9Series.setData([]);
    ema20Series.setData([]);
    didInitialLoad = false;
    lastTimeframeLoaded = null;

    startRefreshLoop();
  });
});

window.addEventListener("resize", () => {
  chart.applyOptions({ width: chartEl.clientWidth });
});

setInterval(updateCountdown, 1000);
startRefreshLoop();
