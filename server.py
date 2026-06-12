import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, send_from_directory

load_dotenv()
load_dotenv(os.path.expanduser("~/elite_scanner/.env"))

APP = Flask(__name__, static_folder="static")
ET = ZoneInfo("America/New_York")

SYMBOL = "AAPL"
ALPACA_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")


def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def get_headers():
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing Alpaca keys. Put them in ~/elite_scanner/.env or this folder's .env.")
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }




def fetch_latest_trade(symbol):
    url = f"{DATA_BASE_URL}/v2/stocks/{symbol}/trades/latest"
    params = {
        "feed": os.getenv("ALPACA_STOCK_FEED", "sip"),
    }
    response = requests.get(url, headers=get_headers(), params=params, timeout=10)
    response.raise_for_status()
    trade = response.json().get("trade") or {}
    return {
        "price": trade.get("p"),
        "size": trade.get("s"),
        "timestamp": trade.get("t"),
    }


def fetch_bars(symbol, start, end, timeframe="1Min", limit=10000):
    url = f"{DATA_BASE_URL}/v2/stocks/{symbol}/bars"
    params = {
        "timeframe": timeframe,
        "start": iso_utc(start),
        "end": iso_utc(end),
        "limit": limit,
        "adjustment": "raw",
        "feed": os.getenv("ALPACA_STOCK_FEED", "sip"),
    }
    response = requests.get(url, headers=get_headers(), params=params, timeout=20)
    response.raise_for_status()
    return response.json().get("bars", [])


def today_et():
    return datetime.now(ET).date()


def et_datetime(day, hour, minute=0):
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ET)


def previous_weekday(day):
    d = day - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def is_regular_bar(bar_time):
    t = bar_time.astimezone(ET)
    market_open = et_datetime(t.date(), 9, 30)
    market_close = et_datetime(t.date(), 16, 0)
    return market_open <= t <= market_close


def calc_ema(candles, period):
    values = []
    multiplier = 2 / (period + 1)
    ema = None

    for c in candles:
        close = c["close"]
        if ema is None:
            ema = close
        else:
            ema = (close - ema) * multiplier + ema

        values.append({
            "time": c["time"],
            "value": round(ema, 4),
        })

    return values


def calc_vwap(candles):
    values = []
    cumulative_pv = 0.0
    cumulative_volume = 0.0

    for c in candles:
        typical = (c["high"] + c["low"] + c["close"]) / 3
        volume = c.get("volume") or 0

        cumulative_pv += typical * volume
        cumulative_volume += volume

        if cumulative_volume > 0:
            values.append({
                "time": c["time"],
                "value": round(cumulative_pv / cumulative_volume, 4),
            })

    return values


def calc_levels(today_bars, prev_bars):
    day = today_et()

    premarket_start = et_datetime(day, 4, 0)
    market_open = et_datetime(day, 9, 30)

    pre_bars = []
    for b in today_bars:
        t = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
        if premarket_start <= t < market_open:
            pre_bars.append(b)

    regular_prev = []
    prev_day = previous_weekday(day)
    prev_open = et_datetime(prev_day, 9, 30)
    prev_close = et_datetime(prev_day, 16, 0)

    for b in prev_bars:
        t = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
        if prev_open <= t <= prev_close:
            regular_prev.append(b)

    pmh = max((b["h"] for b in pre_bars), default=None)
    pml = min((b["l"] for b in pre_bars), default=None)

    pdh = max((b["h"] for b in regular_prev), default=None)
    pdl = min((b["l"] for b in regular_prev), default=None)
    pdc = regular_prev[-1]["c"] if regular_prev else None

    return {
        "pmh": pmh,
        "pml": pml,
        "pdh": pdh,
        "pdl": pdl,
        "pdc": pdc,
        "premarket_window": "04:00 ET <= candle time < 09:30 ET",
    }




def round_price(value):
    if value is None:
        return None
    return round(float(value), 2)


def cluster_levels(levels, max_gap=0.08):
    if not levels:
        return []

    levels = sorted(round_price(x) for x in levels if x is not None)
    clusters = []

    for level in levels:
        if not clusters:
            clusters.append([level])
            continue

        current = clusters[-1]
        avg = sum(current) / len(current)

        if abs(level - avg) <= max_gap:
            current.append(level)
        else:
            clusters.append([level])

    results = []
    for cluster in clusters:
        avg = round_price(sum(cluster) / len(cluster))
        results.append({
            "price": avg,
            "touches": len(cluster),
        })

    return results


def detect_support_resistance(candles, current_price=None, lookback=3, max_levels=3):
    highs = []
    lows = []

    if len(candles) < lookback * 2 + 1:
        return {"support": [], "resistance": []}

    for i in range(lookback, len(candles) - lookback):
        window = candles[i - lookback:i + lookback + 1]
        center = candles[i]

        if center["high"] == max(c["high"] for c in window):
            highs.append(center["high"])

        if center["low"] == min(c["low"] for c in window):
            lows.append(center["low"])

    resistance_clusters = cluster_levels(highs)
    support_clusters = cluster_levels(lows)

    if current_price is not None:
        resistance_clusters = [x for x in resistance_clusters if x["price"] >= current_price]
        support_clusters = [x for x in support_clusters if x["price"] <= current_price]

        resistance_clusters = sorted(
            resistance_clusters,
            key=lambda x: (abs(x["price"] - current_price), -x["touches"])
        )[:max_levels]

        support_clusters = sorted(
            support_clusters,
            key=lambda x: (abs(x["price"] - current_price), -x["touches"])
        )[:max_levels]
    else:
        resistance_clusters = sorted(resistance_clusters, key=lambda x: -x["touches"])[:max_levels]
        support_clusters = sorted(support_clusters, key=lambda x: -x["touches"])[:max_levels]

    return {
        "support": support_clusters,
        "resistance": resistance_clusters,
    }


@APP.route("/")
def home():
    return send_from_directory("static", "index.html")


@APP.route("/app.js")
def app_js():
    return send_from_directory("static", "app.js")


@APP.route("/api/chart/aapl")
def chart_data():
    from flask import request
    tf = request.args.get("timeframe", "1Min")
    allowed = {
        "1m": "1Min",
        "1Min": "1Min",
        "5m": "5Min",
        "5Min": "5Min",
        "15m": "15Min",
        "15Min": "15Min",
    }
    timeframe = allowed.get(tf, "1Min")
    now = datetime.now(ET)
    day = now.date()
    prev_day = previous_weekday(day)

    today_start = et_datetime(day, 4, 0)
    today_end = now

    prev_start = et_datetime(prev_day, 9, 30)
    prev_end = et_datetime(prev_day, 16, 5)

    try:
        today_bars = fetch_bars(SYMBOL, today_start, today_end, timeframe=timeframe)
        prev_bars = fetch_bars(SYMBOL, prev_start, prev_end, timeframe=timeframe)

        levels = calc_levels(today_bars, prev_bars)

        candles = []
        for b in today_bars:
            bar_dt = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
            candles.append({
                "time": int(datetime.fromisoformat(b["t"].replace("Z", "+00:00")).timestamp()),
                "et_time": bar_dt.strftime("%H:%M"),
                "open": b["o"],
                "high": b["h"],
                "low": b["l"],
                "close": b["c"],
                "volume": b.get("v", 0),
            })

        regular_candles = []
        for c in candles:
            utc_dt = datetime.fromtimestamp(c["time"], tz=timezone.utc)
            if is_regular_bar(utc_dt):
                regular_candles.append(c)

        latest_trade = fetch_latest_trade(SYMBOL)
        current_price = latest_trade.get("price") or (candles[-1]["close"] if candles else None)

        indicators_source = regular_candles if regular_candles else candles
        sr_source = regular_candles if regular_candles else candles
        support_resistance = detect_support_resistance(sr_source, current_price=current_price)

        # Make the active candle feel live by updating the latest candle with latest trade price.
        if candles and current_price:
            candles[-1]["close"] = current_price
            candles[-1]["high"] = max(candles[-1]["high"], current_price)
            candles[-1]["low"] = min(candles[-1]["low"], current_price)

        return jsonify({
            "symbol": SYMBOL,
            "timeframe": timeframe,
            "timestamp": now.isoformat(),
            "current_price": current_price,
            "latest_trade": latest_trade,
            "candles": candles,
            "levels": levels,
            "support_resistance": support_resistance,
            "indicators": {
                "vwap": calc_vwap(indicators_source),
                "ema9": calc_ema(indicators_source, 9),
                "ema20": calc_ema(indicators_source, 20),
            },
            "data_status": "ok",
            "errors": [],
        })
    except Exception as e:
        return jsonify({
            "symbol": SYMBOL,
            "timeframe": timeframe,
            "timestamp": now.isoformat(),
            "current_price": None,
            "candles": [],
            "levels": {},
            "indicators": {},
            "data_status": "error",
            "errors": [str(e)],
        }), 500


if __name__ == "__main__":
    APP.run(host="127.0.0.1", port=8899, debug=True)
