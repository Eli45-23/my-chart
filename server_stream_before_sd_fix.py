import json
import os
import queue
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import websocket
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, send_from_directory, request

load_dotenv()
load_dotenv(os.path.expanduser("~/elite_scanner/.env"))

APP = Flask(__name__, static_folder="static")
ET = ZoneInfo("America/New_York")

SYMBOL = "AAPL"
ALPACA_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
FEED = os.getenv("ALPACA_STOCK_FEED", "sip").lower()

TIMEFRAMES = {
    "1Min": 60,
    "5Min": 300,
    "15Min": 900,
}

subscribers = {tf: [] for tf in TIMEFRAMES}
live_candles = {tf: None for tf in TIMEFRAMES}
latest_trade = None
stream_status = {
    "connected": False,
    "last_message": None,
    "error": None,
}


def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def get_headers():
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing Alpaca keys. Put them in ~/elite_scanner/.env or this folder's .env.")
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }


def fetch_bars(symbol, start, end, timeframe="1Min", limit=10000):
    url = f"{DATA_BASE_URL}/v2/stocks/{symbol}/bars"
    params = {
        "timeframe": timeframe,
        "start": iso_utc(start),
        "end": iso_utc(end),
        "limit": limit,
        "adjustment": "raw",
        "feed": FEED,
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


def is_regular_dt(dt_utc):
    t = dt_utc.astimezone(ET)
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
        values.append({"time": c["time"], "value": round(ema, 4)})

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
            values.append({"time": c["time"], "value": round(cumulative_pv / cumulative_volume, 4)})

    return values


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

    return [{"price": round_price(sum(c) / len(c)), "touches": len(c)} for c in clusters]


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

    resistance = cluster_levels(highs)
    support = cluster_levels(lows)

    if current_price is not None:
        resistance = [x for x in resistance if x["price"] >= current_price]
        support = [x for x in support if x["price"] <= current_price]

        resistance = sorted(resistance, key=lambda x: (abs(x["price"] - current_price), -x["touches"]))[:max_levels]
        support = sorted(support, key=lambda x: (abs(x["price"] - current_price), -x["touches"]))[:max_levels]
    else:
        resistance = sorted(resistance, key=lambda x: -x["touches"])[:max_levels]
        support = sorted(support, key=lambda x: -x["touches"])[:max_levels]

    return {"support": support, "resistance": resistance}



def detect_supply_demand_zones(candles, current_price=None, lookback=3, max_zones=2):
    """
    Simple prototype supply/demand detection for chart visualization.

    Demand = swing low area with bounce away.
    Supply = swing high area with rejection away.

    This is chart-prototype logic only. Later we should replace this with
    Mr. Scanner's real supply_demand_engine output.
    """
    if len(candles) < lookback * 2 + 5:
        return {"demand": [], "supply": []}

    demand = []
    supply = []

    for i in range(lookback, len(candles) - lookback - 1):
        window = candles[i - lookback:i + lookback + 1]
        center = candles[i]

        body_low = min(center["open"], center["close"])
        body_high = max(center["open"], center["close"])
        candle_range = max(center["high"] - center["low"], 0.01)

        next_bars = candles[i + 1:i + 4]
        if not next_bars:
            continue

        future_high = max(b["high"] for b in next_bars)
        future_low = min(b["low"] for b in next_bars)

        # Demand: swing low, then impulse/bounce away.
        if center["low"] == min(c["low"] for c in window):
            bounce = future_high - center["low"]
            reaction_score = int(min(100, max(0, (bounce / candle_range) * 35)))
            zone_low = round_price(center["low"])
            zone_high = round_price(body_high)

            if zone_high > zone_low and reaction_score >= 45:
                demand.append({
                    "type": "demand",
                    "low": zone_low,
                    "high": zone_high,
                    "mid": round_price((zone_low + zone_high) / 2),
                    "reaction_score": reaction_score,
                    "label": "Demand Watch",
                    "source": "swing_low_bounce",
                    "time": center["time"],
                })

        # Supply: swing high, then rejection away.
        if center["high"] == max(c["high"] for c in window):
            rejection = center["high"] - future_low
            reaction_score = int(min(100, max(0, (rejection / candle_range) * 35)))
            zone_low = round_price(body_low)
            zone_high = round_price(center["high"])

            if zone_high > zone_low and reaction_score >= 45:
                supply.append({
                    "type": "supply",
                    "low": zone_low,
                    "high": zone_high,
                    "mid": round_price((zone_low + zone_high) / 2),
                    "reaction_score": reaction_score,
                    "label": "Supply Watch",
                    "source": "swing_high_rejection",
                    "time": center["time"],
                })

    if current_price is not None:
        demand = [z for z in demand if z["high"] <= current_price]
        supply = [z for z in supply if z["low"] >= current_price]

        demand = sorted(
            demand,
            key=lambda z: (abs(current_price - z["high"]), -z["reaction_score"])
        )[:max_zones]

        supply = sorted(
            supply,
            key=lambda z: (abs(z["low"] - current_price), -z["reaction_score"])
        )[:max_zones]
    else:
        demand = sorted(demand, key=lambda z: -z["reaction_score"])[:max_zones]
        supply = sorted(supply, key=lambda z: -z["reaction_score"])[:max_zones]

    return {
        "demand": demand,
        "supply": supply,
    }


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

    return {
        "pmh": max((b["h"] for b in pre_bars), default=None),
        "pml": min((b["l"] for b in pre_bars), default=None),
        "pdh": max((b["h"] for b in regular_prev), default=None),
        "pdl": min((b["l"] for b in regular_prev), default=None),
        "pdc": regular_prev[-1]["c"] if regular_prev else None,
        "premarket_window": "04:00 ET <= candle time < 09:30 ET",
    }


def normalize_candles(bars):
    candles = []
    for b in bars:
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
    return candles


def bucket_time(timestamp_utc, tf_seconds):
    epoch = int(timestamp_utc.timestamp())
    return epoch - (epoch % tf_seconds)



def parse_alpaca_timestamp(ts):
    """
    Alpaca stream timestamps can include nanoseconds.
    Normalize safely to Python-supported microseconds.
    """
    if not ts:
        raise ValueError("missing timestamp")

    value = str(ts).strip()

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    tz_part = "+00:00"
    main = value

    if "+" in value:
        main, tz = value.rsplit("+", 1)
        tz_part = "+" + tz
    elif value.count("-") > 2:
        main, tz = value.rsplit("-", 1)
        tz_part = "-" + tz

    if "." in main:
        whole, frac = main.split(".", 1)
        frac = "".join(ch for ch in frac if ch.isdigit())
        frac = (frac[:6]).ljust(6, "0")
        value = f"{whole}.{frac}{tz_part}"
    else:
        value = f"{main}{tz_part}"

    return datetime.fromisoformat(value).astimezone(timezone.utc)

def update_live_candles(price, size, trade_time_utc):
    global latest_trade

    latest_trade = {
        "price": price,
        "size": size,
        "timestamp": trade_time_utc.isoformat().replace("+00:00", "Z"),
    }

    for tf, seconds in TIMEFRAMES.items():
        bucket = bucket_time(trade_time_utc, seconds)
        candle = live_candles.get(tf)

        if not candle or candle["time"] != bucket:
            candle = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size or 0,
            }
        else:
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            candle["volume"] = (candle.get("volume") or 0) + (size or 0)

        live_candles[tf] = candle

        event = {
            "type": "live_candle",
            "symbol": SYMBOL,
            "timeframe": tf,
            "candle": candle,
            "latest_trade": latest_trade,
            "stream_status": stream_status,
        }

        for q in list(subscribers.get(tf, [])):
            try:
                q.put_nowait(event)
            except Exception:
                pass


def on_open(ws):
    print("ALPACA STREAM OPENED")
    stream_status["connected"] = True
    stream_status["error"] = None

    ws.send(json.dumps({
        "action": "auth",
        "key": ALPACA_KEY,
        "secret": ALPACA_SECRET,
    }))

    ws.send(json.dumps({
        "action": "subscribe",
        "trades": [SYMBOL],
    }))


def on_message(ws, message):
    stream_status["last_message"] = datetime.now(timezone.utc).isoformat()

    try:
        data = json.loads(message)
    except Exception:
        return

    if not isinstance(data, list):
        return

    for item in data:
        if item.get("T") != "t":
            print("ALPACA STREAM MESSAGE:", item)
            continue

        if item.get("S") != SYMBOL:
            continue

        price = item.get("p")
        size = item.get("s", 0)
        ts = item.get("t")

        if price is None or not ts:
            continue

        try:
            trade_time = parse_alpaca_timestamp(ts)
        except Exception as e:
            print(f"SKIP TRADE BAD TIMESTAMP: {ts} error={e}")
            continue

        update_live_candles(float(price), int(size or 0), trade_time)


def on_error(ws, error):
    print("ALPACA STREAM ERROR:", error)
    stream_status["connected"] = False
    stream_status["error"] = str(error)


def on_close(ws, close_status_code, close_msg):
    stream_status["connected"] = False
    stream_status["error"] = f"closed: {close_status_code} {close_msg}"


def stream_worker():
    while True:
        try:
            if not ALPACA_KEY or not ALPACA_SECRET:
                stream_status["connected"] = False
                stream_status["error"] = "missing Alpaca keys"
                time.sleep(5)
                continue

            url = f"wss://stream.data.alpaca.markets/v2/{FEED}"

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            stream_status["connected"] = False
            stream_status["error"] = str(e)

        time.sleep(3)


@APP.route("/")
def home():
    return send_from_directory("static", "index_stream.html")


@APP.route("/app_stream.js")
def app_stream_js():
    return send_from_directory("static", "app_stream.js")


@APP.route("/api/chart/aapl")
def chart_data():
    tf = request.args.get("timeframe", "1Min")
    timeframe = tf if tf in TIMEFRAMES else "1Min"

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
        candles = normalize_candles(today_bars)

        current_price = latest_trade["price"] if latest_trade else (candles[-1]["close"] if candles else None)

        if candles and live_candles.get(timeframe):
            if candles[-1]["time"] == live_candles[timeframe]["time"]:
                candles[-1] = live_candles[timeframe]
            else:
                candles.append(live_candles[timeframe])

        regular_candles = []
        for c in candles:
            dt_utc = datetime.fromtimestamp(c["time"], tz=timezone.utc)
            if is_regular_dt(dt_utc):
                regular_candles.append(c)

        indicators_source = regular_candles if regular_candles else candles
        support_resistance = detect_support_resistance(indicators_source, current_price=current_price)
        supply_demand = detect_supply_demand_zones(indicators_source, current_price=current_price)

        return jsonify({
            "symbol": SYMBOL,
            "timeframe": timeframe,
            "timestamp": now.isoformat(),
            "current_price": current_price,
            "latest_trade": latest_trade,
            "stream_status": stream_status,
            "candles": candles,
            "levels": levels,
            "support_resistance": support_resistance,
            "supply_demand": supply_demand,
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
            "latest_trade": latest_trade,
            "stream_status": stream_status,
            "candles": [],
            "levels": {},
            "support_resistance": {"support": [], "resistance": []},
            "supply_demand": {"demand": [], "supply": []},
            "indicators": {},
            "data_status": "error",
            "errors": [str(e)],
        }), 500


@APP.route("/api/stream/aapl")
def stream_chart():
    tf = request.args.get("timeframe", "1Min")
    timeframe = tf if tf in TIMEFRAMES else "1Min"

    q = queue.Queue(maxsize=100)
    subscribers[timeframe].append(q)

    def event_stream():
        try:
            while True:
                try:
                    event = q.get(timeout=15)
                    yield f"data: {json.dumps(event)}\n\n"
                except queue.Empty:
                    heartbeat = {
                        "type": "heartbeat",
                        "symbol": SYMBOL,
                        "timeframe": timeframe,
                        "stream_status": stream_status,
                        "latest_trade": latest_trade,
                    }
                    yield f"data: {json.dumps(heartbeat)}\n\n"
        finally:
            if q in subscribers.get(timeframe, []):
                subscribers[timeframe].remove(q)

    return Response(event_stream(), mimetype="text/event-stream")


@APP.route("/api/debug/stream-status")
def debug_stream_status():
    return jsonify({
        "stream_status": stream_status,
        "latest_trade": latest_trade,
        "live_candles": live_candles,
        "subscriber_counts": {k: len(v) for k, v in subscribers.items()},
        "feed": FEED,
        "symbol": SYMBOL,
    })


if __name__ == "__main__":
    thread = threading.Thread(target=stream_worker, daemon=True)
    thread.start()

    APP.run(host="127.0.0.1", port=8900, debug=False, threaded=True)
