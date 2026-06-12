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



def average_volume(candles, end_index, length=20):
    start = max(0, end_index - length)
    vols = [c.get("volume") or 0 for c in candles[start:end_index] if c.get("volume") is not None]
    if not vols:
        return 0
    return sum(vols) / len(vols)


def zone_width_ok(low, high, current_price=None, max_bps=45):
    if low is None or high is None or high <= low:
        return False

    if current_price:
        width_bps = ((high - low) / current_price) * 10000
        return width_bps <= max_bps

    return True


def label_zone(score):
    if score >= 85:
        return "A+ Zone"
    if score >= 75:
        return "A Zone"
    if score >= 60:
        return "B Zone"
    return "Weak Zone"


def build_zone(kind, low, high, reaction_score, volume_score, impulse_score, freshness_score, source, candle_time):
    low = round_price(low)
    high = round_price(high)

    if low is None or high is None or high <= low:
        return None

    mid = round_price((low + high) / 2)

    quality_score = int(
        reaction_score * 0.35 +
        volume_score * 0.25 +
        impulse_score * 0.25 +
        freshness_score * 0.15
    )

    if kind == "demand":
        trigger = mid
        invalidation = round_price(low - 0.05)
    else:
        trigger = mid
        invalidation = round_price(high + 0.05)

    return {
        "type": kind,
        "low": low,
        "high": high,
        "mid": mid,
        "trigger": trigger,
        "invalidation": invalidation,
        "reaction_score": int(reaction_score),
        "volume_score": int(volume_score),
        "impulse_score": int(impulse_score),
        "freshness_score": int(freshness_score),
        "quality_score": quality_score,
        "label": label_zone(quality_score),
        "source": source,
        "time": candle_time,
    }



def get_supply_demand_settings(timeframe):
    """
    Timeframe-specific supply/demand rules.

    1m = tighter precision zones.
    5m = broader structure zones.
    15m = major zones only.
    """
    if timeframe == "5Min":
        return {
            "lookback": 2,
            "max_zones": 2,
            "min_quality": 58,
            "max_bps": 75,
            "volume_length": 10,
            "future_bars": 2,
            "zone_width_factor": 0.55,
            "label_suffix": "5m Structure",
        }

    if timeframe == "15Min":
        return {
            "lookback": 2,
            "max_zones": 2,
            "min_quality": 45,
            "max_bps": 120,
            "volume_length": 8,
            "future_bars": 2,
            "zone_width_factor": 0.70,
            "label_suffix": "15m Major",
        }

    return {
        "lookback": 3,
        "max_zones": 2,
        "min_quality": 55,
        "max_bps": 45,
        "volume_length": 20,
        "future_bars": 4,
        "zone_width_factor": 0.45,
        "label_suffix": "1m Precision",
    }


def detect_supply_demand_zones(candles, current_price=None, timeframe="1Min"):
    """
    Standalone Alpaca-based supply/demand zone detector.

    Demand = swing low + volume/reaction + impulse away.
    Supply = swing high + volume/reaction + impulse away.

    This is still standalone chart logic, but it uses real candle data,
    volume, reaction, width filtering, and quality scoring.
    """
    settings = get_supply_demand_settings(timeframe)
    lookback = settings["lookback"]
    max_zones = settings["max_zones"]
    min_quality = settings["min_quality"]
    max_bps = settings["max_bps"]
    volume_length = settings["volume_length"]
    future_bars_count = settings["future_bars"]
    zone_width_factor = settings["zone_width_factor"]
    label_suffix = settings["label_suffix"]

    min_required = lookback * 2 + volume_length + future_bars_count
    if len(candles) < min_required:
        return {
            "demand": [],
            "supply": [],
            "meta": {
                "timeframe": timeframe,
                "reason": "not_enough_candles",
                "candles": len(candles),
                "min_required": min_required,
                "rule_set": label_suffix,
            }
        }

    demand = []
    supply = []

    total = len(candles)

    for i in range(lookback, len(candles) - lookback - future_bars_count):
        window = candles[i - lookback:i + lookback + 1]
        center = candles[i]

        candle_range = max(center["high"] - center["low"], 0.01)
        body_low = min(center["open"], center["close"])
        body_high = max(center["open"], center["close"])
        body_size = max(body_high - body_low, 0.01)

        future = candles[i + 1:i + 1 + future_bars_count]
        if not future:
            continue

        future_high = max(b["high"] for b in future)
        future_low = min(b["low"] for b in future)

        avg_vol = average_volume(candles, i, volume_length)
        candle_vol = center.get("volume") or 0
        vol_ratio = candle_vol / avg_vol if avg_vol > 0 else 1
        volume_score = min(100, max(0, vol_ratio * 45))

        freshness_score = min(100, max(15, (i / total) * 100))

        # Demand zone: swing low + bounce away.
        if center["low"] == min(c["low"] for c in window):
            bounce = future_high - center["low"]
            reaction_score = min(100, max(0, (bounce / candle_range) * 35))
            impulse_score = min(100, max(0, (bounce / body_size) * 25))

            # Use candle body to avoid giant wick zones.
            zone_low = center["low"]
            zone_high = min(body_high, center["low"] + max(body_size, candle_range * zone_width_factor))

            if zone_width_ok(zone_low, zone_high, current_price=current_price, max_bps=max_bps):
                zone = build_zone(
                    "demand",
                    zone_low,
                    zone_high,
                    reaction_score,
                    volume_score,
                    impulse_score,
                    freshness_score,
                    "swing_low_volume_bounce",
                    center["time"],
                )

                if zone and zone["quality_score"] >= min_quality:
                    zone["timeframe"] = timeframe
                    zone["rule_set"] = label_suffix
                    zone["label"] = f"{zone['label']} / {label_suffix}"
                    demand.append(zone)

        # Supply zone: swing high + rejection away.
        if center["high"] == max(c["high"] for c in window):
            rejection = center["high"] - future_low
            reaction_score = min(100, max(0, (rejection / candle_range) * 35))
            impulse_score = min(100, max(0, (rejection / body_size) * 25))

            # Use candle body to avoid giant wick zones.
            zone_high = center["high"]
            zone_low = max(body_low, center["high"] - max(body_size, candle_range * zone_width_factor))

            if zone_width_ok(zone_low, zone_high, current_price=current_price, max_bps=max_bps):
                zone = build_zone(
                    "supply",
                    zone_low,
                    zone_high,
                    reaction_score,
                    volume_score,
                    impulse_score,
                    freshness_score,
                    "swing_high_volume_rejection",
                    center["time"],
                )

                if zone and zone["quality_score"] >= min_quality:
                    zone["timeframe"] = timeframe
                    zone["rule_set"] = label_suffix
                    zone["label"] = f"{zone['label']} / {label_suffix}"
                    supply.append(zone)

    if current_price is not None:
        demand = [z for z in demand if z["high"] <= current_price]
        supply = [z for z in supply if z["low"] >= current_price]

        demand = sorted(
            demand,
            key=lambda z: (abs(current_price - z["high"]), -z["quality_score"])
        )[:max_zones]

        supply = sorted(
            supply,
            key=lambda z: (abs(z["low"] - current_price), -z["quality_score"])
        )[:max_zones]
    else:
        demand = sorted(demand, key=lambda z: -z["quality_score"])[:max_zones]
        supply = sorted(supply, key=lambda z: -z["quality_score"])[:max_zones]

    return {
        "demand": demand,
        "supply": supply,
        "meta": {
            "timeframe": timeframe,
            "rule_set": label_suffix,
            "lookback": lookback,
            "min_quality": min_quality,
            "max_bps": max_bps,
            "volume_length": volume_length,
            "future_bars": future_bars_count,
            "candles": len(candles),
        }
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



def build_liquidity_sweep_zones(current_price, levels=None, support_resistance=None, supply_demand=None):
    """
    Standalone chart-only liquidity sweep zones.

    Upside candidates:
    - PMH
    - PDH
    - nearest resistance
    - supply zone highs

    Downside candidates:
    - PML
    - PDL
    - nearest support
    - demand zone lows

    This is visual-only. Not a trade signal.
    """
    levels = levels or {}
    support_resistance = support_resistance or {"support": [], "resistance": []}
    supply_demand = supply_demand or {"demand": [], "supply": []}

    if current_price is None:
        return {
            "upside": [],
            "downside": [],
            "status": "NO_PRICE",
            "note": "No current price available.",
        }

    upside = []
    downside = []

    def add_candidate(side, price, source, kind="level", confidence="Watch"):
        if price is None:
            return

        price = round_price(price)

        candidate = {
            "side": side,
            "price": price,
            "low": round_price(price - 0.03),
            "high": round_price(price + 0.03),
            "source": source,
            "kind": kind,
            "confidence": confidence,
            "distance": round_price(abs(price - current_price)),
            "meaning": (
                "Fake breakout above this area then fail back below."
                if side == "upside"
                else "Fake breakdown below this area then reclaim."
            ),
            "context_only": True,
            "not_trade_signal": True,
        }

        if side == "upside":
            if price >= current_price:
                upside.append(candidate)
        else:
            if price <= current_price:
                downside.append(candidate)

    add_candidate("upside", levels.get("pmh"), "PMH", "premarket_high", "Premarket Watch")
    add_candidate("upside", levels.get("pdh"), "PDH", "previous_day_high", "Watch")
    add_candidate("downside", levels.get("pml"), "PML", "premarket_low", "Premarket Watch")
    add_candidate("downside", levels.get("pdl"), "PDL", "previous_day_low", "Watch")

    for idx, r in enumerate(support_resistance.get("resistance", []) or []):
        add_candidate("upside", r.get("price"), f"R{idx + 1}", "resistance", "Watch")

    for idx, s in enumerate(support_resistance.get("support", []) or []):
        add_candidate("downside", s.get("price"), f"S{idx + 1}", "support", "Watch")

    for idx, z in enumerate(supply_demand.get("supply", []) or []):
        add_candidate("upside", z.get("high"), f"Supply {idx + 1}", "supply_zone_high", z.get("label", "Watch"))

    for idx, z in enumerate(supply_demand.get("demand", []) or []):
        add_candidate("downside", z.get("low"), f"Demand {idx + 1}", "demand_zone_low", z.get("label", "Watch"))

    upside = sorted(upside, key=lambda x: x["distance"])[:2]
    downside = sorted(downside, key=lambda x: x["distance"])[:2]

    status = "SWEEP_WATCH" if upside or downside else "NO_ACTIVE_SWEEP"

    return {
        "upside": upside,
        "downside": downside,
        "status": status,
        "note": "Liquidity sweep areas are chart-only watch areas. Confirm manually. Not a buy/sell signal.",
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



def ranges_overlap(low1, high1, low2, high2):
    if low1 is None or high1 is None or low2 is None or high2 is None:
        return False
    return max(low1, low2) <= min(high1, high2)


def price_in_range(price, low, high):
    if price is None or low is None or high is None:
        return False
    return low <= price <= high


def zone_age_minutes(zone, candles):
    if not candles or not zone.get("time"):
        return None

    last_time = candles[-1]["time"]
    age_seconds = max(0, last_time - zone["time"])
    return int(age_seconds / 60)


def count_zone_touches(zone, candles):
    low = zone.get("low")
    high = zone.get("high")
    zone_time = zone.get("time")
    touches = 0

    for c in candles:
        if zone_time and c["time"] <= zone_time:
            continue

        if ranges_overlap(low, high, c["low"], c["high"]):
            touches += 1

    return touches


def zone_broken_through(zone, candles):
    low = zone.get("low")
    high = zone.get("high")
    kind = zone.get("type")
    zone_time = zone.get("time")

    for c in candles:
        if zone_time and c["time"] <= zone_time:
            continue

        if kind == "demand" and c["close"] < low:
            return True

        if kind == "supply" and c["close"] > high:
            return True

    return False


def zone_caused_follow_through(zone, candles, current_price=None):
    low = zone.get("low")
    high = zone.get("high")
    kind = zone.get("type")
    zone_time = zone.get("time")

    if low is None or high is None:
        return False

    width = max(high - low, 0.05)
    follow_threshold = max(width * 1.5, 0.25)

    touched = False
    best_move = 0

    for c in candles:
        if zone_time and c["time"] <= zone_time:
            continue

        if not touched and ranges_overlap(low, high, c["low"], c["high"]):
            touched = True

        if touched:
            if kind == "demand":
                best_move = max(best_move, c["high"] - high)
            elif kind == "supply":
                best_move = max(best_move, low - c["low"])

    return best_move >= follow_threshold


def zone_session_confidence(zone):
    zone_time = zone.get("time")
    if not zone_time:
        return "unknown"

    dt = datetime.fromtimestamp(zone_time, tz=timezone.utc).astimezone(ET)
    open_t = et_datetime(dt.date(), 9, 30)
    close_t = et_datetime(dt.date(), 16, 0)

    if dt < open_t:
        return "premarket_low_confidence"

    if open_t <= dt <= close_t:
        return "regular_session_confirmed"

    return "after_hours_low_confidence"


def htf_confirms_zone(zone, htf_zones):
    for htf in htf_zones:
        for htf_zone in (htf.get("demand", []) + htf.get("supply", [])):
            if zone.get("type") != htf_zone.get("type"):
                continue

            if ranges_overlap(zone.get("low"), zone.get("high"), htf_zone.get("low"), htf_zone.get("high")):
                return True

    return False


def enhance_supply_demand_zones(supply_demand, candles, current_price=None, timeframe="1Min", htf_zones=None):
    htf_zones = htf_zones or []
    result = {
        "demand": [],
        "supply": [],
        "meta": supply_demand.get("meta", {}),
    }

    for side in ["demand", "supply"]:
        for zone in supply_demand.get(side, []) or []:
            zone = dict(zone)

            touches = count_zone_touches(zone, candles)
            tested = touches > 0
            broken = zone_broken_through(zone, candles)
            follow_through = zone_caused_follow_through(zone, candles, current_price=current_price)
            age_min = zone_age_minutes(zone, candles)
            session_conf = zone_session_confidence(zone)
            htf_confirmed = htf_confirms_zone(zone, htf_zones)

            score = int(zone.get("quality_score", 0))

            if tested:
                score += 5
            if follow_through:
                score += 10
            if htf_confirmed:
                score += 10
            if session_conf == "regular_session_confirmed":
                score += 5
            if broken:
                score -= 25
            if touches >= 3:
                score -= 10

            score = max(0, min(100, score))

            zone.update({
                "touches": touches,
                "tested": tested,
                "untested": not tested,
                "broken_through": broken,
                "caused_follow_through": follow_through,
                "age_minutes": age_min,
                "session_confidence": session_conf,
                "higher_timeframe_confirmed": htf_confirmed,
                "display_score": score,
                "worth_showing": score >= 60 and not broken,
            })

            result[side].append(zone)

    result["demand"] = sorted(
        result["demand"],
        key=lambda z: (not z.get("worth_showing", False), -z.get("display_score", 0))
    )

    result["supply"] = sorted(
        result["supply"],
        key=lambda z: (not z.get("worth_showing", False), -z.get("display_score", 0))
    )

    return result


def build_level_clusters(current_price, levels=None, support_resistance=None, supply_demand=None, liquidity_sweeps=None):
    levels = levels or {}
    support_resistance = support_resistance or {"support": [], "resistance": []}
    supply_demand = supply_demand or {"demand": [], "supply": []}
    liquidity_sweeps = liquidity_sweeps or {"upside": [], "downside": []}

    raw = []

    def add(kind, low, high, label, source, score=50):
        if low is None or high is None:
            return

        low = round_price(low)
        high = round_price(high)

        if high < low:
            low, high = high, low

        raw.append({
            "kind": kind,
            "low": low,
            "high": high,
            "mid": round_price((low + high) / 2),
            "label": label,
            "source": source,
            "score": score,
        })

    # Static major levels.
    add("upside", levels.get("pmh"), levels.get("pmh"), "PMH", "premarket high", 65)
    add("upside", levels.get("pdh"), levels.get("pdh"), "PDH", "previous day high", 65)
    add("downside", levels.get("pml"), levels.get("pml"), "PML", "premarket low", 65)
    add("downside", levels.get("pdl"), levels.get("pdl"), "PDL", "previous day low", 65)

    for i, r in enumerate(support_resistance.get("resistance", []) or []):
        add("upside", r.get("price"), r.get("price"), f"R{i + 1}", "resistance", 60 + r.get("touches", 0) * 5)

    for i, s in enumerate(support_resistance.get("support", []) or []):
        add("downside", s.get("price"), s.get("price"), f"S{i + 1}", "support", 60 + s.get("touches", 0) * 5)

    for i, z in enumerate(supply_demand.get("supply", []) or []):
        add("upside", z.get("low"), z.get("high"), f"Supply {i + 1}", z.get("label", "supply"), z.get("display_score", z.get("quality_score", 50)))

    for i, z in enumerate(supply_demand.get("demand", []) or []):
        add("downside", z.get("low"), z.get("high"), f"Demand {i + 1}", z.get("label", "demand"), z.get("display_score", z.get("quality_score", 50)))

    for i, z in enumerate(liquidity_sweeps.get("upside", []) or []):
        add("upside", z.get("low"), z.get("high"), f"Upside Sweep {z.get('source', i + 1)}", "liquidity sweep", 70)

    for i, z in enumerate(liquidity_sweeps.get("downside", []) or []):
        add("downside", z.get("low"), z.get("high"), f"Downside Sweep {z.get('source', i + 1)}", "liquidity sweep", 70)

    clusters = []
    max_gap = 0.12

    for item in sorted(raw, key=lambda x: (x["kind"], x["low"])):
        matched = None

        for cluster in clusters:
            if cluster["kind"] != item["kind"]:
                continue

            if item["low"] <= cluster["high"] + max_gap and item["high"] >= cluster["low"] - max_gap:
                matched = cluster
                break

        if not matched:
            clusters.append({
                "kind": item["kind"],
                "low": item["low"],
                "high": item["high"],
                "mid": item["mid"],
                "sources": [item["label"]],
                "details": [item],
                "score": item["score"],
            })
        else:
            matched["low"] = round_price(min(matched["low"], item["low"]))
            matched["high"] = round_price(max(matched["high"], item["high"]))
            matched["mid"] = round_price((matched["low"] + matched["high"]) / 2)
            matched["sources"].append(item["label"])
            matched["details"].append(item)
            matched["score"] = max(matched["score"], item["score"])

    if current_price is not None:
        upside = [c for c in clusters if c["kind"] == "upside" and c["high"] >= current_price]
        downside = [c for c in clusters if c["kind"] == "downside" and c["low"] <= current_price]

        upside = sorted(upside, key=lambda c: (abs(c["low"] - current_price), -c["score"]))[:3]
        downside = sorted(downside, key=lambda c: (abs(current_price - c["high"]), -c["score"]))[:3]
        clusters = upside + downside

    for c in clusters:
        c["label"] = " / ".join(c["sources"][:4])
        c["source_count"] = len(c["sources"])
        c["cluster_type"] = "resistance/supply/sweep cluster" if c["kind"] == "upside" else "support/demand/sweep cluster"

    return {
        "clusters": clusters,
        "note": "Merged nearby support/resistance, supply/demand, and liquidity sweep areas to reduce chart noise.",
    }


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
        raw_supply_demand = detect_supply_demand_zones(indicators_source, current_price=current_price, timeframe=timeframe)

        htf_zones = []
        # Higher-timeframe confirmation for 1m/5m charts.
        for htf in ["5Min", "15Min"]:
            if htf == timeframe:
                continue

            try:
                htf_bars = fetch_bars(SYMBOL, today_start, today_end, timeframe=htf)
                htf_candles = normalize_candles(htf_bars)
                htf_regular = []
                for hc in htf_candles:
                    hdt = datetime.fromtimestamp(hc["time"], tz=timezone.utc)
                    if is_regular_dt(hdt):
                        htf_regular.append(hc)
                htf_source = htf_regular if htf_regular else htf_candles
                htf_zones.append(detect_supply_demand_zones(htf_source, current_price=current_price, timeframe=htf))
            except Exception:
                pass

        supply_demand = enhance_supply_demand_zones(
            raw_supply_demand,
            indicators_source,
            current_price=current_price,
            timeframe=timeframe,
            htf_zones=htf_zones,
        )

        liquidity_sweeps = build_liquidity_sweep_zones(
            current_price,
            levels=levels,
            support_resistance=support_resistance,
            supply_demand=supply_demand,
        )

        level_clusters = build_level_clusters(
            current_price,
            levels=levels,
            support_resistance=support_resistance,
            supply_demand=supply_demand,
            liquidity_sweeps=liquidity_sweeps,
        )

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
            "liquidity_sweeps": liquidity_sweeps,
            "level_clusters": level_clusters,
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
            "liquidity_sweeps": {"upside": [], "downside": [], "status": "ERROR"},
            "level_clusters": {"clusters": [], "note": "error"},
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
