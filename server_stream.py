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




def distance_bps(price_a, price_b):
    if price_a is None or price_b is None or price_b == 0:
        return None
    return abs(price_a - price_b) / price_b * 10000


def is_premarket_timestamp(ts_epoch):
    if not ts_epoch:
        return False
    dt = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).astimezone(ET)
    return et_datetime(dt.date(), 4, 0) <= dt < et_datetime(dt.date(), 9, 30)


def is_regular_timestamp(ts_epoch):
    if not ts_epoch:
        return False
    dt = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).astimezone(ET)
    return et_datetime(dt.date(), 9, 30) <= dt <= et_datetime(dt.date(), 16, 0)


def reaction_after_level(level_price, candles, side, start_time=None, bars_forward=8):
    """
    Measures whether price reacted after touching a level.
    side='support' expects bounce up.
    side='resistance' expects rejection down.
    """
    if level_price is None:
        return {"touched": False, "reaction_score": 0, "follow_through": False}

    tolerance = max(0.03, level_price * 0.00025)
    touched_index = None

    for i, c in enumerate(candles):
        if start_time and c["time"] <= start_time:
            continue

        if c["low"] - tolerance <= level_price <= c["high"] + tolerance:
            touched_index = i
            break

    if touched_index is None:
        return {"touched": False, "reaction_score": 0, "follow_through": False}

    future = candles[touched_index:min(len(candles), touched_index + bars_forward)]
    if not future:
        return {"touched": True, "reaction_score": 20, "follow_through": False}

    if side == "support":
        move = max(c["high"] for c in future) - level_price
    else:
        move = level_price - min(c["low"] for c in future)

    reaction_score = int(min(100, max(0, move / max(tolerance, 0.03) * 18)))
    follow_through = move >= max(0.20, tolerance * 3)

    return {
        "touched": True,
        "reaction_score": reaction_score,
        "follow_through": follow_through,
    }


def level_was_broken(level_price, candles, side, start_time=None):
    if level_price is None:
        return False

    tolerance = max(0.03, level_price * 0.0002)

    for c in candles:
        if start_time and c["time"] <= start_time:
            continue

        if side == "support" and c["close"] < level_price - tolerance:
            return True

        if side == "resistance" and c["close"] > level_price + tolerance:
            return True

    return False


def count_level_touches(level_price, candles, start_time=None):
    if level_price is None:
        return 0

    tolerance = max(0.03, level_price * 0.00025)
    touches = 0
    last_touch_index = -99

    for i, c in enumerate(candles):
        if start_time and c["time"] <= start_time:
            continue

        if c["low"] - tolerance <= level_price <= c["high"] + tolerance:
            # avoid counting consecutive candles as separate full touches
            if i - last_touch_index >= 3:
                touches += 1
                last_touch_index = i

    return touches


def score_support_resistance_level(level, candles, current_price=None, side="support"):
    price = level.get("price")
    touches = int(level.get("touches", 0) or 0)

    # Add real touch count from candles, not only clustered swing count.
    real_touches = count_level_touches(price, candles)
    touches = max(touches, real_touches)

    reaction = reaction_after_level(price, candles, side)
    broken = level_was_broken(price, candles, side)

    score = 0

    # Touch count
    if touches >= 3:
        score += 30
    elif touches == 2:
        score += 22
    elif touches == 1:
        score += 10

    # Reaction / follow-through
    score += min(25, int(reaction["reaction_score"] * 0.25))
    if reaction["follow_through"]:
        score += 15

    # Distance: closer useful levels matter more, but do not overreward.
    bps = distance_bps(price, current_price) if current_price else None
    if bps is not None:
        if bps <= 40:
            score += 15
        elif bps <= 90:
            score += 10
        elif bps <= 160:
            score += 5

    # Session confidence from the most recent touch
    score += 5

    if broken:
        score -= 35

    score = max(0, min(100, score))

    if score >= 80:
        label = "Strong"
    elif score >= 65:
        label = "Valid"
    elif score >= 50:
        label = "Watch"
    else:
        label = "Weak"

    enhanced = dict(level)
    enhanced.update({
        "price": round_price(price),
        "touches": touches,
        "reaction_score": reaction["reaction_score"],
        "follow_through": reaction["follow_through"],
        "broken": broken,
        "reliability_score": score,
        "reliability_label": label,
        "worth_showing": score >= 50 and not broken,
    })
    return enhanced


def filter_and_score_support_resistance(support_resistance, candles, current_price=None):
    result = {"support": [], "resistance": []}

    for s in support_resistance.get("support", []) or []:
        enhanced = score_support_resistance_level(s, candles, current_price=current_price, side="support")
        if enhanced["worth_showing"]:
            result["support"].append(enhanced)

    for r in support_resistance.get("resistance", []) or []:
        enhanced = score_support_resistance_level(r, candles, current_price=current_price, side="resistance")
        if enhanced["worth_showing"]:
            result["resistance"].append(enhanced)

    result["support"] = sorted(result["support"], key=lambda x: (-x["reliability_score"], abs((current_price or x["price"]) - x["price"])))[:3]
    result["resistance"] = sorted(result["resistance"], key=lambda x: (-x["reliability_score"], abs((current_price or x["price"]) - x["price"])))[:3]

    result["meta"] = {
        "rule": "2+ touches preferred; 1-touch levels need reaction/confluence; broken levels hidden",
        "min_score_to_show": 50,
    }

    return result


def reliability_grade(score):
    if score >= 85:
        return "A+"
    if score >= 75:
        return "A"
    if score >= 60:
        return "B"
    if score >= 45:
        return "Weak"
    return "Hidden"


def final_zone_reliability(zone):
    score = int(zone.get("display_score", zone.get("quality_score", 0)) or 0)

    # Stronger if higher timeframe confirmed.
    if zone.get("higher_timeframe_confirmed"):
        score += 8

    # Stronger if regular-session.
    if zone.get("session_confidence") == "regular_session_confirmed":
        score += 6

    # Stronger if it actually caused follow-through.
    if zone.get("caused_follow_through"):
        score += 8

    # Downgrade if already hit too many times.
    touches = int(zone.get("touches", 0) or 0)
    if touches >= 3:
        score -= 10

    # Downgrade broken zones hard.
    if zone.get("broken_through"):
        score -= 35

    # Premarket zones are lower confidence unless htf confirms.
    if zone.get("session_confidence") == "premarket_low_confidence" and not zone.get("higher_timeframe_confirmed"):
        score -= 8

    score = max(0, min(100, score))
    grade = reliability_grade(score)

    zone = dict(zone)
    zone.update({
        "reliability_score": score,
        "reliability_grade": grade,
        "worth_showing": score >= 60 and not zone.get("broken_through", False),
    })

    # Make label cleaner.
    base = zone.get("label", "")
    if "Weak Zone" in base and score >= 60:
        base = base.replace("Weak Zone", f"{grade} Zone")
    elif "B Zone" in base or "A Zone" in base or "A+ Zone" in base:
        parts = base.split("/")
        if parts:
            parts[0] = f" {grade} Zone "
            base = "/".join(parts).strip()
    else:
        base = f"{grade} Zone / {zone.get('rule_set', '')}"

    zone["label"] = base.strip()
    return zone


def filter_reliable_supply_demand(supply_demand):
    result = {
        "demand": [],
        "supply": [],
        "meta": dict(supply_demand.get("meta", {})),
    }

    for side in ["demand", "supply"]:
        for zone in supply_demand.get(side, []) or []:
            z = final_zone_reliability(zone)
            if z["worth_showing"]:
                result[side].append(z)

    result["demand"] = sorted(result["demand"], key=lambda z: -z.get("reliability_score", 0))[:2]
    result["supply"] = sorted(result["supply"], key=lambda z: -z.get("reliability_score", 0))[:2]

    result["meta"]["reliability_filter"] = "show B-or-better unbroken zones by default"
    return result


def filter_reliable_liquidity_sweeps(liquidity_sweeps, support_resistance=None, supply_demand=None):
    support_resistance = support_resistance or {"support": [], "resistance": []}
    supply_demand = supply_demand or {"demand": [], "supply": []}

    valid_sources = set()

    for r in support_resistance.get("resistance", []) or []:
        if r.get("reliability_score", 0) >= 50:
            valid_sources.add(f"R{len(valid_sources)+1}")

    for s in support_resistance.get("support", []) or []:
        if s.get("reliability_score", 0) >= 50:
            valid_sources.add(f"S{len(valid_sources)+1}")

    # Keep PMH/PML/PDH/PDL and supply/demand based sweeps if generated.
    allowed_kinds = {
        "premarket_high",
        "premarket_low",
        "previous_day_high",
        "previous_day_low",
        "resistance",
        "support",
        "supply_zone_high",
        "demand_zone_low",
    }

    result = {
        "upside": [],
        "downside": [],
        "status": liquidity_sweeps.get("status", "NO_ACTIVE_SWEEP"),
        "note": liquidity_sweeps.get("note", ""),
    }

    for side in ["upside", "downside"]:
        for z in liquidity_sweeps.get(side, []) or []:
            z = dict(z)
            kind = z.get("kind")
            confidence = z.get("confidence", "")

            score = 55

            if kind in ["previous_day_high", "previous_day_low"]:
                score += 15
            if kind in ["premarket_high", "premarket_low"]:
                score += 5
                z["confidence"] = "Premarket Watch / RTH confirmation required"
            if kind in ["resistance", "support"]:
                score += 10
            if kind in ["supply_zone_high", "demand_zone_low"]:
                score += 15

            if "Weak" in str(confidence):
                score -= 10

            score = max(0, min(100, score))
            z["reliability_score"] = score
            z["reliability_grade"] = reliability_grade(score)

            if kind in allowed_kinds and score >= 55:
                result[side].append(z)

    result["upside"] = sorted(result["upside"], key=lambda z: z.get("distance", 999))[:2]
    result["downside"] = sorted(result["downside"], key=lambda z: z.get("distance", 999))[:2]

    return result



def detect_structure_reaction_zones(candles, current_price=None, timeframe="1Min"):
    """
    Detect live reaction/watch zones from recent price action.
    These are not confirmed support/resistance yet. They are lighter watch zones
    based on recent wick rejections, body rejection, failed pushes, and repeated stalls.
    """
    if not candles or len(candles) < 12:
        return {"support_watch": [], "resistance_watch": [], "meta": {"reason": "not_enough_candles"}}

    settings = {
        # Reaction zones are intentionally short-term.
        # They show what price is reacting to right now, not old structure.
        # Approximate 30 minutes of candles per selected timeframe.
        "1Min": {"lookback": 30, "min_score": 48, "max_zones": 3, "zone_width": 0.06, "near_bps": 90, "window_minutes": 30},
        "5Min": {"lookback": 6, "min_score": 50, "max_zones": 3, "zone_width": 0.10, "near_bps": 140, "window_minutes": 30},
        "15Min": {"lookback": 2, "min_score": 44, "max_zones": 2, "zone_width": 0.18, "near_bps": 220, "window_minutes": 30},
    }.get(timeframe, {"lookback": 6, "min_score": 50, "max_zones": 3, "zone_width": 0.10, "near_bps": 140, "window_minutes": 30})

    recent = candles[-settings["lookback"]:]

    # Hard-filter by timestamp too, so reaction zones stay limited to the
    # most recent 30 minutes even if candle counts/timeframes change later.
    if candles and settings.get("window_minutes"):
        last_ts = candles[-1].get("time")
        if last_ts:
            cutoff_ts = last_ts - int(settings["window_minutes"] * 60)
            recent = [c for c in recent if c.get("time", 0) >= cutoff_ts]
    if len(recent) < 8:
        return {"support_watch": [], "resistance_watch": [], "meta": {"reason": "not_enough_recent_candles"}}

    volumes = [c.get("volume", 0) for c in recent]
    avg_volume = sum(volumes) / max(1, len(volumes))
    avg_range = sum(max(c["high"] - c["low"], 0.01) for c in recent) / max(1, len(recent))
    zone_width = max(settings["zone_width"], avg_range * 0.20)

    raw = []

    def add_candidate(kind, price, candle, reason, base_score):
        if price is None:
            return

        distance = abs(price - current_price) if current_price is not None else 0
        bps = distance_bps(price, current_price) if current_price else 0

        if bps and bps > settings["near_bps"]:
            base_score -= 12

        volume_score = 0
        if avg_volume > 0:
            rel_vol = candle.get("volume", 0) / avg_volume
            if rel_vol >= 1.8:
                volume_score = 12
            elif rel_vol >= 1.25:
                volume_score = 8
            elif rel_vol >= 1.0:
                volume_score = 4

        body = abs(candle["close"] - candle["open"])
        full_range = max(candle["high"] - candle["low"], 0.01)
        upper_wick = candle["high"] - max(candle["open"], candle["close"])
        lower_wick = min(candle["open"], candle["close"]) - candle["low"]

        wick_score = 0
        if kind == "support_watch" and lower_wick >= body * 1.2 and lower_wick >= full_range * 0.35:
            wick_score = 14
        if kind == "resistance_watch" and upper_wick >= body * 1.2 and upper_wick >= full_range * 0.35:
            wick_score = 14

        score = max(0, min(100, int(base_score + volume_score + wick_score)))

        raw.append({
            "type": kind,
            "price": round_price(price),
            "low": round_price(price - zone_width / 2),
            "high": round_price(price + zone_width / 2),
            "score": score,
            "reason": reason,
            "time": candle.get("time"),
            "volume_ratio": round(candle.get("volume", 0) / avg_volume, 2) if avg_volume else None,
            "distance": round_price(distance),
            "label": "Reaction Watch",
            "confirmed": False,
            "not_trade_signal": True,
        })

    for i in range(2, len(recent) - 2):
        c = recent[i]
        prev1 = recent[i - 1]
        next1 = recent[i + 1]
        next2 = recent[i + 2]

        full_range = max(c["high"] - c["low"], 0.01)
        upper_wick = c["high"] - max(c["open"], c["close"])
        lower_wick = min(c["open"], c["close"]) - c["low"]

        # Support watch: lower wick, then price reclaims/bounces shortly after.
        if lower_wick >= full_range * 0.35 and (
            next1["close"] > c["low"] + full_range * 0.45
            or next2["close"] > c["low"] + full_range * 0.45
        ):
            add_candidate("support_watch", c["low"], c, "lower wick rejection / possible support reaction", 38)

        # Resistance watch: upper wick, then price rejects/falls shortly after.
        if upper_wick >= full_range * 0.35 and (
            next1["close"] < c["high"] - full_range * 0.45
            or next2["close"] < c["high"] - full_range * 0.45
        ):
            add_candidate("resistance_watch", c["high"], c, "upper wick rejection / possible resistance reaction", 38)

        # Failed upside push / body rejection.
        if c["high"] > prev1["high"] and next1["close"] < c["close"]:
            add_candidate("resistance_watch", max(c["open"], c["close"]), c, "failed upside push / body rejection", 34)

        # Failed downside push / reclaim.
        if c["low"] < prev1["low"] and next1["close"] > c["close"]:
            add_candidate("support_watch", min(c["open"], c["close"]), c, "failed downside push / body reclaim", 34)

    def merge_candidates(items):
        merged = []

        for item in sorted(items, key=lambda x: x["price"]):
            match = None

            for m in merged:
                if item["low"] <= m["high"] + zone_width and item["high"] >= m["low"] - zone_width:
                    match = m
                    break

            if match is None:
                item = dict(item)
                item["touches"] = 1
                item["reasons"] = [item["reason"]]
                merged.append(item)
            else:
                match["low"] = round_price(min(match["low"], item["low"]))
                match["high"] = round_price(max(match["high"], item["high"]))
                match["price"] = round_price((match["low"] + match["high"]) / 2)
                match["score"] = min(100, max(match["score"], item["score"]) + 5)
                match["touches"] = match.get("touches", 1) + 1
                match.setdefault("reasons", []).append(item["reason"])
                match["reason"] = " / ".join(sorted(set(match["reasons"]))[:2])

        filtered = [m for m in merged if m["score"] >= settings["min_score"]]

        if current_price is not None:
            filtered = sorted(filtered, key=lambda x: (abs(x["price"] - current_price), -x["score"]))
        else:
            filtered = sorted(filtered, key=lambda x: -x["score"])

        return filtered[:settings["max_zones"]]

    support_watch = merge_candidates([r for r in raw if r["type"] == "support_watch"])
    resistance_watch = merge_candidates([r for r in raw if r["type"] == "resistance_watch"])

    return {
        "support_watch": support_watch,
        "resistance_watch": resistance_watch,
        "meta": {
            "rule": "Reaction zones use only the most recent 30 minutes; watch-only until confirmed by more touches or follow-through.",
            "min_score": settings["min_score"],
            "timeframe": timeframe,
            "window_minutes": settings.get("window_minutes", 30),
            "candles_used": len(recent),
        },
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








SETUP_LOG_DIR = "logs"
SETUP_LOG_PATH = os.path.join(SETUP_LOG_DIR, "confirmation_setups.jsonl")
SETUP_OUTCOME_PATH = os.path.join(SETUP_LOG_DIR, "setup_outcomes.jsonl")
_logged_setup_keys = set()


def ensure_setup_log_dir():
    os.makedirs(SETUP_LOG_DIR, exist_ok=True)


def setup_key(symbol, timeframe, setup):
    return "|".join([
        str(symbol),
        str(timeframe),
        str(setup.get("status")),
        str(setup.get("direction")),
        str(setup.get("source")),
        str(setup.get("level_price")),
        str(setup.get("candle_time")),
    ])


def append_jsonl(path, payload):
    ensure_setup_log_dir()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")


def log_confirmation_setups(symbol, timeframe, confirmation_setups, professional_context, current_price=None):
    """
    Logs read-only setup context once per unique setup.
    This is for review/backtesting. It does not place trades.
    """
    setups = confirmation_setups.get("setups", []) if confirmation_setups else []
    if not setups:
        return 0

    count = 0
    now_ts = datetime.now(timezone.utc).isoformat()

    for setup in setups:
        # Only log meaningful setup states, not empty/noise.
        if setup.get("status") not in {"WATCH", "CONFIRMED", "INVALIDATED"}:
            continue

        key = setup_key(symbol, timeframe, setup)
        if key in _logged_setup_keys:
            continue

        _logged_setup_keys.add(key)

        payload = {
            "logged_at": now_ts,
            "symbol": symbol,
            "timeframe": timeframe,
            "current_price": round_price(current_price),
            "setup": setup,
            "professional_context": {
                "professional_grade": professional_context.get("professional_grade"),
                "market_alignment": professional_context.get("market_alignment"),
                "no_trade": professional_context.get("no_trade"),
                "warnings": professional_context.get("warnings", []),
                "aapl_regime": professional_context.get("aapl", {}).get("regime", {}).get("regime"),
                "aapl_chop_score": professional_context.get("aapl", {}).get("regime", {}).get("chop_score"),
                "aapl_rvol": professional_context.get("aapl", {}).get("rvol"),
                "aapl_atr14": professional_context.get("aapl", {}).get("atr14"),
                "spy_trend": professional_context.get("spy", {}).get("trend", {}).get("label"),
                "qqq_trend": professional_context.get("qqq", {}).get("trend", {}).get("label"),
            },
            "read_only": True,
        }

        append_jsonl(SETUP_LOG_PATH, payload)
        count += 1

    return count


def setup_direction_move(setup, future_candles):
    direction = setup.get("direction")
    level_price = setup.get("level_price")

    if level_price is None or not future_candles:
        return {
            "max_favorable_move": None,
            "max_adverse_move": None,
        }

    if direction == "bullish":
        max_high = max(c["high"] for c in future_candles)
        min_low = min(c["low"] for c in future_candles)
        favorable = max_high - level_price
        adverse = level_price - min_low
    elif direction == "bearish":
        min_low = min(c["low"] for c in future_candles)
        max_high = max(c["high"] for c in future_candles)
        favorable = level_price - min_low
        adverse = max_high - level_price
    else:
        favorable = None
        adverse = None

    if favorable is not None:
        favorable = max(0, favorable)
    if adverse is not None:
        adverse = max(0, adverse)

    return {
        "max_favorable_move": round_price(favorable),
        "max_adverse_move": round_price(adverse),
    }


def evaluate_setup_outcomes(symbol, timeframe, candles, confirmation_setups):
    """
    Evaluates active setup context after 1, 3, 5, and 10 candles.
    Since this is intraday/live, each API refresh can append new outcome snapshots.
    """
    setups = confirmation_setups.get("setups", []) if confirmation_setups else []
    if not setups or not candles:
        return []

    outcomes = []
    by_time = {c.get("time"): idx for idx, c in enumerate(candles)}
    now_ts = datetime.now(timezone.utc).isoformat()

    for setup in setups:
        candle_time = setup.get("candle_time")
        if candle_time not in by_time:
            continue

        start_idx = by_time[candle_time]

        for horizon in [1, 3, 5, 10]:
            end_idx = start_idx + horizon
            if end_idx >= len(candles):
                continue

            future = candles[start_idx + 1:end_idx + 1]
            if not future:
                continue

            move = setup_direction_move(setup, future)
            invalidation = setup.get("invalidation")
            direction = setup.get("direction")

            invalidated = False
            if invalidation is not None:
                if direction == "bullish":
                    invalidated = any(c["close"] < invalidation for c in future)
                elif direction == "bearish":
                    invalidated = any(c["close"] > invalidation for c in future)

            outcome = {
                "evaluated_at": now_ts,
                "symbol": symbol,
                "timeframe": timeframe,
                "horizon_candles": horizon,
                "setup_key": setup_key(symbol, timeframe, setup),
                "setup_status": setup.get("status"),
                "professional_grade": setup.get("professional_grade"),
                "professional_score": setup.get("professional_score"),
                "direction": direction,
                "source": setup.get("source"),
                "level_price": setup.get("level_price"),
                "trigger": setup.get("trigger"),
                "invalidation": invalidation,
                "invalidated_within_horizon": invalidated,
                "max_favorable_move": move.get("max_favorable_move"),
                "max_adverse_move": move.get("max_adverse_move"),
                "last_future_close": round_price(future[-1]["close"]),
                "read_only": True,
            }

            append_jsonl(SETUP_OUTCOME_PATH, outcome)
            outcomes.append(outcome)

    return outcomes


def read_jsonl_tail(path, limit=200):
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()[-limit:]

    rows = []
    for line in lines:
        try:
            rows.append(json.loads(line))
        except Exception:
            continue

    return rows


def summarize_setup_performance(limit=500):
    outcomes = read_jsonl_tail(SETUP_OUTCOME_PATH, limit=limit)
    if not outcomes:
        return {
            "total_outcomes": 0,
            "summary": [],
            "note": "No outcome logs yet. Let the chart run during market hours.",
        }

    buckets = {}

    for row in outcomes:
        key = (
            row.get("timeframe"),
            row.get("horizon_candles"),
            row.get("direction"),
            row.get("professional_grade"),
            row.get("source"),
        )
        bucket = buckets.setdefault(key, {
            "timeframe": row.get("timeframe"),
            "horizon_candles": row.get("horizon_candles"),
            "direction": row.get("direction"),
            "professional_grade": row.get("professional_grade"),
            "source": row.get("source"),
            "count": 0,
            "invalidated": 0,
            "avg_favorable_move": 0.0,
            "avg_adverse_move": 0.0,
        })

        bucket["count"] += 1

        if row.get("invalidated_within_horizon"):
            bucket["invalidated"] += 1

        fav = row.get("max_favorable_move")
        adv = row.get("max_adverse_move")

        if fav is not None:
            bucket["avg_favorable_move"] += fav
        if adv is not None:
            bucket["avg_adverse_move"] += adv

    summary = []

    for bucket in buckets.values():
        count = max(1, bucket["count"])
        bucket["avg_favorable_move"] = round(bucket["avg_favorable_move"] / count, 3)
        bucket["avg_adverse_move"] = round(bucket["avg_adverse_move"] / count, 3)
        bucket["invalidation_rate"] = round(bucket["invalidated"] / count, 3)
        summary.append(bucket)

    summary = sorted(
        summary,
        key=lambda x: (
            x["horizon_candles"] or 0,
            x["invalidation_rate"],
            -x["avg_favorable_move"],
            x["avg_adverse_move"],
        )
    )

    return {
        "total_outcomes": len(outcomes),
        "summary": summary[:50],
        "read_only": True,
        "note": "Performance summary is based on logged chart context, not executed trades.",
    }


def calc_atr14(candles, period=14):
    values = []
    if not candles:
        return values

    prev_close = None
    true_ranges = []

    for c in candles:
        high = c["high"]
        low = c["low"]
        close = c["close"]

        if prev_close is None:
            tr = high - low
        else:
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )

        true_ranges.append(tr)
        prev_close = close

        if len(true_ranges) < period:
            atr = sum(true_ranges) / len(true_ranges)
        else:
            atr = sum(true_ranges[-period:]) / period

        values.append({
            "time": c["time"],
            "value": round(atr, 4),
        })

    return values


def calc_rvol(candles, length=20):
    if not candles:
        return None

    latest = candles[-1]
    if len(candles) < 2:
        return None

    start = max(0, len(candles) - 1 - length)
    sample = [c.get("volume") or 0 for c in candles[start:len(candles) - 1]]
    if not sample:
        return None

    avg = sum(sample) / len(sample)
    if avg <= 0:
        return None

    return round((latest.get("volume") or 0) / avg, 2)


def slope_from_series(series, bars=3):
    if not series or len(series) <= bars:
        return {
            "slope": 0,
            "label": "FLAT",
        }

    now = series[-1].get("value")
    then = series[-1 - bars].get("value")

    if now is None or then is None:
        return {
            "slope": 0,
            "label": "FLAT",
        }

    slope = now - then

    if slope > 0.03:
        label = "RISING"
    elif slope < -0.03:
        label = "FALLING"
    else:
        label = "FLAT"

    return {
        "slope": round(slope, 4),
        "label": label,
    }


def candle_body_ratio(candle):
    rng = max(candle["high"] - candle["low"], 0.01)
    body = abs(candle["close"] - candle["open"])
    return body / rng


def detect_chop_regime(candles, indicators, current_price=None):
    if not candles or len(candles) < 12:
        return {
            "regime": "UNKNOWN",
            "chop_score": 50,
            "trend_score": 0,
            "reason": "not_enough_candles",
        }

    recent = candles[-12:]
    closes = [c["close"] for c in recent]
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]

    recent_range = max(highs) - min(lows)
    avg_range = sum(max(c["high"] - c["low"], 0.01) for c in recent) / len(recent)
    avg_body_ratio = sum(candle_body_ratio(c) for c in recent) / len(recent)

    ema9 = indicators.get("ema9") or []
    ema20 = indicators.get("ema20") or []
    vwap = indicators.get("vwap") or []

    ema_cross_noise = 0
    paired = list(zip(ema9[-12:], ema20[-12:]))
    last_side = None

    for e9, e20 in paired:
        v9 = e9.get("value")
        v20 = e20.get("value")
        if v9 is None or v20 is None:
            continue

        side = "above" if v9 > v20 else "below" if v9 < v20 else "same"
        if last_side and side != last_side and side != "same":
            ema_cross_noise += 1
        if side != "same":
            last_side = side

    vwap_slope = slope_from_series(vwap, bars=3)

    overlap_count = 0
    for i in range(1, len(recent)):
        prev = recent[i - 1]
        cur = recent[i]
        if max(prev["low"], cur["low"]) <= min(prev["high"], cur["high"]):
            overlap_count += 1

    overlap_ratio = overlap_count / max(1, len(recent) - 1)

    chop_score = 0
    if avg_body_ratio < 0.45:
        chop_score += 20
    if overlap_ratio > 0.60:
        chop_score += 25
    if ema_cross_noise >= 2:
        chop_score += 25
    if vwap_slope["label"] == "FLAT":
        chop_score += 20
    if recent_range < avg_range * 3:
        chop_score += 10

    trend_score = 100 - min(100, chop_score)

    if chop_score >= 70:
        regime = "CHOP"
    elif chop_score >= 50:
        regime = "RANGE"
    else:
        regime = "TREND"

    return {
        "regime": regime,
        "chop_score": int(min(100, chop_score)),
        "trend_score": int(max(0, trend_score)),
        "vwap_slope": vwap_slope,
        "ema_cross_noise": ema_cross_noise,
        "overlap_ratio": round(overlap_ratio, 2),
        "avg_body_ratio": round(avg_body_ratio, 2),
        "recent_range": round_price(recent_range),
        "avg_range": round_price(avg_range),
        "reason": "CHOP if overlap/cross noise/flat VWAP dominate; TREND if structure is cleaner.",
    }


def fetch_context_symbol(symbol, start, end, timeframe):
    try:
        bars = fetch_bars(symbol, start, end, timeframe=timeframe)
        candles = normalize_candles(bars)
        regular = []
        for c in candles:
            dt_utc = datetime.fromtimestamp(c["time"], tz=timezone.utc)
            if is_regular_dt(dt_utc):
                regular.append(c)
        source = regular if regular else candles
        indicators = {
            "vwap": calc_vwap(source),
            "ema9": calc_ema(source, 9),
            "ema20": calc_ema(source, 20),
            "atr14": calc_atr14(source, 14),
        }
        current = source[-1]["close"] if source else None
        trend = confirmation_trend(current, indicators)
        regime = detect_chop_regime(source, indicators, current_price=current)
        return {
            "symbol": symbol,
            "current_price": round_price(current),
            "trend": trend,
            "regime": regime,
            "rvol": calc_rvol(source, 20),
            "data_status": "ok",
        }
    except Exception as e:
        return {
            "symbol": symbol,
            "current_price": None,
            "trend": {"label": "UNKNOWN"},
            "regime": {"regime": "UNKNOWN", "reason": str(e)},
            "rvol": None,
            "data_status": "error",
            "error": str(e),
        }


def build_professional_market_context(candles, indicators, current_price, timeframe, today_start, today_end):
    aapl_trend = confirmation_trend(current_price, indicators)
    aapl_regime = detect_chop_regime(candles, indicators, current_price=current_price)
    aapl_rvol = calc_rvol(candles, 20)
    atr14 = latest_indicator_value(indicators.get("atr14"))

    spy = fetch_context_symbol("SPY", today_start, today_end, timeframe)
    qqq = fetch_context_symbol("QQQ", today_start, today_end, timeframe)

    spy_bull = spy.get("trend", {}).get("bullish", False)
    qqq_bull = qqq.get("trend", {}).get("bullish", False)
    spy_bear = spy.get("trend", {}).get("bearish", False)
    qqq_bear = qqq.get("trend", {}).get("bearish", False)

    market_bullish = spy_bull and qqq_bull
    market_bearish = spy_bear and qqq_bear

    if market_bullish:
        market_alignment = "BULLISH"
    elif market_bearish:
        market_alignment = "BEARISH"
    elif spy_bull or qqq_bull or spy_bear or qqq_bear:
        market_alignment = "MIXED"
    else:
        market_alignment = "UNKNOWN"

    relative_strength = "UNKNOWN"
    if current_price is not None and candles and spy.get("current_price") and qqq.get("current_price"):
        aapl_open = candles[0]["open"]
        if aapl_open:
            aapl_change = (current_price - aapl_open) / aapl_open
        else:
            aapl_change = 0

        # Use trend agreement as a practical intraday RS proxy.
        if aapl_change > 0 and market_alignment in {"MIXED", "BULLISH"} and aapl_trend["bullish"]:
            relative_strength = "STRONG"
        elif aapl_change < 0 and market_alignment in {"MIXED", "BEARISH"} and aapl_trend["bearish"]:
            relative_strength = "WEAK"
        else:
            relative_strength = "NEUTRAL"

    no_trade = False
    warnings = []

    if aapl_regime["regime"] == "CHOP":
        no_trade = True
        warnings.append("AAPL chop regime detected.")

    if spy.get("regime", {}).get("regime") == "CHOP" and qqq.get("regime", {}).get("regime") == "CHOP":
        no_trade = True
        warnings.append("SPY and QQQ both choppy.")

    if market_alignment == "MIXED":
        warnings.append("SPY and QQQ are mixed.")

    if aapl_rvol is not None and aapl_rvol < 0.80:
        warnings.append("AAPL relative volume is low.")

    if atr14 is not None and atr14 < 0.15:
        warnings.append("AAPL ATR is low for active intraday movement.")

    if no_trade:
        professional_grade = "NO_TRADE"
    elif aapl_trend["bullish"] and market_bullish and aapl_regime["regime"] == "TREND" and (aapl_rvol or 0) >= 1:
        professional_grade = "A"
    elif aapl_trend["bearish"] and market_bearish and aapl_regime["regime"] == "TREND" and (aapl_rvol or 0) >= 1:
        professional_grade = "A"
    elif market_alignment in {"BULLISH", "BEARISH"} and aapl_regime["regime"] != "CHOP":
        professional_grade = "B"
    elif market_alignment == "MIXED":
        professional_grade = "C"
    else:
        professional_grade = "C"

    return {
        "timeframe": timeframe,
        "aapl": {
            "trend": aapl_trend,
            "regime": aapl_regime,
            "rvol": aapl_rvol,
            "atr14": round_price(atr14),
            "relative_strength": relative_strength,
        },
        "spy": spy,
        "qqq": qqq,
        "market_alignment": market_alignment,
        "professional_grade": professional_grade,
        "no_trade": no_trade,
        "warnings": warnings,
        "read_only": True,
        "note": "Professional context only. It does not place trades.",
    }


def grade_confirmation_setups_with_context(confirmation_setups, professional_context):
    confirmation_setups = dict(confirmation_setups or {})
    setups = []

    market_alignment = professional_context.get("market_alignment")
    regime = professional_context.get("aapl", {}).get("regime", {}).get("regime")
    no_trade = professional_context.get("no_trade", False)

    for setup in confirmation_setups.get("setups", []) or []:
        s = dict(setup)
        score = int(s.get("score", 0) or 0)

        direction = s.get("direction")
        aligned = (
            direction == "bullish" and market_alignment == "BULLISH"
        ) or (
            direction == "bearish" and market_alignment == "BEARISH"
        )

        if aligned:
            score += 15
        else:
            score -= 10

        if regime == "TREND":
            score += 10
        elif regime == "RANGE":
            score -= 5
        elif regime == "CHOP":
            score -= 25

        if no_trade:
            score -= 35

        score = max(0, min(100, score))

        if no_trade:
            grade = "NO_TRADE"
        elif score >= 90:
            grade = "A+"
        elif score >= 80:
            grade = "A"
        elif score >= 65:
            grade = "B"
        elif score >= 50:
            grade = "C"
        else:
            grade = "NO_TRADE"

        s["professional_score"] = score
        s["professional_grade"] = grade
        s["market_aligned"] = aligned
        s["market_alignment"] = market_alignment
        s["regime"] = regime
        s["no_trade_context"] = no_trade

        setups.append(s)

    confirmation_setups["setups"] = sorted(
        setups,
        key=lambda s: (
            0 if s.get("professional_grade") == "A+" else
            1 if s.get("professional_grade") == "A" else
            2 if s.get("professional_grade") == "B" else
            3 if s.get("professional_grade") == "C" else 4,
            -s.get("professional_score", 0),
        )
    )

    confirmation_setups["professional_context_applied"] = True
    return confirmation_setups


def latest_indicator_value(series):
    if not series:
        return None
    return series[-1].get("value")


def build_confirmation_level_candidates(levels=None, support_resistance=None, supply_demand=None):
    levels = levels or {}
    support_resistance = support_resistance or {"support": [], "resistance": []}
    supply_demand = supply_demand or {"demand": [], "supply": []}

    candidates = []

    def add(side, price, name, kind, low=None, high=None, confidence=None):
        if price is None:
            return
        candidates.append({
            "side": side,
            "price": round_price(price),
            "name": name,
            "kind": kind,
            "low": round_price(low if low is not None else price),
            "high": round_price(high if high is not None else price),
            "confidence": confidence or "watch",
        })

    add("upside", levels.get("pmh"), "PMH", "premarket_high")
    add("downside", levels.get("pml"), "PML", "premarket_low")
    add("upside", levels.get("pdh"), "PDH", "previous_day_high")
    add("downside", levels.get("pdl"), "PDL", "previous_day_low")

    for idx, r in enumerate(support_resistance.get("resistance", []) or []):
        add("upside", r.get("price"), f"R{idx + 1}", "resistance", confidence=r.get("reliability_label"))

    for idx, s in enumerate(support_resistance.get("support", []) or []):
        add("downside", s.get("price"), f"S{idx + 1}", "support", confidence=s.get("reliability_label"))

    for idx, z in enumerate(supply_demand.get("supply", []) or []):
        add("upside", z.get("high"), f"Supply {idx + 1}", "supply", low=z.get("low"), high=z.get("high"), confidence=z.get("label"))

    for idx, z in enumerate(supply_demand.get("demand", []) or []):
        add("downside", z.get("low"), f"Demand {idx + 1}", "demand", low=z.get("low"), high=z.get("high"), confidence=z.get("label"))

    return candidates


def confirmation_trend(current_price, indicators):
    vwap = latest_indicator_value(indicators.get("vwap"))
    ema9 = latest_indicator_value(indicators.get("ema9"))
    ema20 = latest_indicator_value(indicators.get("ema20"))

    bullish = (
        current_price is not None
        and vwap is not None
        and ema9 is not None
        and ema20 is not None
        and current_price > vwap
        and ema9 > ema20
    )
    bearish = (
        current_price is not None
        and vwap is not None
        and ema9 is not None
        and ema20 is not None
        and current_price < vwap
        and ema9 < ema20
    )

    if bullish:
        label = "BULLISH"
    elif bearish:
        label = "BEARISH"
    else:
        label = "MIXED"

    return {
        "label": label,
        "price": round_price(current_price),
        "vwap": round_price(vwap),
        "ema9": round_price(ema9),
        "ema20": round_price(ema20),
        "bullish": bullish,
        "bearish": bearish,
        "rules": "Bullish = price above VWAP and EMA9 above EMA20. Bearish = price below VWAP and EMA9 below EMA20.",
    }


def detect_confirmation_setups(candles, current_price=None, levels=None, support_resistance=None, supply_demand=None, indicators=None, lookback=8):
    """
    Read-only chart confirmation layer.

    WATCH:
      Price touched/swept a level, but all confirmation rules are not complete.

    CONFIRMED:
      Trend, reclaim/rejection, volume, and structure agree.

    INVALIDATED:
      Price closes through the level in the wrong direction.

    This is chart context only. It does not place trades.
    """
    indicators = indicators or {}
    candles = candles or []
    trend = confirmation_trend(current_price, indicators)
    candidates = build_confirmation_level_candidates(levels, support_resistance, supply_demand)

    if len(candles) < 3 or not candidates:
        return {
            "status": "NO_SETUP",
            "trend": trend,
            "setups": [],
            "meta": {
                "rule": "Need enough candles and at least one reference level.",
                "read_only": True,
            },
        }

    setups = []
    start = max(1, len(candles) - lookback)

    for i in range(start, len(candles)):
        candle = candles[i]
        prev = candles[i - 1] if i > 0 else None
        next_candle = candles[i + 1] if i + 1 < len(candles) else None

        for level in candidates:
            price = level.get("price")
            if price is None:
                continue

            tolerance = max(0.03, price * 0.00025)
            touched = candle["low"] - tolerance <= price <= candle["high"] + tolerance

            if not touched:
                continue

            wick_below = candle["low"] < price - tolerance
            wick_above = candle["high"] > price + tolerance
            bullish_reclaim = wick_below and candle["close"] > price
            bearish_rejection = wick_above and candle["close"] < price

            if bullish_reclaim:
                direction = "bullish"
                reclaim_confirmed = True
                interaction = "wick_below_close_back_above"
            elif bearish_rejection:
                direction = "bearish"
                reclaim_confirmed = True
                interaction = "wick_above_close_back_below"
            else:
                direction = "bullish" if level.get("side") == "downside" else "bearish"
                reclaim_confirmed = False
                interaction = "level_touched_waiting_for_reclaim_or_rejection"

            avg_vol = average_volume(candles, i, 20)
            candle_vol = candle.get("volume") or 0
            volume_confirmed = avg_vol > 0 and candle_vol > avg_vol
            volume_ratio = round(candle_vol / avg_vol, 2) if avg_vol > 0 else None

            higher_low = prev is not None and candle["low"] > prev["low"]
            lower_high = prev is not None and candle["high"] < prev["high"]
            breaks_trigger_high = next_candle is not None and (
                next_candle["high"] > candle["high"] or next_candle["close"] > candle["high"]
            )
            breaks_trigger_low = next_candle is not None and (
                next_candle["low"] < candle["low"] or next_candle["close"] < candle["low"]
            )

            if direction == "bullish":
                structure_confirmed = higher_low or breaks_trigger_high
                trend_confirmed = trend["bullish"]
                invalidated = candle["close"] < price - tolerance
                trigger = round_price(candle["high"])
                invalidation = round_price(price - tolerance)
            else:
                structure_confirmed = lower_high or breaks_trigger_low
                trend_confirmed = trend["bearish"]
                invalidated = candle["close"] > price + tolerance
                trigger = round_price(candle["low"])
                invalidation = round_price(price + tolerance)

            score = 0
            if reclaim_confirmed:
                score += 35
            if volume_confirmed:
                score += 20
            if structure_confirmed:
                score += 20
            if trend_confirmed:
                score += 20
            if level.get("name") in {"PMH", "PML", "PDH", "PDL"}:
                score += 5
            score = min(100, score)

            if invalidated:
                status = "INVALIDATED"
            elif reclaim_confirmed and volume_confirmed and structure_confirmed and trend_confirmed:
                status = "CONFIRMED"
            else:
                status = "WATCH"

            setups.append({
                "status": status,
                "direction": direction,
                "source": level.get("name"),
                "kind": level.get("kind"),
                "level_price": round_price(price),
                "level_low": level.get("low"),
                "level_high": level.get("high"),
                "interaction": interaction,
                "trigger": trigger,
                "invalidation": invalidation,
                "score": score,
                "volume_ratio": volume_ratio,
                "trend_confirmed": bool(trend_confirmed),
                "volume_confirmed": bool(volume_confirmed),
                "structure_confirmed": bool(structure_confirmed),
                "reclaim_confirmed": bool(reclaim_confirmed),
                "candle_time": candle.get("time"),
                "read_only": True,
                "note": "Chart context only. Not an order or automatic trade signal.",
            })

    priority = {"CONFIRMED": 0, "WATCH": 1, "INVALIDATED": 2}
    setups = sorted(
        setups,
        key=lambda s: (
            priority.get(s["status"], 9),
            -s.get("score", 0),
            abs((current_price or s["level_price"]) - s["level_price"]),
        )
    )[:6]

    if any(s["status"] == "CONFIRMED" for s in setups):
        overall = "CONFIRMED"
    elif any(s["status"] == "WATCH" for s in setups):
        overall = "WATCH"
    elif any(s["status"] == "INVALIDATED" for s in setups):
        overall = "INVALIDATED"
    else:
        overall = "NO_SETUP"

    return {
        "status": overall,
        "trend": trend,
        "setups": setups,
        "meta": {
            "read_only": True,
            "lookback_bars": lookback,
            "volume_rule": "Current candle volume greater than previous 20-candle average.",
            "trend_rule": "Price vs VWAP and EMA9 vs EMA20.",
            "confirmation_rule": "Reclaim/rejection + volume + structure + trend.",
            "labels": ["WATCH", "CONFIRMED", "INVALIDATED"],
        },
    }


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
        raw_support_resistance = detect_support_resistance(indicators_source, current_price=current_price)
        support_resistance = filter_and_score_support_resistance(
            raw_support_resistance,
            indicators_source,
            current_price=current_price,
        )

        structure_reactions = detect_structure_reaction_zones(
            indicators_source,
            current_price=current_price,
            timeframe=timeframe,
        )

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

        enhanced_supply_demand = enhance_supply_demand_zones(
            raw_supply_demand,
            indicators_source,
            current_price=current_price,
            timeframe=timeframe,
            htf_zones=htf_zones,
        )
        supply_demand = filter_reliable_supply_demand(enhanced_supply_demand)

        raw_liquidity_sweeps = build_liquidity_sweep_zones(
            current_price,
            levels=levels,
            support_resistance=support_resistance,
            supply_demand=supply_demand,
        )
        liquidity_sweeps = filter_reliable_liquidity_sweeps(
            raw_liquidity_sweeps,
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

        indicators = {
            "vwap": calc_vwap(indicators_source),
            "ema9": calc_ema(indicators_source, 9),
            "ema20": calc_ema(indicators_source, 20),
            "atr14": calc_atr14(indicators_source, 14),
        }

        confirmation_setups = detect_confirmation_setups(
            indicators_source,
            current_price=current_price,
            levels=levels,
            support_resistance=support_resistance,
            supply_demand=supply_demand,
            indicators=indicators,
        )

        professional_context = build_professional_market_context(
            indicators_source,
            indicators,
            current_price,
            timeframe,
            today_start,
            today_end,
        )

        confirmation_setups = grade_confirmation_setups_with_context(
            confirmation_setups,
            professional_context,
        )

        logged_setups = log_confirmation_setups(
            SYMBOL,
            timeframe,
            confirmation_setups,
            professional_context,
            current_price=current_price,
        )

        setup_outcomes = evaluate_setup_outcomes(
            SYMBOL,
            timeframe,
            indicators_source,
            confirmation_setups,
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
            "structure_reactions": structure_reactions,
            "supply_demand": supply_demand,
            "liquidity_sweeps": liquidity_sweeps,
            "level_clusters": level_clusters,
            "confirmation_setups": confirmation_setups,
            "professional_context": professional_context,
            "setup_logging": {
                "logged_setups": logged_setups,
                "outcomes_evaluated": len(setup_outcomes),
                "setup_log_path": SETUP_LOG_PATH,
                "outcome_log_path": SETUP_OUTCOME_PATH,
                "read_only": True,
            },
            "indicators": indicators,
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
            "structure_reactions": {"support_watch": [], "resistance_watch": [], "meta": {"reason": "error"}},
            "supply_demand": {"demand": [], "supply": []},
            "liquidity_sweeps": {"upside": [], "downside": [], "status": "ERROR"},
            "level_clusters": {"clusters": [], "note": "error"},
            "confirmation_setups": {"status": "ERROR", "trend": {}, "setups": [], "meta": {"read_only": True}},
            "professional_context": {"professional_grade": "ERROR", "warnings": ["chart error"], "read_only": True},
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




@APP.route("/api/debug/setup-performance")
def debug_setup_performance():
    limit = request.args.get("limit", "500")
    try:
        limit = int(limit)
    except Exception:
        limit = 500

    return jsonify(summarize_setup_performance(limit=limit))


@APP.route("/api/debug/recent-setups")
def debug_recent_setups():
    limit = request.args.get("limit", "50")
    try:
        limit = int(limit)
    except Exception:
        limit = 50

    return jsonify({
        "setups": read_jsonl_tail(SETUP_LOG_PATH, limit=limit),
        "outcomes": read_jsonl_tail(SETUP_OUTCOME_PATH, limit=limit),
        "read_only": True,
    })


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
