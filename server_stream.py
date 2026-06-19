import json
import math
import os
import queue
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
import websocket
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, send_from_directory, request

from app_config import AI_SNAPSHOT_CACHE_SECONDS, DEFAULT_OPENAI_MODEL, SYMBOL, SYMBOL_PATTERN, TIMEFRAMES
from indicators import calc_ema, calc_vwap, cluster_levels, round_price
from market_time import ET, build_market_session_status, et_datetime, previous_weekday, today_et

load_dotenv()
load_dotenv(os.path.expanduser("~/elite_scanner/.env"))

APP = Flask(__name__, static_folder="static")
ALPACA_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
TRADING_BASE_URL = os.getenv("ALPACA_TRADING_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://api.alpaca.markets"
FEED = os.getenv("ALPACA_STOCK_FEED", "sip").lower()


subscribers = {}
live_candles = {}
latest_trades = {}
latest_candle_audits = {}
recent_validated_candles = {}
candle_audit_lock = threading.Lock()
stream_lock = threading.Lock()
stream_ws = None
stream_authenticated = False
requested_stream_symbols = {SYMBOL}
subscribed_stream_symbols = set()
stream_status_by_symbol = {}
AI_PLAYBOOK_PATH = os.path.join(os.path.dirname(__file__), "docs", "ai_trading_playbook.md")
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
AI_REVIEW_SAFETY_TEXT = "Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase."
ENABLE_AI_AUTO_REVIEW = os.getenv("ENABLE_AI_AUTO_REVIEW", "false").lower() == "true"
_ai_snapshot_cache = {}
_ai_snapshot_lock = threading.Lock()
_latest_ai_review_lock = threading.Lock()
_ai_event_lock = threading.Lock()
_ai_event_state = {
    "fingerprint": None,
    "ai_review_recommended": False,
    "latest_event_reason": None,
    "latest_event_time": None,
}


def empty_ai_review():
    return {
        "decision": "WAIT",
        "bias": "neutral",
        "confidence": 0,
        "summary": f"No fresh AI review requested. {AI_REVIEW_SAFETY_TEXT}",
        "direct_answer": "No question has been asked yet.",
        "application_to_current_setup": "No current setup review is available.",
        "what_ai_sees": "No current review is available.",
        "professional_reasoning": "Wait for a current structured chart snapshot and review.",
        "entry_conditions": [],
        "trap_warnings": [],
        "options_risk_notes": [],
        "exit_plan": {
            "invalidation": None,
            "target_1": None,
            "target_2": None,
            "target_3": None,
        },
        "allow_entry_marker": False,
        "entry_marker": {
            "price": None,
            "label": "",
            "direction": "neutral",
        },
        "warnings": ["No fresh AI review requested."],
        "do_not_chase": AI_REVIEW_SAFETY_TEXT,
        "manual_confirmation_checklist": [
            "Confirm the setup manually.",
            "Confirm risk and invalidation before considering any trade.",
        ],
        "read_only": True,
        "not_financial_advice": True,
        "not_an_order": True,
        "source": "fallback",
        "ai_review_status": "No fresh AI review requested.",
        "snapshot_summary": {},
        "ai_review_recommended": False,
        "latest_event_reason": None,
        "latest_event_time": None,
        "ai_auto_review_enabled": ENABLE_AI_AUTO_REVIEW,
    }


LATEST_AI_REVIEW = empty_ai_review()


def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def get_headers():
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing Alpaca keys. Put them in ~/elite_scanner/.env or this folder's .env.")
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }


def normalize_symbol(value, default=SYMBOL):
    symbol = str(value or default).strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError("Invalid symbol. Use 1-10 letters, numbers, dots, or hyphens.")
    return symbol


def empty_stream_status(symbol=SYMBOL, error="waiting for live stream"):
    return {
        "symbol": normalize_symbol(symbol),
        "connected": False,
        "last_message": None,
        "error": error,
    }


def get_stream_status(symbol=SYMBOL):
    symbol = normalize_symbol(symbol)
    with stream_lock:
        status = stream_status_by_symbol.setdefault(symbol, empty_stream_status(symbol))
        return dict(status)


def set_stream_status(symbol=SYMBOL, **updates):
    symbol = normalize_symbol(symbol)
    with stream_lock:
        status = stream_status_by_symbol.setdefault(symbol, empty_stream_status(symbol))
        status.update(updates)
        status["symbol"] = symbol
        return dict(status)


def get_latest_trade(symbol=SYMBOL):
    symbol = normalize_symbol(symbol)
    with stream_lock:
        trade = latest_trades.get(symbol)
        return dict(trade) if isinstance(trade, dict) else None


def get_live_candles_snapshot(symbol=SYMBOL):
    symbol = normalize_symbol(symbol)
    with stream_lock:
        candles = live_candles.get(symbol) or {}
        return {tf: dict(candle) if isinstance(candle, dict) else None for tf, candle in candles.items()}


def reset_live_symbol_state(symbol=SYMBOL):
    symbol = normalize_symbol(symbol)
    with stream_lock:
        live_candles[symbol] = {tf: None for tf in TIMEFRAMES}
        latest_trades.pop(symbol, None)
        stream_status_by_symbol.setdefault(symbol, empty_stream_status(symbol))


def _subscriber_count_unlocked(symbol=None):
    if symbol:
        symbol = normalize_symbol(symbol)
        return {
            tf: len(subscribers.get((symbol, tf), []))
            for tf in TIMEFRAMES
        }
    return {
        f"{sym}:{tf}": len(queues)
        for (sym, tf), queues in subscribers.items()
    }


def ensure_stream_symbol_subscribed(symbol=SYMBOL):
    symbol = normalize_symbol(symbol)
    ws = None
    should_send = False
    with stream_lock:
        requested_stream_symbols.add(symbol)
        live_candles.setdefault(symbol, {tf: None for tf in TIMEFRAMES})
        stream_status_by_symbol.setdefault(symbol, empty_stream_status(symbol))
        ws = stream_ws
        should_send = bool(ws and stream_authenticated and symbol not in subscribed_stream_symbols)

    if not should_send:
        return False

    try:
        print(f"Starting stream for {symbol}")
        ws.send(json.dumps({
            "action": "subscribe",
            "trades": [symbol],
        }))
        with stream_lock:
            subscribed_stream_symbols.add(symbol)
            status = stream_status_by_symbol.setdefault(symbol, empty_stream_status(symbol))
            status.update({
                "connected": True,
                "error": None,
                "symbol": symbol,
            })
        print(f"Subscribed live trades for {symbol}: trades: ['{symbol}']")
        return True
    except Exception as error:
        set_stream_status(symbol, connected=False, error=str(error))
        print(f"ALPACA SUBSCRIBE ERROR for {symbol}:", error)
        return False


def get_related_market_symbols(symbol):
    symbol = normalize_symbol(symbol)
    groups = {
        "SPY": ("QQQ", "IWM", "DIA"),
        "QQQ": ("SPY", "IWM", "DIA"),
        "IWM": ("SPY", "QQQ", "DIA"),
        "TSLA": ("SPY", "QQQ", "XLY"),
    }
    sectors = {
        "XLK": {"AAPL", "MSFT", "META", "GOOGL", "GOOG", "AMZN"},
        "SMH": {"NVDA", "AMD", "AVGO", "MU"},
        "XLF": {"JPM", "BAC", "GS"},
        "XLE": {"XOM", "CVX"},
        "XLV": {"UNH", "JNJ", "PFE"},
    }
    related = list(groups.get(symbol, ("SPY", "QQQ", None)))
    if symbol not in groups:
        sector = next((etf for etf, members in sectors.items() if symbol in members), None)
        related[2] = sector
    related = [item for item in related if item and item != symbol]
    return {
        "primary_market": related[0] if related else None,
        "secondary_market": related[1] if len(related) > 1 else None,
        "sector_or_peer": related[2] if len(related) > 2 else None,
        "related_symbols": related,
    }


def fetch_bars(symbol, start, end, timeframe="1Min", limit=10000):
    if end <= start:
        return []

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
    return response.json().get("bars") or []


def fetch_alpaca_option_contracts(symbol, expiration=None):
    today = datetime.now(ET).date()
    params = {
        "underlying_symbols": symbol,
        "status": "active",
        "expiration_date_gte": expiration or today.isoformat(),
        "expiration_date_lte": expiration or (today + timedelta(days=14)).isoformat(),
        "limit": 1000,
    }
    bases = [TRADING_BASE_URL.rstrip("/")]
    alternate = "https://paper-api.alpaca.markets" if "paper-api." not in bases[0] else "https://api.alpaca.markets"
    if alternate not in bases:
        bases.append(alternate)

    last_error = None
    for base in bases:
        try:
            response = requests.get(
                f"{base}/v2/options/contracts",
                headers=get_headers(),
                params=params,
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
            contracts = payload.get("option_contracts") or payload.get("contracts") or []
            return contracts if isinstance(contracts, list) else []
        except requests.HTTPError as error:
            last_error = error
            if error.response is None or error.response.status_code not in {401, 403}:
                break
    if last_error:
        raise last_error
    return []


def fetch_alpaca_option_snapshots(option_symbols):
    symbols = [symbol for symbol in option_symbols or [] if symbol][:100]
    if not symbols:
        return {}
    response = requests.get(
        f"{DATA_BASE_URL.rstrip('/')}/v1beta1/options/snapshots",
        headers=get_headers(),
        params={"symbols": ",".join(symbols), "limit": len(symbols)},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    snapshots = payload.get("snapshots") or payload.get("option_snapshots") or {}
    return snapshots if isinstance(snapshots, dict) else {}


def is_regular_dt(dt_utc):
    t = dt_utc.astimezone(ET)
    market_open = et_datetime(t.date(), 9, 30)
    market_close = et_datetime(t.date(), 16, 0)
    return market_open <= t <= market_close


def detect_support_resistance(candles, current_price=None, lookback=3, max_levels=6):
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

                if zone and zone["quality_score"] >= max(30, min_quality - 25):
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

                if zone and zone["quality_score"] >= max(30, min_quality - 25):
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

def calc_levels(today_bars, prev_bars, session_day=None):
    day = session_day or today_et()
    premarket_start = et_datetime(day, 4, 0)
    market_open = et_datetime(day, 9, 30)
    opening_5m_end = et_datetime(day, 9, 35)

    pre_bars = []
    for b in today_bars:
        t = (
            datetime.fromtimestamp(b["time"], tz=timezone.utc).astimezone(ET)
            if b.get("time") is not None
            else datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
        )
        if premarket_start <= t < market_open:
            pre_bars.append(b)

    regular_prev = []
    prev_day = previous_weekday(day)
    prev_open = et_datetime(prev_day, 9, 30)
    prev_close = et_datetime(prev_day, 16, 0)

    for b in prev_bars:
        t = (
            datetime.fromtimestamp(b["time"], tz=timezone.utc).astimezone(ET)
            if b.get("time") is not None
            else datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
        )
        if prev_open <= t <= prev_close:
            regular_prev.append(b)

    regular_today = []
    opening_5m_bars = []
    for b in today_bars:
        t = (
            datetime.fromtimestamp(b["time"], tz=timezone.utc).astimezone(ET)
            if b.get("time") is not None
            else datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
        )
        if market_open <= t <= et_datetime(day, 16, 0):
            regular_today.append(b)
        if market_open <= t < opening_5m_end:
            opening_5m_bars.append(b)

    opening_5m_complete = len({b.get("time") for b in opening_5m_bars if b.get("time") is not None}) >= 5

    return {
        "pmh": max((b.get("high", b.get("h")) for b in pre_bars), default=None),
        "pml": min((b.get("low", b.get("l")) for b in pre_bars), default=None),
        "pdh": max((b.get("high", b.get("h")) for b in regular_prev), default=None),
        "pdl": min((b.get("low", b.get("l")) for b in regular_prev), default=None),
        "pdc": regular_prev[-1].get("close", regular_prev[-1].get("c")) if regular_prev else None,
        "hod": max((b.get("high", b.get("h")) for b in regular_today), default=None),
        "lod": min((b.get("low", b.get("l")) for b in regular_today), default=None),
        "opening_5m_high": max((b.get("high", b.get("h")) for b in opening_5m_bars), default=None) if opening_5m_complete else None,
        "opening_5m_low": min((b.get("low", b.get("l")) for b in opening_5m_bars), default=None) if opening_5m_complete else None,
        "opening_5m_complete": opening_5m_complete,
        "opening_5m_window": "09:30 ET <= candle time < 09:35 ET",
        "premarket_window": "04:00 ET <= candle time < 09:30 ET",
    }


def fvg_quality_grade(score):
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    return "WEAK"


def detect_fair_value_gaps(candles, symbol=SYMBOL, timeframe="1Min", levels=None, supply_demand=None, support_resistance=None, atr14=None):
    """
    Deterministic read-only fair value gap detector.
    Uses validated/rebuilt chart candles only.
    """
    candles = candles or []
    levels = levels or {}
    supply_demand = supply_demand or {"demand": [], "supply": []}
    support_resistance = support_resistance or {"support": [], "resistance": []}
    if len(candles) < 3:
        return {"bullish": [], "bearish": [], "all": [], "meta": {"read_only": True, "reason": "Need at least three candles."}}

    if atr14 is None:
        atr_values = calc_atr14(candles, 14)
        atr14 = latest_indicator_value(atr_values)
    if atr14 is None:
        ranges = [c.get("high", 0) - c.get("low", 0) for c in candles[-20:] if c.get("high") is not None and c.get("low") is not None]
        atr14 = _median(ranges) or 0.10

    tolerance = max(0.03, (atr14 or 0) * 0.20)

    def price_near(price, reference):
        return price is not None and reference is not None and abs(price - reference) <= tolerance

    def confluence(midpoint, bottom, top):
        labels = []
        for label, price in [
            ("PMH", levels.get("pmh")),
            ("PML", levels.get("pml")),
            ("PDH", levels.get("pdh")),
            ("PDL", levels.get("pdl")),
            ("HOD", levels.get("hod")),
            ("LOD", levels.get("lod")),
            ("OPEN 5M HIGH", levels.get("opening_5m_high")),
            ("OPEN 5M LOW", levels.get("opening_5m_low")),
        ]:
            if price_near(midpoint, price) or (price is not None and bottom - tolerance <= price <= top + tolerance):
                labels.append(label)
        for side in ["demand", "supply"]:
            for zone in supply_demand.get(side, []) or []:
                if ranges_overlap(bottom, top, zone.get("low"), zone.get("high")):
                    labels.append("SUPPLY/DEMAND")
                    break
        for side in ["support", "resistance"]:
            for level in support_resistance.get(side, []) or []:
                if level.get("quality_grade") in {"A", "B"} and price_near(midpoint, level.get("price")):
                    labels.append(f"{level.get('quality_grade')} {side.upper()}")
                    break
        return list(dict.fromkeys(labels))

    def fvg_price_interacting_now(gap_type, bottom, top):
        latest = candles[-1] if candles else {}
        latest_high = latest.get("high")
        latest_low = latest.get("low")
        latest_close = latest.get("close")
        if latest_high is None or latest_low is None:
            return False
        if ranges_overlap(bottom, top, latest_low, latest_high):
            return True
        if latest_close is not None and bottom - tolerance <= latest_close <= top + tolerance:
            return True
        edge = top if gap_type == "BULLISH_FVG" else bottom
        return price_near(latest_close, edge) if latest_close is not None else False

    def candle_body(candle):
        if candle.get("open") is None or candle.get("close") is None:
            return 0
        return abs(candle["close"] - candle["open"])

    def candle_body_bounds(candle):
        if candle.get("open") is None or candle.get("close") is None:
            return None, None
        return min(candle["open"], candle["close"]), max(candle["open"], candle["close"])

    def displacement_metrics(candle1, candle2, direction):
        c1_body = candle_body(candle1)
        c2_body = candle_body(candle2)
        c2_range = max(0.0001, candle2.get("high", 0) - candle2.get("low", 0))
        middle_body_percent = c2_body / c2_range * 100
        c1_body_low, c1_body_high = candle_body_bounds(candle1)
        c2_body_low, c2_body_high = candle_body_bounds(candle2)
        c2_close = candle2.get("close")
        c2_open = candle2.get("open")
        if direction == "bullish":
            direction_ok = c2_close is not None and c2_open is not None and c2_close > c2_open
            closed_beyond = c2_close is not None and candle1.get("high") is not None and c2_close > candle1["high"]
            full_body_engulf = (
                c1_body_low is not None and c2_body_low is not None
                and c2_body_low <= c1_body_low and c2_body_high >= c1_body_high
            )
        else:
            direction_ok = c2_close is not None and c2_open is not None and c2_close < c2_open
            closed_beyond = c2_close is not None and candle1.get("low") is not None and c2_close < candle1["low"]
            full_body_engulf = (
                c1_body_low is not None and c2_body_low is not None
                and c2_body_low <= c1_body_low and c2_body_high >= c1_body_high
            )
        body_overpowers = c2_body > max(0.0001, c1_body)
        body_ratio = c2_body / max(0.0001, c1_body)
        middle_engulfed = bool(closed_beyond and (full_body_engulf or body_ratio >= 1.25))
        range_vs_atr = c2_range / max(0.01, atr14 or 0.01)
        displacement_score = int(max(0, min(100,
            (20 if direction_ok else 0)
            + min(20, body_ratio / 1.25 * 20)
            + (25 if closed_beyond else 0)
            + min(20, middle_body_percent / 65 * 20)
            + min(15, range_vs_atr * 15)
        )))
        engulfing_score = 100 if full_body_engulf and closed_beyond else 75 if middle_engulfed else 40 if body_overpowers else 0
        is_chop_context = middle_body_percent < 35 or range_vs_atr < 0.45 or not closed_beyond
        return {
            "candle1_body": c1_body,
            "candle2_body": c2_body,
            "displacement_score": displacement_score,
            "engulfing_score": int(engulfing_score),
            "middle_candle_body_percent": round(middle_body_percent, 1),
            "middle_candle_engulfed_c1": middle_engulfed,
            "middle_candle_closed_beyond_c1": bool(closed_beyond),
            "middle_candle_direction_ok": bool(direction_ok),
            "middle_candle_body_overpowered_c1": bool(body_overpowers),
            "is_chop_context": bool(is_chop_context),
        }

    gaps = []
    for index in range(2, len(candles)):
        candle1 = candles[index - 2]
        candle2 = candles[index - 1]
        candle3 = candles[index]
        gap_type = None
        direction = None
        bottom = top = None
        c1_high = candle1.get("high")
        c1_low = candle1.get("low")
        c3_high = candle3.get("high")
        c3_low = candle3.get("low")
        bullish_rule_passed = c1_high is not None and c3_low is not None and c1_high < c3_low
        bearish_rule_passed = c1_low is not None and c3_high is not None and c1_low > c3_high
        if bullish_rule_passed:
            gap_type = "BULLISH_FVG"
            direction = "bullish"
            bottom = c1_high
            top = c3_low
        elif bearish_rule_passed:
            gap_type = "BEARISH_FVG"
            direction = "bearish"
            bottom = c3_high
            top = c1_low
        if not gap_type or top is None or bottom is None or top <= bottom:
            continue

        created_at = candle3.get("time")
        candle1_time = candle1.get("time")
        candle2_time = candle2.get("time")
        candle3_time = candle3.get("time")
        following = candles[index + 1:]
        height = max(0.0001, top - bottom)
        touch_count = 0
        last_touch_time = None
        fill_percentage = 0.0
        if gap_type == "BULLISH_FVG":
            deepest = top
            for candle in following:
                if candle.get("low") is None:
                    continue
                if candle["low"] <= top and candle.get("high", candle["low"]) >= bottom:
                    touch_count += 1
                    last_touch_time = candle.get("time")
                    deepest = min(deepest, candle["low"])
            fill_percentage = max(0, min(100, (top - deepest) / height * 100))
        else:
            highest = bottom
            for candle in following:
                if candle.get("high") is None:
                    continue
                if candle["high"] >= bottom and candle.get("low", candle["high"]) <= top:
                    touch_count += 1
                    last_touch_time = candle.get("time")
                    highest = max(highest, candle["high"])
            fill_percentage = max(0, min(100, (highest - bottom) / height * 100))

        if fill_percentage >= 99:
            status = "FILLED"
        elif fill_percentage > 0:
            status = "PARTIALLY_FILLED"
        else:
            status = "ACTIVE"

        age = len(candles) - index - 1
        midpoint = (top + bottom) / 2
        gap_vs_atr = height / max(0.01, atr14 or 0.01)
        metrics = displacement_metrics(candle1, candle2, direction)
        impulse_range = candle2.get("high", 0) - candle2.get("low", 0)
        impulse_score = min(100, int((impulse_range / max(0.01, atr14 or 0.01)) * 35))
        freshness_score = max(10, 100 - age * 4)
        fill_score = 100 if status == "ACTIVE" else 72 if status == "PARTIALLY_FILLED" else 18
        size_score = 25 if gap_vs_atr < 0.12 else 95 if gap_vs_atr <= 1.4 else 55
        confluence_labels = confluence(midpoint, bottom, top)
        has_key_confluence = bool(confluence_labels)
        price_interacting_now = fvg_price_interacting_now(gap_type, bottom, top)
        confluence_score = min(100, len(confluence_labels) * 28)
        quality_score = int(
            freshness_score * 0.12
            + fill_score * 0.18
            + metrics["displacement_score"] * 0.25
            + metrics["engulfing_score"] * 0.12
            + size_score * 0.15
            + confluence_score * 0.13
            + impulse_score * 0.05
        )
        if status == "FILLED":
            quality_score -= 35
        if touch_count >= 3:
            quality_score -= 10
        if gap_vs_atr < 0.08:
            quality_score = min(quality_score, 49)
        if not metrics["middle_candle_closed_beyond_c1"]:
            quality_score = min(quality_score, 49)
        if not metrics["middle_candle_body_overpowered_c1"]:
            quality_score = min(quality_score, 49)
        if metrics["displacement_score"] < 55:
            quality_score = min(quality_score, 49)
        elif metrics["displacement_score"] < 70 or not metrics["middle_candle_engulfed_c1"]:
            quality_score = min(quality_score, 64)
        if metrics["is_chop_context"]:
            quality_score -= 12
        quality_score = max(0, min(100, quality_score))
        grade = fvg_quality_grade(quality_score)
        warnings = []
        if status == "FILLED":
            warnings.append("FVG is filled; audit context only.")
        if touch_count >= 3:
            warnings.append("FVG has multiple touches.")
        if gap_vs_atr < 0.08:
            warnings.append("Tiny FVG relative to ATR; hidden in Clean Mode.")
        if not metrics["middle_candle_closed_beyond_c1"]:
            warnings.append("Middle candle did not close beyond candle 1.")
        if not metrics["middle_candle_body_overpowered_c1"]:
            warnings.append("Middle candle body did not overpower candle 1.")
        if metrics["displacement_score"] < 55:
            warnings.append("Weak middle-candle displacement.")
        if metrics["is_chop_context"]:
            warnings.append("FVG formed in chop-like candle context.")
        if grade == "WEAK":
            warnings.append("Weak FVG quality.")
        hidden_reasons = []
        if status == "FILLED":
            hidden_reasons.append("filled FVG")
        if status == "INVALID":
            hidden_reasons.append("invalid FVG")
        if grade == "WEAK":
            hidden_reasons.append("weak FVG quality")
        c_grade_exception = (
            grade == "C"
            and status in {"ACTIVE", "PARTIALLY_FILLED"}
            and price_interacting_now
            and has_key_confluence
            and metrics["displacement_score"] >= 70
            and metrics["middle_candle_closed_beyond_c1"]
            and metrics["middle_candle_body_overpowered_c1"]
            and not metrics["is_chop_context"]
        )
        if grade == "C" and not c_grade_exception:
            hidden_reasons.append("C-grade FVG hidden in Clean Mode")
        if grade == "C" and c_grade_exception:
            warnings.append("C-grade FVG shown only because price is interacting with confluence.")
        if gap_vs_atr < 0.08:
            hidden_reasons.append("tiny gap")
        if metrics["displacement_score"] < 70 or not metrics["middle_candle_engulfed_c1"]:
            hidden_reasons.append("insufficient displacement/engulfing")
        if metrics["is_chop_context"]:
            hidden_reasons.append("chop context")
        if status == "PARTIALLY_FILLED" and (fill_percentage >= 75 or touch_count >= 3):
            hidden_reasons.append("partially filled FVG no longer cleanly respected")
        visible_in_clean_mode = (
            status in {"ACTIVE", "PARTIALLY_FILLED"}
            and (grade in {"A", "B"} or c_grade_exception)
            and metrics["displacement_score"] >= 70
            and metrics["middle_candle_closed_beyond_c1"]
            and metrics["middle_candle_body_overpowered_c1"]
            and not metrics["is_chop_context"]
            and not hidden_reasons
        )

        gaps.append({
            "id": f"{symbol}-{timeframe}-{gap_type}-{created_at}-{round_price(bottom)}-{round_price(top)}",
            "symbol": symbol,
            "timeframe": timeframe,
            "direction": direction,
            "type": gap_type,
            "candle1_time": candle1_time,
            "candle2_time": candle2_time,
            "candle3_time": candle3_time,
            "candle1_open": round_price(candle1.get("open")),
            "candle1_high": round_price(c1_high),
            "candle1_low": round_price(c1_low),
            "candle1_close": round_price(candle1.get("close")),
            "candle2_open": round_price(candle2.get("open")),
            "candle2_high": round_price(candle2.get("high")),
            "candle2_low": round_price(candle2.get("low")),
            "candle2_close": round_price(candle2.get("close")),
            "candle3_open": round_price(candle3.get("open")),
            "candle3_high": round_price(c3_high),
            "candle3_low": round_price(c3_low),
            "candle3_close": round_price(candle3.get("close")),
            "top": round_price(top),
            "bottom": round_price(bottom),
            "midpoint": round_price(midpoint),
            "gap_size": round_price(height),
            "created_at": created_at,
            "last_touch_time": last_touch_time,
            "touch_count": touch_count,
            "fill_percentage": round(fill_percentage, 1),
            "status": status,
            "quality_score": quality_score,
            "quality_grade": grade,
            "confluence": confluence_labels,
            "confluence_count": len(confluence_labels),
            "has_key_confluence": has_key_confluence,
            "price_interacting_now": price_interacting_now,
            "impulse_score": impulse_score,
            "freshness_score": freshness_score,
            "size_score": size_score,
            "displacement_score": metrics["displacement_score"],
            "engulfing_score": metrics["engulfing_score"],
            "middle_candle_body_percent": metrics["middle_candle_body_percent"],
            "middle_candle_engulfed_c1": metrics["middle_candle_engulfed_c1"],
            "middle_candle_closed_beyond_c1": metrics["middle_candle_closed_beyond_c1"],
            "is_chop_context": metrics["is_chop_context"],
            "worth_showing": visible_in_clean_mode,
            "visible_in_clean_mode": visible_in_clean_mode,
            "clean_mode_hidden_reason": "; ".join(dict.fromkeys(hidden_reasons)) if hidden_reasons else None,
            "hidden_reason": "; ".join(dict.fromkeys(hidden_reasons)) if hidden_reasons else None,
            "selected_as_primary_clean_mode_fvg": False,
            "source": "fvg_engine",
            "reason": "strict_3_candle_imbalance_with_displacement",
            "rule_passed": True,
            "bullish_rule_passed": bool(bullish_rule_passed),
            "bearish_rule_passed": bool(bearish_rule_passed),
            "warnings": warnings,
            "read_only": True,
        })

    gaps = sorted(gaps, key=lambda gap: (
        0 if gap["status"] == "ACTIVE" else 1 if gap["status"] == "PARTIALLY_FILLED" else 2,
        -gap["quality_score"],
        -(gap.get("created_at") or 0),
    ))
    clean_representatives = []
    for gap in gaps:
        if not gap.get("visible_in_clean_mode"):
            continue
        duplicate = any(
            other.get("direction") == gap.get("direction") and (
                ranges_overlap(gap.get("bottom"), gap.get("top"), other.get("bottom"), other.get("top"))
                or abs((gap.get("midpoint") or 0) - (other.get("midpoint") or 0)) <=
                max(
                    tolerance,
                    abs((gap.get("top") or 0) - (gap.get("bottom") or 0)),
                    abs((other.get("top") or 0) - (other.get("bottom") or 0)),
                ) * 1.75
            )
            for other in clean_representatives
        )
        if duplicate:
            gap["visible_in_clean_mode"] = False
            gap["worth_showing"] = False
            gap["selected_as_primary_clean_mode_fvg"] = False
            reason = "duplicate lower-priority FVG"
            gap["hidden_reason"] = f"{gap.get('hidden_reason')}; {reason}" if gap.get("hidden_reason") else reason
            gap["clean_mode_hidden_reason"] = gap["hidden_reason"]
            gap["warnings"].append("Duplicate lower-priority FVG hidden in Clean Mode.")
        else:
            gap["selected_as_primary_clean_mode_fvg"] = True
            clean_representatives.append(gap)
    bullish = [gap for gap in gaps if gap["type"] == "BULLISH_FVG"][:8]
    bearish = [gap for gap in gaps if gap["type"] == "BEARISH_FVG"][:8]
    return {
        "bullish": bullish,
        "bearish": bearish,
        "all": bullish + bearish,
        "meta": {
            "rule": "Bullish FVG when candle1.high < candle3.low; bearish FVG when candle1.low > candle3.high.",
            "quality_rule": "Freshness, unfilled status, impulse, size vs ATR, and confluence.",
            "read_only": True,
        },
        "read_only": True,
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


def level_quality_grade(score):
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    return "WEAK"


def score_support_resistance_level(
    level,
    candles,
    current_price=None,
    side="support",
    levels=None,
    vwap=None,
    supply_demand=None,
    level_clusters=None,
    atr14=None,
):
    price = level.get("price")
    touches = int(level.get("touches", 0) or 0)
    levels = levels or {}
    supply_demand = supply_demand or {"demand": [], "supply": []}
    level_clusters = level_clusters or {"clusters": []}

    # Add real touch count from candles, not only clustered swing count.
    real_touches = count_level_touches(price, candles)
    touches = max(touches, real_touches)

    reaction = reaction_after_level(price, candles, side)
    broken = level_was_broken(price, candles, side)
    tolerance = max(0.03, (atr14 or 0) * 0.15, (price or 0) * 0.00025)

    touch_score = 0
    if touches == 1:
        touch_score = 10
    elif touches == 2:
        touch_score = 18
    elif 3 <= touches <= 5:
        touch_score = 25
    elif touches > 5:
        touch_score = max(8, 25 - (touches - 5) * 4)

    reaction_score = min(25, int(reaction["reaction_score"] * 0.25))
    if reaction["follow_through"]:
        reaction_score = min(25, reaction_score + 5)

    touch_indices = [
        idx for idx, candle in enumerate(candles)
        if candle["low"] - tolerance <= price <= candle["high"] + tolerance
    ]
    bars_since_touch = len(candles) - 1 - touch_indices[-1] if touch_indices else len(candles)
    if bars_since_touch <= 5:
        freshness_score = 15
    elif bars_since_touch <= 15:
        freshness_score = 11
    elif bars_since_touch <= 35:
        freshness_score = 6
    else:
        freshness_score = 2

    confluence_labels = []
    reference_levels = [
        ("PMH", levels.get("pmh")),
        ("PML", levels.get("pml")),
        ("PDH", levels.get("pdh")),
        ("PDL", levels.get("pdl")),
        ("PDC", levels.get("pdc")),
        ("VWAP", vwap),
    ]
    for label, reference_price in reference_levels:
        if reference_price is not None and abs(price - reference_price) <= tolerance * 2:
            confluence_labels.append(label)

    for zone_side in ["demand", "supply"]:
        for zone in supply_demand.get(zone_side, []) or []:
            if any(
                boundary is not None and abs(price - boundary) <= tolerance * 2
                for boundary in [zone.get("low"), zone.get("high")]
            ):
                confluence_labels.append(zone_side.title())
                break

    for cluster in level_clusters.get("clusters", []) or []:
        if cluster.get("low") is not None and cluster.get("high") is not None:
            if cluster["low"] - tolerance <= price <= cluster["high"] + tolerance:
                confluence_labels.append("Level cluster")
                break

    confluence_labels = list(dict.fromkeys(confluence_labels))
    confluence_score = min(20, len(confluence_labels) * 7)

    bps = distance_bps(price, current_price) if current_price else None
    distance_score = 0
    if bps is not None:
        if bps <= 40:
            distance_score = 10
        elif bps <= 90:
            distance_score = 7
        elif bps <= 160:
            distance_score = 4

    chopped_through = sum(
        1 for candle in candles
        if candle["low"] < price - tolerance and candle["high"] > price + tolerance
    )
    cleanliness_score = max(0, 15 - chopped_through * 3)
    if broken:
        cleanliness_score = 0

    score = touch_score + reaction_score + freshness_score + confluence_score + distance_score + cleanliness_score
    if broken:
        score -= 25
    score = max(0, min(100, int(score)))
    grade = level_quality_grade(score)

    reasons = [
        f"{touches} clean touch{'es' if touches != 1 else ''}",
        f"reaction {reaction['reaction_score']}/100",
        f"freshness {freshness_score}/15",
        f"cleanliness {cleanliness_score}/15",
    ]
    if confluence_labels:
        reasons.append(f"confluence: {', '.join(confluence_labels)}")
    if bps is not None:
        reasons.append(f"{round(bps)} bps from current price")
    if touches > 5:
        reasons.append("many touches reduce cleanliness")
    if broken:
        reasons.append("level was broken")

    enhanced = dict(level)
    enhanced.update({
        "price": round_price(price),
        "touches": touches,
        "touch_count": touches,
        "reaction_score": reaction["reaction_score"],
        "freshness_score": freshness_score,
        "confluence_score": confluence_score,
        "cleanliness_score": cleanliness_score,
        "quality_score": score,
        "quality_grade": grade,
        "quality_reasons": reasons,
        "follow_through": reaction["follow_through"],
        "broken": broken,
        "reliability_score": score,
        "reliability_label": grade,
        "worth_showing": not broken,
        "read_only": True,
    })
    return enhanced


def filter_and_score_support_resistance(
    support_resistance,
    candles,
    current_price=None,
    levels=None,
    vwap=None,
    supply_demand=None,
    level_clusters=None,
    atr14=None,
):
    result = {"support": [], "resistance": []}

    for s in support_resistance.get("support", []) or []:
        enhanced = score_support_resistance_level(
            s, candles, current_price=current_price, side="support", levels=levels, vwap=vwap,
            supply_demand=supply_demand, level_clusters=level_clusters, atr14=atr14,
        )
        if enhanced["worth_showing"]:
            result["support"].append(enhanced)

    for r in support_resistance.get("resistance", []) or []:
        enhanced = score_support_resistance_level(
            r, candles, current_price=current_price, side="resistance", levels=levels, vwap=vwap,
            supply_demand=supply_demand, level_clusters=level_clusters, atr14=atr14,
        )
        if enhanced["worth_showing"]:
            result["resistance"].append(enhanced)

    result["support"] = sorted(result["support"], key=lambda x: (-x["quality_score"], abs((current_price or x["price"]) - x["price"])))[:6]
    result["resistance"] = sorted(result["resistance"], key=lambda x: (-x["quality_score"], abs((current_price or x["price"]) - x["price"])))[:6]

    result["meta"] = {
        "rule": "Weighted level quality: touches, reaction, freshness, confluence, distance, and cleanliness",
        "quality_grades": {"A": "80-100", "B": "65-79", "C": "50-64", "WEAK": "below 50"},
        "broken_levels_hidden": True,
        "read_only": True,
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


def zone_quality_grade(score):
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    return "WEAK"


def final_zone_reliability(
    zone,
    levels=None,
    vwap=None,
    support_resistance=None,
    level_clusters=None,
    atr14=None,
):
    levels = levels or {}
    support_resistance = support_resistance or {"support": [], "resistance": []}
    level_clusters = level_clusters or {"clusters": []}
    low = zone.get("low")
    high = zone.get("high")
    width = max(0, (high or 0) - (low or 0))
    touches = int(zone.get("touches", 0) or 0)

    base_freshness = int(zone.get("freshness_score", 0) or 0)
    freshness_score = max(0, min(100, base_freshness - touches * 15))

    if touches == 0:
        retest_score = 100
    elif touches == 1:
        retest_score = 85
    elif touches == 2:
        retest_score = 65
    elif touches == 3:
        retest_score = 35
    else:
        retest_score = 10

    if atr14 and atr14 > 0:
        width_ratio = width / atr14
        if width_ratio <= 0.35:
            width_score = 100
        elif width_ratio <= 0.60:
            width_score = 80
        elif width_ratio <= 1.0:
            width_score = 55
        elif width_ratio <= 1.5:
            width_score = 30
        else:
            width_score = 10
    else:
        width_score = max(10, 100 - int(width * 250))

    tolerance = max(0.04, (atr14 or 0) * 0.20)
    confluence_labels = []

    def overlaps_price(price):
        return price is not None and low is not None and high is not None and low - tolerance <= price <= high + tolerance

    for label, price in [
        ("PMH", levels.get("pmh")),
        ("PML", levels.get("pml")),
        ("PDH", levels.get("pdh")),
        ("PDL", levels.get("pdl")),
        ("PDC", levels.get("pdc")),
        ("VWAP", vwap),
    ]:
        if overlaps_price(price):
            confluence_labels.append(label)

    for side in ["support", "resistance"]:
        for level in support_resistance.get(side, []) or []:
            if level.get("quality_grade") in {"A", "B"} and overlaps_price(level.get("price")):
                confluence_labels.append(f"{level.get('quality_grade')} {side}")
                break

    for cluster in level_clusters.get("clusters", []) or []:
        if ranges_overlap(low, high, cluster.get("low"), cluster.get("high")):
            confluence_labels.append("Level cluster")
            break

    confluence_labels = list(dict.fromkeys(confluence_labels))
    confluence_score = min(100, len(confluence_labels) * 35)

    impulse_score = int(zone.get("impulse_score", 0) or 0)
    reaction_score = int(zone.get("reaction_score", 0) or 0)
    volume_score = int(zone.get("volume_score", 0) or 0)

    score = int(
        freshness_score * 0.18 +
        impulse_score * 0.20 +
        reaction_score * 0.20 +
        retest_score * 0.15 +
        volume_score * 0.10 +
        width_score * 0.08 +
        confluence_score * 0.09
    )

    if zone.get("higher_timeframe_confirmed"):
        score += 6
    if zone.get("caused_follow_through"):
        score += 5
    if zone.get("broken_through"):
        score -= 40
    if zone.get("session_confidence") == "premarket_low_confidence" and not zone.get("higher_timeframe_confirmed"):
        score -= 6

    score = max(0, min(100, score))
    grade = zone_quality_grade(score)

    reasons = [
        f"freshness {freshness_score}/100",
        f"impulse {impulse_score}/100",
        f"reaction {reaction_score}/100",
        f"retests {touches}",
        f"volume {volume_score}/100",
        f"width {width_score}/100",
    ]
    if confluence_labels:
        reasons.append(f"confluence: {', '.join(confluence_labels)}")
    if touches >= 3:
        reasons.append("many retests weaken zone")
    if zone.get("higher_timeframe_confirmed"):
        reasons.append("higher timeframe confirmed")
    if zone.get("broken_through"):
        reasons.append("zone was broken")

    zone = dict(zone)
    zone.update({
        "freshness_score": freshness_score,
        "impulse_score": impulse_score,
        "reaction_score": reaction_score,
        "retest_score": retest_score,
        "volume_score": volume_score,
        "width_score": width_score,
        "confluence_score": confluence_score,
        "zone_quality_score": score,
        "zone_quality_grade": grade,
        "zone_quality_reasons": reasons,
        "reliability_score": score,
        "reliability_grade": grade,
        "display_score": score,
        "worth_showing": not zone.get("broken_through", False),
        "read_only": True,
    })

    # Make label cleaner.
    base = zone.get("label", "")
    if "Weak Zone" in base and score >= 50:
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


def filter_reliable_supply_demand(
    supply_demand,
    levels=None,
    vwap=None,
    support_resistance=None,
    level_clusters=None,
    atr14=None,
):
    result = {
        "demand": [],
        "supply": [],
        "meta": dict(supply_demand.get("meta", {})),
    }

    for side in ["demand", "supply"]:
        for zone in supply_demand.get(side, []) or []:
            z = final_zone_reliability(
                zone,
                levels=levels,
                vwap=vwap,
                support_resistance=support_resistance,
                level_clusters=level_clusters,
                atr14=atr14,
            )
            if z["worth_showing"] or z.get("reaction_status") == "FAILED":
                result[side].append(z)

    result["demand"] = sorted(result["demand"], key=lambda z: -z.get("zone_quality_score", 0))[:4]
    result["supply"] = sorted(result["supply"], key=lambda z: -z.get("zone_quality_score", 0))[:4]

    result["meta"]["zone_quality_rule"] = "Weighted freshness, impulse, reaction, retests, volume, width, and confluence"
    result["meta"]["zone_quality_grades"] = {"A": "80-100", "B": "65-79", "C": "50-64", "WEAK": "below 50"}
    result["meta"]["reaction_rule"] = "Read-only HOLD, RECLAIM, REJECTION, and FAILED context from zone touches and closes"
    result["meta"]["read_only"] = True
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


def _median(values):
    values = sorted(float(value) for value in values if isinstance(value, (int, float)) and math.isfinite(value))
    if not values:
        return None
    middle = len(values) // 2
    return values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2


def raw_candles_from_bars(bars, symbol, timeframe, provider="alpaca"):
    raw_candles = []
    for bar in bars or []:
        raw_candles.append({
            "raw_open": bar.get("o"),
            "raw_high": bar.get("h"),
            "raw_low": bar.get("l"),
            "raw_close": bar.get("c"),
            "raw_volume": bar.get("v"),
            "raw_timestamp": bar.get("t"),
            "data_provider": provider,
            "raw_symbol": symbol,
            "raw_timeframe": timeframe,
            "read_only": True,
        })
    return raw_candles


def _audited_candle(raw):
    timestamp = raw.get("raw_timestamp")
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).astimezone(timezone.utc)
        epoch = int(parsed.timestamp())
        et_time = parsed.astimezone(ET).strftime("%H:%M")
    except Exception:
        parsed = None
        epoch = None
        et_time = None
    raw_values = {
        "open": raw.get("raw_open"),
        "high": raw.get("raw_high"),
        "low": raw.get("raw_low"),
        "close": raw.get("raw_close"),
        "volume": raw.get("raw_volume"),
        "timestamp": timestamp,
    }
    return {
        "timestamp": timestamp,
        "time": epoch,
        "et_time": et_time,
        "open": raw.get("raw_open"),
        "high": raw.get("raw_high"),
        "low": raw.get("raw_low"),
        "close": raw.get("raw_close"),
        "volume": raw.get("raw_volume"),
        "validation_status": "VALID",
        "validation_reasons": [],
        "excluded_from_calculations": False,
        "excluded_from_display": False,
        "raw_values": raw_values,
        "corrected_values": None,
        "data_provider": raw.get("data_provider"),
        "raw_symbol": raw.get("raw_symbol"),
        "raw_timeframe": raw.get("raw_timeframe"),
        "read_only": True,
    }


def validate_raw_candles(raw_candles):
    audited = [_audited_candle(raw) for raw in raw_candles or []]
    seen = {}
    numeric_fields = ("open", "high", "low", "close")

    for candle in audited:
        reasons = candle["validation_reasons"]
        timestamp = candle.get("timestamp")
        if candle.get("time") is None:
            reasons.append("invalid or impossible timestamp")
        for field in numeric_fields:
            value = candle.get(field)
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                reasons.append(f"missing or non-finite {field}")
            elif value <= 0:
                reasons.append(f"zero or negative {field}")
        volume = candle.get("volume")
        if not isinstance(volume, (int, float)) or not math.isfinite(volume):
            reasons.append("volume missing or non-finite")
        elif volume < 0:
            reasons.append("negative volume")
        if not reasons:
            if candle["high"] < candle["low"]:
                reasons.append("high below low")
            if not candle["low"] <= candle["open"] <= candle["high"]:
                reasons.append("open outside high/low")
            if not candle["low"] <= candle["close"] <= candle["high"]:
                reasons.append("close outside high/low")
        values = tuple(candle.get(field) for field in (*numeric_fields, "volume"))
        if timestamp in seen and seen[timestamp]["values"] != values:
            reasons.append("duplicate timestamp with conflicting OHLCV")
            first = seen[timestamp]["candle"]
            if "duplicate timestamp with conflicting OHLCV" not in first["validation_reasons"]:
                first["validation_reasons"].append("duplicate timestamp with conflicting OHLCV")
            first["validation_status"] = "REJECTED"
        else:
            seen[timestamp] = {"values": values, "candle": candle}
        if reasons:
            candle["validation_status"] = "REJECTED"

    structurally_valid = [candle for candle in audited if candle["validation_status"] == "VALID"]
    for index, candle in enumerate(structurally_valid):
        prior = structurally_valid[max(0, index - 20):index]
        following = structurally_valid[index + 1:index + 3]
        if len(prior) < 8:
            continue
        median_range = _median([item["high"] - item["low"] for item in prior])
        median_volume = _median([item.get("volume") or 0 for item in prior])
        median_wick = _median([
            max(item["high"] - max(item["open"], item["close"]), min(item["open"], item["close"]) - item["low"])
            for item in prior
        ])
        if not median_range or median_range <= 0:
            continue
        candle_range = candle["high"] - candle["low"]
        wick = max(
            candle["high"] - max(candle["open"], candle["close"]),
            min(candle["open"], candle["close"]) - candle["low"],
        )
        previous = prior[-1]
        next_candle = following[0] if following else None
        close_move = abs(candle["close"] - previous["close"]) / previous["close"] if previous["close"] else 0
        snapback = (
            abs(next_candle["close"] - previous["close"]) / max(abs(candle["close"] - previous["close"]), 0.0001)
            if next_candle else 0
        )
        neighbor_high = max(item["high"] for item in prior[-3:] + following[:1])
        neighbor_low = min(item["low"] for item in prior[-3:] + following[:1])
        neighbor_deviation = max(
            max(0, candle["high"] - neighbor_high),
            max(0, neighbor_low - candle["low"]),
        ) / previous["close"]
        extreme_range = candle_range > median_range * 6
        low_confirmation_volume = median_volume is not None and (candle.get("volume") or 0) < median_volume * 2
        immediate_snapback = next_candle is not None and snapback >= 0.7
        confirmed_snapback = immediate_snapback and close_move >= 0.005
        extreme_wick = bool(median_wick and median_wick > 0 and wick > median_wick * 5)
        outside_neighbors = neighbor_deviation > 0.0075

        evidence = []
        if extreme_range:
            evidence.append(f"range {candle_range / median_range:.1f}x recent median")
        if low_confirmation_volume and extreme_range:
            evidence.append("extreme move lacks confirming volume")
        if confirmed_snapback:
            evidence.append("next candle immediately snaps back")
        if outside_neighbors:
            evidence.append("price extends far outside neighboring candles")
        if extreme_wick and immediate_snapback:
            evidence.append("extreme wick has no follow-through")

        if extreme_range and low_confirmation_volume and (confirmed_snapback or outside_neighbors):
            candle["validation_status"] = "REJECTED"
            candle["validation_reasons"].extend(evidence)
        elif len(evidence) >= 3:
            candle["validation_status"] = "SUSPICIOUS"
            candle["validation_reasons"].extend(evidence)

    for candle in audited:
        excluded = candle["validation_status"] == "REJECTED"
        candle["excluded_from_calculations"] = excluded
        candle["excluded_from_display"] = excluded
    validated = [candle for candle in audited if not candle["excluded_from_calculations"]]
    return audited, validated


def aggregate_validated_candles(validated_1min, timeframe, audited_1min=None):
    seconds = TIMEFRAMES.get(timeframe)
    if timeframe == "1Min" or not seconds:
        return list(validated_1min or [])
    buckets = {}
    available_counts = {}
    for candle in audited_1min or validated_1min or []:
        if candle.get("time") is not None:
            bucket = candle["time"] - candle["time"] % seconds
            available_counts[bucket] = available_counts.get(bucket, 0) + 1
    for candle in validated_1min or []:
        if candle.get("time") is None:
            continue
        buckets.setdefault(candle["time"] - candle["time"] % seconds, []).append(candle)
    rebuilt = []
    for bucket, source in sorted(buckets.items()):
        source = sorted(source, key=lambda item: item["time"])
        expected = max(1, available_counts.get(bucket, len(source)))
        coverage = len(source) / expected
        if coverage < 0.60:
            continue
        values = {
            "open": source[0]["open"],
            "high": max(item["high"] for item in source),
            "low": min(item["low"] for item in source),
            "close": source[-1]["close"],
            "volume": sum(item.get("volume") or 0 for item in source),
        }
        corrected = len(source) < expected or any(item.get("validation_status") == "CORRECTED" for item in source)
        rebuilt.append({
            "timestamp": datetime.fromtimestamp(bucket, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "time": bucket,
            "et_time": datetime.fromtimestamp(bucket, tz=timezone.utc).astimezone(ET).strftime("%H:%M"),
            **values,
            "validation_status": "CORRECTED" if corrected else "VALID",
            "validation_reasons": [f"rebuilt from {len(source)}/{expected} validated 1Min candles"] if corrected else ["rebuilt from validated 1Min candles"],
            "excluded_from_calculations": False,
            "excluded_from_display": False,
            "raw_values": None,
            "corrected_values": values,
            "source_candle_count": len(source),
            "expected_source_candle_count": expected,
            "source_coverage": round(coverage, 2),
            "data_provider": "rebuilt_from_validated_1min",
            "raw_symbol": source[0].get("raw_symbol"),
            "raw_timeframe": timeframe,
            "read_only": True,
        })
    return rebuilt


def compare_provider_aggregates(provider_bars, rebuilt, symbol, timeframe):
    raw = raw_candles_from_bars(provider_bars, symbol, timeframe)
    provider = {_audited_candle(item).get("time"): _audited_candle(item) for item in raw}
    comparisons = []
    for candle in rebuilt or []:
        other = provider.get(candle.get("time"))
        if not other:
            continue
        price_match = all(abs(float(candle[key]) - float(other[key])) <= 0.01 for key in ("open", "high", "low", "close"))
        provider_volume = other.get("volume") or 0
        volume_delta = abs((candle.get("volume") or 0) - provider_volume)
        volume_match = provider_volume == 0 or volume_delta / provider_volume <= 0.02
        if not price_match or not volume_match:
            comparisons.append({
                "time": candle.get("time"),
                "price_match": price_match,
                "volume_match": volume_match,
                "rebuilt": {key: candle.get(key) for key in ("open", "high", "low", "close", "volume")},
                "provider": {key: other.get(key) for key in ("open", "high", "low", "close", "volume")},
            })
    return comparisons


def build_candle_integrity_bundle(bars_1min, symbol, timeframe, provider_bars=None):
    raw_1min = raw_candles_from_bars(bars_1min, symbol, "1Min")
    raw_provider = raw_candles_from_bars(provider_bars or bars_1min, symbol, timeframe)
    audited, validated_1min = validate_raw_candles(raw_1min)
    candles = aggregate_validated_candles(validated_1min, timeframe, audited)
    comparisons = compare_provider_aggregates(provider_bars or [], candles, symbol, timeframe) if timeframe != "1Min" else []
    suspicious = [item for item in audited if item["validation_status"] == "SUSPICIOUS"]
    rejected = [item for item in audited if item["validation_status"] == "REJECTED"]
    corrected = [item for item in candles if item["validation_status"] == "CORRECTED"]
    warnings = []
    if suspicious:
        warnings.append(f"{len(suspicious)} suspicious candle(s) retained with warning.")
    if rejected:
        warnings.append(f"{len(rejected)} rejected candle(s) filtered.")
    if corrected:
        warnings.append(f"{len(corrected)} {timeframe} candle(s) rebuilt with incomplete validated-minute coverage.")
    if comparisons:
        warnings.append(f"{len(comparisons)} provider {timeframe} candle(s) differ from validated 1Min rebuild.")
    minimum = 8 if timeframe == "1Min" else 3
    status = "DEGRADED" if len(candles) < minimum else ("WARNING" if warnings else "CLEAN")
    return {
        "data_quality_status": status,
        "candle_accuracy_mode": "VALIDATED" if timeframe == "1Min" else "REBUILT_FROM_1MIN",
        "raw_candle_count": len(raw_provider),
        "validated_candle_count": len(candles),
        "suspicious_candle_count": len(suspicious),
        "rejected_candle_count": len(rejected),
        "corrected_candle_count": len(corrected),
        "candle_data_warnings": warnings,
        "bad_print_filter_enabled": True,
        "raw_candles": raw_provider,
        "raw_1min_candles": raw_1min,
        "audited_1min_candles": audited,
        "validated_1min_candles": validated_1min,
        "validated_candles": candles,
        "suspicious_candles": suspicious,
        "rejected_candles": rejected,
        "cross_timeframe_validation": comparisons,
        "read_only": True,
    }


def _comparison_ohlcv(candle):
    if not candle:
        return None
    if "raw_open" in candle:
        return {
            "open": candle.get("raw_open"),
            "high": candle.get("raw_high"),
            "low": candle.get("raw_low"),
            "close": candle.get("raw_close"),
            "volume": candle.get("raw_volume"),
        }
    return {key: candle.get(key) for key in ("open", "high", "low", "close", "volume")}


def _comparison_timestamp_et(epoch):
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(ET).isoformat()


def build_candle_comparison(audit, symbol, timeframe):
    """
    Build a read-only audit view. Raw/provider candles never feed calculations here.
    """
    audit = audit or {}
    seconds = TIMEFRAMES.get(timeframe, 300)
    raw_provider = [_audited_candle(item) for item in audit.get("raw_candles") or []]
    rebuilt = audit.get("validated_candles") or []
    displayed = audit.get("validated_candles") or []
    rejected = audit.get("rejected_candles") or []
    suspicious = audit.get("suspicious_candles") or []

    def bucket(candle):
        epoch = candle.get("time")
        return epoch if timeframe == "1Min" or epoch is None else epoch - epoch % seconds

    provider_by_bucket = {bucket(item): item for item in raw_provider if bucket(item) is not None}
    rebuilt_by_bucket = {bucket(item): item for item in rebuilt if bucket(item) is not None}
    displayed_by_bucket = {bucket(item): item for item in displayed if bucket(item) is not None}
    rejected_by_bucket = {}
    for item in rejected:
        rejected_by_bucket.setdefault(bucket(item), []).append(item)

    comparison_rows = []
    mismatch_rows = []
    for epoch in sorted(set(provider_by_bucket) | set(rebuilt_by_bucket) | set(rejected_by_bucket)):
        provider = provider_by_bucket.get(epoch)
        rebuilt_candle = rebuilt_by_bucket.get(epoch)
        displayed_candle = displayed_by_bucket.get(epoch)
        rejected_sources = rejected_by_bucket.get(epoch) or []
        reasons = []
        if rejected_sources:
            reasons.append(
                f"Raw Alpaca {timeframe} aggregate contains {len(rejected_sources)} rejected source 1Min candle(s); "
                "bad print filtered from rebuilt/displayed candle."
            )
        cross_mismatch = next(
            (item for item in audit.get("cross_timeframe_validation") or [] if item.get("time") == epoch),
            None,
        )
        if cross_mismatch and not rejected_sources:
            reasons.append("Raw Alpaca provider aggregate differs from the validated 1Min rebuild.")
        if provider and not rebuilt_candle:
            reasons.append("No rebuilt candle met the validated-minute coverage requirement.")
        if rebuilt_candle and not provider:
            reasons.append("No raw Alpaca provider aggregate was available for this bucket.")

        raw_values = _comparison_ohlcv(provider)
        rebuilt_values = _comparison_ohlcv(rebuilt_candle)
        displayed_values = _comparison_ohlcv(displayed_candle)
        values_differ = raw_values != rebuilt_values if raw_values is not None and rebuilt_values is not None else bool(reasons)
        if values_differ and not reasons:
            reasons.append("Raw Alpaca provider aggregate differs from the validated 1Min rebuild.")

        row = {
            "timestamp_et": _comparison_timestamp_et(epoch),
            "time": epoch,
            "raw_provider_ohlcv": raw_values,
            "rebuilt_ohlcv": rebuilt_values,
            "displayed_ohlcv": displayed_values,
            "validation_status": (
                "REJECTED_SOURCE_FILTERED"
                if rejected_sources
                else (rebuilt_candle or provider or {}).get("validation_status", "UNKNOWN")
            ),
            "used_for_display": displayed_candle is not None,
            "used_for_calculations": rebuilt_candle is not None,
            "rejected_source_candles": [
                {
                    "timestamp_et": _comparison_timestamp_et(item.get("time")),
                    "raw_ohlcv": _comparison_ohlcv(item),
                    "validation_status": item.get("validation_status"),
                    "validation_reasons": item.get("validation_reasons") or [],
                    "used_for_display": False,
                    "used_for_calculations": False,
                    "read_only": True,
                }
                for item in rejected_sources
            ],
            "mismatch_reason": " ".join(reasons) if reasons else None,
            "read_only": True,
        }
        comparison_rows.append(row)
        if row["mismatch_reason"]:
            mismatch_rows.append(row)

    warnings = list(audit.get("candle_data_warnings") or [])
    if rejected:
        warnings.append("Raw Alpaca bad print filtered; chart displays rebuilt validated candle.")
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "feed": FEED,
        "data_quality_status": audit.get("data_quality_status", "DEGRADED"),
        "candle_accuracy_mode": audit.get("candle_accuracy_mode", "RAW_PROVIDER"),
        "raw_alpaca_1min_bars": audit.get("raw_1min_candles") or [],
        "raw_alpaca_provider_bars": audit.get("raw_candles") or [],
        "validated_1min_candles": audit.get("validated_1min_candles") or [],
        "rebuilt_timeframe_candles": rebuilt,
        "displayed_candles": displayed,
        "rejected_candles": rejected,
        "suspicious_candles": suspicious,
        "comparison_warnings": list(dict.fromkeys(warnings)),
        "mismatches": mismatch_rows,
        "comparisons": comparison_rows,
        "read_only": True,
    }


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


def live_candle_is_suspicious(candle, symbol=SYMBOL):
    with candle_audit_lock:
        context = list(recent_validated_candles.get(symbol) or [])
    if len(context) < 8:
        return False, []
    median_range = _median([item["high"] - item["low"] for item in context[-20:]])
    median_volume = _median([item.get("volume") or 0 for item in context[-20:]])
    if not median_range or median_range <= 0:
        return False, []
    candle_range = candle["high"] - candle["low"]
    extreme_range = candle_range > median_range * 6
    low_confirmation_volume = median_volume is not None and (candle.get("volume") or 0) < median_volume * 2
    reasons = []
    if extreme_range:
        reasons.append(f"live range {candle_range / median_range:.1f}x recent validated median")
    if extreme_range and low_confirmation_volume:
        reasons.append("live extreme move lacks confirming volume")
    return bool(extreme_range and low_confirmation_volume), reasons


def update_live_candles(symbol, price, size, trade_time_utc):
    symbol = normalize_symbol(symbol)

    latest_trade = {
        "symbol": symbol,
        "price": price,
        "size": size,
        "timestamp": trade_time_utc.isoformat().replace("+00:00", "Z"),
    }

    suppress_live_events = False
    suppression_reasons = []
    for tf, seconds in TIMEFRAMES.items():
        bucket = bucket_time(trade_time_utc, seconds)
        with stream_lock:
            symbol_live_candles = live_candles.setdefault(symbol, {frame: None for frame in TIMEFRAMES})
            candle = symbol_live_candles.get(tf)

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

        with stream_lock:
            latest_trades[symbol] = dict(latest_trade)
            live_candles.setdefault(symbol, {frame: None for frame in TIMEFRAMES})[tf] = dict(candle)
        if tf == "1Min":
            suppress_live_events, suppression_reasons = live_candle_is_suspicious(candle, symbol)
            if suppression_reasons:
                set_stream_status(
                    symbol,
                    connected=True,
                    error=None,
                    last_message=datetime.now(timezone.utc).isoformat(),
                )

        event = {
            "type": "data_quality_warning" if suppress_live_events else "live_candle",
            "symbol": symbol,
            "timeframe": tf,
            "candle": None if suppress_live_events else candle,
            "latest_trade": latest_trade,
            "stream_status": get_stream_status(symbol),
            "data_quality_status": "WARNING" if suppress_live_events else "CLEAN",
            "candle_data_warnings": suppression_reasons,
            "bad_print_filter_enabled": True,
        }

        with stream_lock:
            queues = list(subscribers.get((symbol, tf), []))
        for q in queues:
            try:
                q.put_nowait(event)
            except Exception:
                pass


def on_open(ws):
    global stream_ws, stream_authenticated
    print("ALPACA STREAM OPENED")
    with stream_lock:
        stream_ws = ws
        stream_authenticated = True
        subscribed_stream_symbols.clear()
        symbols = sorted(requested_stream_symbols or {SYMBOL})
        for symbol in symbols:
            status = stream_status_by_symbol.setdefault(symbol, empty_stream_status(symbol))
            status.update({"connected": True, "error": None, "symbol": symbol})

    ws.send(json.dumps({
        "action": "auth",
        "key": ALPACA_KEY,
        "secret": ALPACA_SECRET,
    }))

    for symbol in symbols:
        ensure_stream_symbol_subscribed(symbol)


def on_message(ws, message):
    message_time = datetime.now(timezone.utc).isoformat()

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

        try:
            symbol = normalize_symbol(item.get("S"))
        except ValueError:
            continue

        with stream_lock:
            is_requested = symbol in requested_stream_symbols
        if not is_requested:
            continue

        set_stream_status(symbol, connected=True, error=None, last_message=message_time)

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

        update_live_candles(symbol, float(price), int(size or 0), trade_time)


def on_error(ws, error):
    print("ALPACA STREAM ERROR:", error)
    with stream_lock:
        symbols = set(requested_stream_symbols or {SYMBOL})
    for symbol in symbols:
        set_stream_status(symbol, connected=False, error=str(error))


def on_close(ws, close_status_code, close_msg):
    global stream_ws, stream_authenticated
    with stream_lock:
        stream_ws = None
        stream_authenticated = False
        subscribed_stream_symbols.clear()
        symbols = set(requested_stream_symbols or {SYMBOL})
    for symbol in symbols:
        set_stream_status(symbol, connected=False, error=f"closed: {close_status_code} {close_msg}")


def stream_worker():
    while True:
        try:
            if not ALPACA_KEY or not ALPACA_SECRET:
                with stream_lock:
                    symbols = set(requested_stream_symbols or {SYMBOL})
                for symbol in symbols:
                    set_stream_status(symbol, connected=False, error="missing Alpaca keys")
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
            with stream_lock:
                symbols = set(requested_stream_symbols or {SYMBOL})
            for symbol in symbols:
                set_stream_status(symbol, connected=False, error=str(e))

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


def evaluate_zone_reaction(zone, candles):
    zone = zone or {}
    candles = candles or []
    kind = zone.get("type")
    low = zone.get("low")
    high = zone.get("high")
    zone_time = zone.get("time")
    defended_edge = high if kind == "demand" else low if kind == "supply" else None
    failure_edge = low if kind == "demand" else high if kind == "supply" else None
    base = {
        "reaction_status": "NONE",
        "reaction_label": None,
        "defended_edge": round_price(defended_edge),
        "failure_edge": round_price(failure_edge),
        "closes_holding_edge": 0,
        "touches_count": 0,
        "last_reaction_time": None,
        "reaction_confidence": 0,
        "reaction_warnings": [],
        "read_only": True,
    }
    if kind not in {"demand", "supply"} or low is None or high is None:
        base["reaction_warnings"] = ["Zone edges are unavailable."]
        return base

    relevant = [c for c in candles if not zone_time or c.get("time", 0) > zone_time]
    if not relevant:
        return base

    touches = [
        (index, candle) for index, candle in enumerate(relevant)
        if ranges_overlap(low, high, candle.get("low"), candle.get("high"))
    ]
    base["touches_count"] = len(touches)

    failed = next((
        candle for candle in reversed(relevant)
        if (kind == "demand" and candle.get("close") < low)
        or (kind == "supply" and candle.get("close") > high)
    ), None)
    if failed:
        base.update({
            "reaction_status": "FAILED",
            "reaction_label": f"{kind.upper()} FAILED",
            "last_reaction_time": failed.get("time"),
            "reaction_confidence": 100,
            "reaction_warnings": [f"{kind.title()} failed after a close beyond the failure edge."],
        })
        return base

    if not touches:
        base["reaction_status"] = "ACTIVE"
        return base

    last_touch_index, last_touch = touches[-1]
    if kind == "demand":
        stronger_reaction = next((
            candle for index, candle in reversed(list(enumerate(relevant)))
            if candle.get("close") > high and (
                candle.get("low") < low
                or (index > 0 and relevant[index - 1].get("close") <= high)
            )
        ), None)
        reaction_candle = stronger_reaction or next((
            candle for candle in reversed(relevant)
            if candle.get("low") <= high and candle.get("close") > high
        ), None)
        reaction_index = relevant.index(reaction_candle) if reaction_candle else last_touch_index
        holding_candles = relevant[reaction_index:]
        closes_holding = sum(1 for candle in holding_candles if candle.get("close") >= high)
        edge_holding = bool(holding_candles) and all(candle.get("close") >= high for candle in holding_candles)
        stronger = bool(stronger_reaction)
        status = "RECLAIM" if stronger else "HOLD"
        label = "DEMAND RECLAIM" if stronger else "DEMAND HOLD"
    else:
        stronger_reaction = next((
            candle for index, candle in reversed(list(enumerate(relevant)))
            if candle.get("close") < low and (
                candle.get("high") > high
                or (index > 0 and relevant[index - 1].get("close") >= low)
            )
        ), None)
        reaction_candle = stronger_reaction or next((
            candle for candle in reversed(relevant)
            if candle.get("high") >= low and candle.get("close") < low
        ), None)
        reaction_index = relevant.index(reaction_candle) if reaction_candle else last_touch_index
        holding_candles = relevant[reaction_index:]
        closes_holding = sum(1 for candle in holding_candles if candle.get("close") <= low)
        edge_holding = bool(holding_candles) and all(candle.get("close") <= low for candle in holding_candles)
        stronger = bool(stronger_reaction)
        status = "REJECTION" if stronger else "HOLD"
        label = "SUPPLY REJECTION" if stronger else "SUPPLY HOLD"

    if not reaction_candle or not edge_holding:
        base.update({
            "reaction_status": "ACTIVE",
            "last_reaction_time": last_touch.get("time"),
            "reaction_confidence": min(45, 20 + len(touches) * 5),
            "reaction_warnings": ["Zone was touched but the defended edge is not currently holding on closes."],
        })
        return base

    confidence = (68 if stronger else 48) + min(18, closes_holding * 7) + min(10, len(touches) * 3)
    warnings = []
    if closes_holding < 2:
        warnings.append("Reaction is early; wait for additional closes holding the defended edge.")
    if len(touches) >= 3:
        warnings.append("Multiple retests may weaken the zone.")
    base.update({
        "reaction_status": status,
        "reaction_label": label,
        "closes_holding_edge": closes_holding,
        "last_reaction_time": reaction_candle.get("time"),
        "reaction_confidence": min(95, confidence),
        "reaction_warnings": warnings,
    })
    return base


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
            zone.update(evaluate_zone_reaction(zone, candles))

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
_logged_outcome_keys = set()


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
                "market_confirmation": professional_context.get("market_confirmation", {}),
                "market_confirmation_score": professional_context.get("market_confirmation_score"),
                "spy_bias": professional_context.get("spy_bias"),
                "qqq_bias": professional_context.get("qqq_bias"),
                "aapl_relative_strength": professional_context.get("aapl_relative_strength"),
                "no_trade": professional_context.get("no_trade"),
                "warnings": professional_context.get("warnings", []),
                "aapl_regime": professional_context.get("aapl", {}).get("regime", {}).get("regime"),
                "aapl_chop_score": professional_context.get("aapl", {}).get("regime", {}).get("chop_score"),
                "aapl_regime_score": professional_context.get("aapl", {}).get("regime", {}).get("regime_score"),
                "aapl_regime_confidence": professional_context.get("aapl", {}).get("regime", {}).get("regime_confidence"),
                "aapl_action_label": professional_context.get("aapl", {}).get("regime", {}).get("action_label"),
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

            setup_key_value = setup_key(symbol, timeframe, setup)
            outcome_key = f"{setup_key_value}|h{horizon}"

            if outcome_key in _logged_outcome_keys:
                continue

            _logged_outcome_keys.add(outcome_key)

            outcome = {
                "evaluated_at": now_ts,
                "symbol": symbol,
                "timeframe": timeframe,
                "horizon_candles": horizon,
                "setup_key": setup_key_value,
                "outcome_key": outcome_key,
                "setup_status": setup.get("status"),
                "professional_grade": setup.get("professional_grade"),
                "professional_score": setup.get("professional_score"),
                "setup_intent": setup.get("setup_intent"),
                "blocked_reasons": setup.get("blocked_reasons") or [],
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



def load_existing_setup_log_keys():
    """
    Rebuild de-dupe memory from existing log files after restart.
    This prevents repeated setup/outcome logging.
    """
    if os.path.exists(SETUP_LOG_PATH):
        try:
            with open(SETUP_LOG_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        row = json.loads(line)
                        setup = row.get("setup", {})
                        symbol = row.get("symbol", SYMBOL)
                        timeframe = row.get("timeframe")
                        if setup and timeframe:
                            _logged_setup_keys.add(setup_key(symbol, timeframe, setup))
                    except Exception:
                        continue
        except Exception:
            pass

    if os.path.exists(SETUP_OUTCOME_PATH):
        try:
            with open(SETUP_OUTCOME_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        row = json.loads(line)
                        outcome_key = row.get("outcome_key")
                        if outcome_key:
                            _logged_outcome_keys.add(outcome_key)
                        else:
                            setup_key_value = row.get("setup_key")
                            horizon = row.get("horizon_candles")
                            if setup_key_value and horizon is not None:
                                _logged_outcome_keys.add(f"{setup_key_value}|h{horizon}")
                    except Exception:
                        continue
        except Exception:
            pass

    return {
        "setup_keys": len(_logged_setup_keys),
        "outcome_keys": len(_logged_outcome_keys),
    }


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
        professional_grade = row.get("professional_grade")
        setup_intent = row.get("setup_intent")
        blocked_reasons = row.get("blocked_reasons") or []
        performance_view = (
            "tradable"
            if professional_grade in {"A+", "A", "B", "C"}
            and setup_intent not in {"NO TRADE", "RESEARCH CONTEXT"}
            else "research"
        )
        key = (
            row.get("timeframe"),
            row.get("horizon_candles"),
            row.get("direction"),
            professional_grade,
            row.get("source"),
            setup_intent,
            performance_view,
        )
        bucket = buckets.setdefault(key, {
            "timeframe": row.get("timeframe"),
            "horizon_candles": row.get("horizon_candles"),
            "direction": row.get("direction"),
            "professional_grade": professional_grade,
            "source": row.get("source"),
            "setup_intent": setup_intent or ("NO TRADE" if professional_grade == "NO_TRADE" else "WATCH ONLY"),
            "blocked_reasons": blocked_reasons,
            "performance_view": performance_view,
            "count": 0,
            "invalidated": 0,
            "avg_favorable_move": 0.0,
            "avg_adverse_move": 0.0,
        })

        bucket["count"] += 1

        if row.get("invalidated_within_horizon"):
            bucket["invalidated"] += 1
        bucket["blocked_reasons"] = list(dict.fromkeys((bucket.get("blocked_reasons") or []) + blocked_reasons))

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
            "regime": "CHOP",
            "regime_score": 0,
            "chop_score": 50,
            "trend_score": 0,
            "range_score": 0,
            "regime_confidence": "LOW",
            "action_label": "WAIT_FOR_BREAKOUT",
            "regime_reasons": ["Not enough candles for confident regime detection."],
            "regime_warnings": ["Regime confidence is low."],
            "read_only": True,
            "reason": "not_enough_candles",
        }

    recent = candles[-20:]
    closes = [c["close"] for c in recent]
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]

    recent_range = max(highs) - min(lows)
    avg_range = sum(max(c["high"] - c["low"], 0.01) for c in recent) / len(recent)
    avg_body_ratio = sum(candle_body_ratio(c) for c in recent) / len(recent)

    ema9 = indicators.get("ema9") or []
    ema20 = indicators.get("ema20") or []
    vwap = indicators.get("vwap") or []
    atr14 = latest_indicator_value(indicators.get("atr14"))
    rvol = calc_rvol(candles, 20)

    ema_cross_noise = 0
    paired = list(zip(ema9[-20:], ema20[-20:]))
    last_side = None
    ema_gaps = []

    for e9, e20 in paired:
        v9 = e9.get("value")
        v20 = e20.get("value")
        if v9 is None or v20 is None:
            continue
        ema_gaps.append(abs(v9 - v20))

        side = "above" if v9 > v20 else "below" if v9 < v20 else "same"
        if last_side and side != last_side and side != "same":
            ema_cross_noise += 1
        if side != "same":
            last_side = side

    vwap_slope = slope_from_series(vwap, bars=5)
    vwap_values = [item.get("value") for item in vwap[-len(recent):] if item.get("value") is not None]
    vwap_crosses = 0
    last_vwap_side = None
    for close, value in zip(closes[-len(vwap_values):], vwap_values):
        side = "above" if close > value else "below" if close < value else "same"
        if last_vwap_side and side != last_vwap_side and side != "same":
            vwap_crosses += 1
        if side != "same":
            last_vwap_side = side

    overlap_count = 0
    for i in range(1, len(recent)):
        prev = recent[i - 1]
        cur = recent[i]
        if max(prev["low"], cur["low"]) <= min(prev["high"], cur["high"]):
            overlap_count += 1

    overlap_ratio = overlap_count / max(1, len(recent) - 1)
    ema_gap = sum(ema_gaps) / len(ema_gaps) if ema_gaps else 0
    ema_compressed = ema_gap <= max(0.03, (atr14 or avg_range) * 0.20)
    close_above_vwap = sum(1 for close, value in zip(closes[-len(vwap_values):], vwap_values) if close > value)
    close_below_vwap = sum(1 for close, value in zip(closes[-len(vwap_values):], vwap_values) if close < value)
    vwap_consistency = max(close_above_vwap, close_below_vwap) / max(1, len(vwap_values))

    higher_structure = sum(
        1 for i in range(1, len(recent))
        if recent[i]["high"] > recent[i - 1]["high"] and recent[i]["low"] > recent[i - 1]["low"]
    )
    lower_structure = sum(
        1 for i in range(1, len(recent))
        if recent[i]["high"] < recent[i - 1]["high"] and recent[i]["low"] < recent[i - 1]["low"]
    )
    structure_ratio = max(higher_structure, lower_structure) / max(1, len(recent) - 1)

    trend_score = 0
    trend_score += int(vwap_consistency * 25)
    trend_score += 18 if ema_cross_noise == 0 and not ema_compressed else 8 if ema_cross_noise <= 1 else 0
    trend_score += 15 if vwap_slope["label"] != "FLAT" else 0
    trend_score += int(structure_ratio * 20)
    trend_score += 12 if avg_body_ratio >= 0.55 else 6 if avg_body_ratio >= 0.45 else 0
    trend_score += 5 if atr14 is not None and atr14 >= max(0.15, avg_range * 0.65) else 0
    trend_score += 5 if rvol is not None and rvol >= 0.8 else 0

    range_score = 0
    range_score += 25 if vwap_slope["label"] == "FLAT" else 8
    range_score += 20 if recent_range >= avg_range * 3 and recent_range <= avg_range * 7 else 5
    range_score += 18 if 0.40 <= overlap_ratio <= 0.70 else 5
    range_score += 15 if 2 <= vwap_crosses <= 5 else 5
    range_score += 12 if 0.35 <= avg_body_ratio <= 0.60 else 4
    range_score += 10 if ema_cross_noise <= 2 else 3

    chop_score = 0
    chop_score += 20 if avg_body_ratio < 0.40 else 8 if avg_body_ratio < 0.50 else 0
    chop_score += 22 if overlap_ratio > 0.70 else 10 if overlap_ratio > 0.55 else 0
    chop_score += 18 if ema_cross_noise >= 3 else 8 if ema_cross_noise >= 2 else 0
    chop_score += 15 if ema_compressed else 0
    chop_score += 18 if vwap_crosses >= 5 else 8 if vwap_crosses >= 3 else 0
    chop_score += 7 if vwap_slope["label"] == "FLAT" else 0
    chop_score += 8 if atr14 is not None and atr14 < 0.15 else 0
    chop_score += 7 if rvol is not None and rvol < 0.70 else 0

    trend_score = int(max(0, min(100, trend_score)))
    range_score = int(max(0, min(100, range_score)))
    chop_score = int(max(0, min(100, chop_score)))
    scores = {"TREND": trend_score, "RANGE": range_score, "CHOP": chop_score}
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    regime, regime_score = ranked[0]
    score_gap = regime_score - ranked[1][1]

    if regime_score >= 75 and score_gap >= 15:
        confidence = "HIGH"
    elif regime_score >= 58 and score_gap >= 8:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    if regime == "TREND" and confidence != "LOW":
        action_label = "PULLBACKS_ALLOWED"
    elif regime == "RANGE" and confidence != "LOW":
        action_label = "TRADE_RANGE_EDGES_ONLY"
    elif regime == "CHOP" and chop_score >= 65:
        action_label = "NO_NEW_TRADES"
    else:
        action_label = "WAIT_FOR_BREAKOUT"

    reasons = [
        f"VWAP consistency {round(vwap_consistency * 100)}%",
        f"VWAP crosses {vwap_crosses}",
        f"EMA crosses {ema_cross_noise}",
        f"candle overlap {round(overlap_ratio * 100)}%",
        f"body strength {round(avg_body_ratio * 100)}%",
    ]
    if vwap_slope["label"] != "FLAT":
        reasons.append(f"VWAP is {vwap_slope['label'].lower()}")
    if ema_compressed:
        reasons.append("EMA9 and EMA20 are compressed")
    if structure_ratio >= 0.45:
        reasons.append("directional high/low structure is present")

    warnings = []
    if action_label == "NO_NEW_TRADES":
        warnings.append("High chop conditions: no new trades.")
    if action_label == "WAIT_FOR_BREAKOUT":
        warnings.append("Regime scores are mixed: wait for breakout.")
    if vwap_crosses >= 5:
        warnings.append("Price is crossing VWAP repeatedly.")
    if ema_cross_noise >= 3:
        warnings.append("EMA9 and EMA20 are crossing repeatedly.")
    if rvol is not None and rvol < 0.70:
        warnings.append("Relative volume is low.")

    return {
        "regime": regime,
        "regime_score": regime_score,
        "trend_score": trend_score,
        "range_score": range_score,
        "chop_score": chop_score,
        "regime_confidence": confidence,
        "action_label": action_label,
        "regime_reasons": reasons,
        "regime_warnings": warnings,
        "read_only": True,
        "vwap_slope": vwap_slope,
        "ema_cross_noise": ema_cross_noise,
        "ema_compressed": ema_compressed,
        "vwap_crosses": vwap_crosses,
        "overlap_ratio": round(overlap_ratio, 2),
        "avg_body_ratio": round(avg_body_ratio, 2),
        "recent_range": round_price(recent_range),
        "avg_range": round_price(avg_range),
        "rvol": rvol,
        "atr14": round_price(atr14),
        "reason": "Weighted market regime engine using VWAP, EMA, structure, candle quality, ATR, and RVOL.",
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
        session_open = source[0]["open"] if source else None
        percent_change = (
            (current - session_open) / session_open * 100
            if current is not None and session_open
            else None
        )
        recent = source[-8:]
        bullish_structure = sum(
            1 for i in range(1, len(recent))
            if recent[i]["high"] > recent[i - 1]["high"] and recent[i]["low"] > recent[i - 1]["low"]
        )
        bearish_structure = sum(
            1 for i in range(1, len(recent))
            if recent[i]["high"] < recent[i - 1]["high"] and recent[i]["low"] < recent[i - 1]["low"]
        )

        bullish_score = 0
        bearish_score = 0
        if trend.get("price") is not None and trend.get("vwap") is not None:
            if trend["price"] > trend["vwap"]:
                bullish_score += 25
            elif trend["price"] < trend["vwap"]:
                bearish_score += 25
        if trend.get("ema9") is not None and trend.get("ema20") is not None:
            if trend["ema9"] > trend["ema20"]:
                bullish_score += 25
            elif trend["ema9"] < trend["ema20"]:
                bearish_score += 25
        bullish_score += min(20, bullish_structure * 5)
        bearish_score += min(20, bearish_structure * 5)
        if regime.get("regime") == "TREND" and regime.get("action_label") == "PULLBACKS_ALLOWED":
            if trend.get("bullish"):
                bullish_score += 20
            if trend.get("bearish"):
                bearish_score += 20
        elif regime.get("action_label") in {"NO_NEW_TRADES", "WAIT_FOR_BREAKOUT"}:
            bullish_score -= 10
            bearish_score -= 10
        if percent_change is not None:
            if percent_change >= 0.20:
                bullish_score += 10
            elif percent_change <= -0.20:
                bearish_score += 10

        bullish_score = max(0, min(100, int(bullish_score)))
        bearish_score = max(0, min(100, int(bearish_score)))
        confirmation_score = max(bullish_score, bearish_score)

        if confirmation_score < 35:
            bias = "UNKNOWN"
        elif abs(bullish_score - bearish_score) < 15 or regime.get("regime") == "CHOP":
            bias = "MIXED"
        elif bullish_score > bearish_score:
            bias = "BULLISH"
        else:
            bias = "BEARISH"

        return {
            "symbol": symbol,
            "current_price": round_price(current),
            "session_open": round_price(session_open),
            "percent_change": round(percent_change, 3) if percent_change is not None else None,
            "trend": trend,
            "regime": regime,
            "rvol": calc_rvol(source, 20),
            "atr14": round_price(latest_indicator_value(indicators.get("atr14"))),
            "bias": bias,
            "confirmation_score": confirmation_score,
            "bullish_score": bullish_score,
            "bearish_score": bearish_score,
            "data_status": "ok",
        }
    except Exception as e:
        return {
            "symbol": symbol,
            "current_price": None,
            "trend": {"label": "UNKNOWN"},
            "regime": {"regime": "UNKNOWN", "reason": str(e)},
            "rvol": None,
            "bias": "UNKNOWN",
            "confirmation_score": 0,
            "data_status": "error",
            "error": str(e),
        }


def build_professional_market_context(candles, indicators, current_price, timeframe, today_start, today_end, symbol=SYMBOL):
    symbol = normalize_symbol(symbol)
    aapl_trend = confirmation_trend(current_price, indicators)
    aapl_regime = detect_chop_regime(candles, indicators, current_price=current_price)
    aapl_rvol = calc_rvol(candles, 20)
    atr14 = latest_indicator_value(indicators.get("atr14"))

    related_market_symbols = get_related_market_symbols(symbol)
    primary_symbol = related_market_symbols.get("primary_market") or "SPY"
    secondary_symbol = related_market_symbols.get("secondary_market") or "QQQ"
    sector_symbol = related_market_symbols.get("sector_or_peer")
    spy = fetch_context_symbol(primary_symbol, today_start, today_end, timeframe)
    qqq = fetch_context_symbol(secondary_symbol, today_start, today_end, timeframe)
    sector_context = fetch_context_symbol(sector_symbol, today_start, today_end, timeframe) if sector_symbol else None

    spy_bias = spy.get("bias", "UNKNOWN")
    qqq_bias = qqq.get("bias", "UNKNOWN")
    spy_bull = spy_bias == "BULLISH"
    qqq_bull = qqq_bias == "BULLISH"
    spy_bear = spy_bias == "BEARISH"
    qqq_bear = qqq_bias == "BEARISH"

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
    aapl_vs_spy_change = None
    aapl_vs_qqq_change = None
    if current_price is not None and candles:
        aapl_open = candles[0]["open"]
        if aapl_open:
            aapl_change = (current_price - aapl_open) / aapl_open * 100
        else:
            aapl_change = None

        if aapl_change is not None and spy.get("percent_change") is not None:
            aapl_vs_spy_change = round(aapl_change - spy["percent_change"], 3)
        if aapl_change is not None and qqq.get("percent_change") is not None:
            aapl_vs_qqq_change = round(aapl_change - qqq["percent_change"], 3)

        comparisons = [value for value in [aapl_vs_spy_change, aapl_vs_qqq_change] if value is not None]
        if len(comparisons) == 2 and min(comparisons) >= 0.20:
            relative_strength = "STRONG"
        elif len(comparisons) == 2 and max(comparisons) <= -0.20:
            relative_strength = "WEAK"
        elif comparisons:
            relative_strength = "NEUTRAL"

    spy_confirmation_score = int(spy.get("confirmation_score", 0) or 0)
    qqq_confirmation_score = int(qqq.get("confirmation_score", 0) or 0)
    if market_alignment in {"BULLISH", "BEARISH"}:
        market_confirmation_score = int((spy_confirmation_score + qqq_confirmation_score) / 2)
    elif market_alignment == "MIXED":
        market_confirmation_score = int((spy_confirmation_score + qqq_confirmation_score) / 4)
    else:
        market_confirmation_score = 0

    market_reasons = [
        f"{primary_symbol} bias {spy_bias} score {spy_confirmation_score}",
        f"{secondary_symbol} bias {qqq_bias} score {qqq_confirmation_score}",
    ]
    if relative_strength != "UNKNOWN":
        market_reasons.append(f"{symbol} relative strength {relative_strength}")

    market_warnings = []
    if market_alignment == "MIXED":
        market_warnings.append(f"{primary_symbol} and {secondary_symbol} are not aligned.")
    if market_alignment == "UNKNOWN":
        market_warnings.append("Market confirmation is unavailable.")
    if spy.get("regime", {}).get("action_label") in {"NO_NEW_TRADES", "WAIT_FOR_BREAKOUT"}:
        market_warnings.append(f"{primary_symbol} regime is not confirming clean continuation.")
    if qqq.get("regime", {}).get("action_label") in {"NO_NEW_TRADES", "WAIT_FOR_BREAKOUT"}:
        market_warnings.append(f"{secondary_symbol} regime is not confirming clean continuation.")

    market_confirmation = {
        "market_confirmation": market_alignment,
        "market_confirmation_score": market_confirmation_score,
        "spy_confirmation_score": spy_confirmation_score,
        "qqq_confirmation_score": qqq_confirmation_score,
        "spy_bias": spy_bias,
        "qqq_bias": qqq_bias,
        "primary_market_symbol": primary_symbol,
        "secondary_market_symbol": secondary_symbol,
        "sector_or_peer_symbol": sector_symbol,
        "primary_market_bias": spy_bias,
        "secondary_market_bias": qqq_bias,
        "selected_symbol_relative_strength": relative_strength,
        "aapl_relative_strength": relative_strength,
        "aapl_vs_spy_change": aapl_vs_spy_change,
        "aapl_vs_qqq_change": aapl_vs_qqq_change,
        "market_reasons": market_reasons,
        "market_warnings": market_warnings,
        "read_only": True,
    }

    no_trade = False
    warnings = []

    if aapl_regime["regime"] == "CHOP":
        no_trade = True
        warnings.append(f"{symbol} chop regime detected.")

    if spy.get("regime", {}).get("regime") == "CHOP" and qqq.get("regime", {}).get("regime") == "CHOP":
        no_trade = True
        warnings.append(f"{primary_symbol} and {secondary_symbol} both choppy.")

    if market_alignment == "MIXED":
        warnings.append(f"{primary_symbol} and {secondary_symbol} are mixed.")
        aapl_regime["regime_confidence"] = "LOW"
        if aapl_regime.get("action_label") != "NO_NEW_TRADES":
            aapl_regime["action_label"] = "WAIT_FOR_BREAKOUT"
        aapl_regime.setdefault("regime_warnings", []).append(f"{primary_symbol} and {secondary_symbol} context is mixed.")

    if aapl_regime.get("action_label") == "NO_NEW_TRADES":
        no_trade = True
        warnings.append("Market regime says no new trades.")
    elif aapl_regime.get("action_label") == "WAIT_FOR_BREAKOUT":
        warnings.append("Market regime says wait for breakout.")

    if aapl_rvol is not None and aapl_rvol < 0.80:
        warnings.append(f"{symbol} relative volume is low.")

    if atr14 is not None and atr14 < 0.15:
        warnings.append(f"{symbol} ATR is low for active intraday movement.")

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
        "symbol": symbol,
        "selected_symbol": symbol,
        "related_market_symbols": related_market_symbols,
        "selected": {
            "trend": aapl_trend,
            "regime": aapl_regime,
            "rvol": aapl_rvol,
            "atr14": round_price(atr14),
            "relative_strength": relative_strength,
        },
        "aapl": {
            "trend": aapl_trend,
            "regime": aapl_regime,
            "rvol": aapl_rvol,
            "atr14": round_price(atr14),
            "relative_strength": relative_strength,
        },
        "spy": spy,
        "qqq": qqq,
        "primary_market_context": spy,
        "secondary_market_context": qqq,
        "sector_or_peer_context": sector_context,
        "market_alignment": market_alignment,
        "market_confirmation": market_confirmation,
        "market_confirmation_score": market_confirmation_score,
        "spy_confirmation_score": spy_confirmation_score,
        "qqq_confirmation_score": qqq_confirmation_score,
        "spy_bias": spy_bias,
        "qqq_bias": qqq_bias,
        "aapl_relative_strength": relative_strength,
        "aapl_vs_spy_change": aapl_vs_spy_change,
        "aapl_vs_qqq_change": aapl_vs_qqq_change,
        "market_reasons": market_reasons,
        "market_warnings": market_warnings,
        "professional_grade": professional_grade,
        "no_trade": no_trade,
        "warnings": warnings,
        "read_only": True,
        "note": "Professional context only. It does not place trades.",
    }




def trend_label_matches_direction(trend_label, direction):
    if direction == "bullish":
        return trend_label in {"BULLISH", "UPTREND", "STRONG_BULLISH"}
    if direction == "bearish":
        return trend_label in {"BEARISH", "DOWNTREND", "STRONG_BEARISH"}
    return False


def strict_trade_quality_grade(setup, professional_context):
    """
    Strict read-only setup grading v2.

    Uses:
    - confirmation stage
    - level / zone quality
    - market regime
    - SPY / QQQ market confirmation
    - AAPL relative strength
    - risk/reward grade
    - trend / reclaim / structure / volume confirmations

    This does not create orders.
    It only scores whether the chart context is good enough to respect.
    """
    professional_context = professional_context or {}
    setup = setup or {}

    direction = setup.get("direction")
    status = setup.get("status")
    setup_score = setup.get("score") or 0

    confirmation_stage = setup.get("confirmation_stage") or status or "WATCH"
    confirmation_score = setup.get("confirmation_score")
    if confirmation_score is None:
        confirmation_score = setup_score

    risk_reward = setup.get("risk_reward") or {}
    rr_grade = risk_reward.get("rr_grade")
    rr_2 = risk_reward.get("rr_2")
    rr_1 = risk_reward.get("rr_1")

    aapl_context = professional_context.get("aapl", {}) or {}
    spy_context = professional_context.get("spy", {}) or {}
    qqq_context = professional_context.get("qqq", {}) or {}

    regime_obj = aapl_context.get("regime", {}) or {}
    regime = regime_obj.get("regime")
    chop_score = regime_obj.get("chop_score")
    regime_confidence = regime_obj.get("regime_confidence")
    action_label = regime_obj.get("action_label")

    aapl_rvol = aapl_context.get("rvol")
    atr14 = aapl_context.get("atr14")

    # Backward-compatible market fields.
    old_market_alignment = professional_context.get("market_alignment")
    market_confirmation_obj = (
        professional_context.get("market_confirmation")
        or professional_context.get("market_confirmation_engine")
        or {}
    )

    market_confirmation = (
        market_confirmation_obj.get("market_confirmation")
        or professional_context.get("market_confirmation")
        or old_market_alignment
        or "UNKNOWN"
    )

    market_confirmation_score = (
        market_confirmation_obj.get("market_confirmation_score")
        or professional_context.get("market_confirmation_score")
        or 0
    )

    spy_bias = (
        market_confirmation_obj.get("spy_bias")
        or professional_context.get("spy_bias")
        or spy_context.get("bias")
        or spy_context.get("trend", {}).get("label")
    )

    qqq_bias = (
        market_confirmation_obj.get("qqq_bias")
        or professional_context.get("qqq_bias")
        or qqq_context.get("bias")
        or qqq_context.get("trend", {}).get("label")
    )

    aapl_relative_strength = (
        market_confirmation_obj.get("aapl_relative_strength")
        or professional_context.get("aapl_relative_strength")
        or "UNKNOWN"
    )

    trend_confirmed = bool(setup.get("trend_confirmed"))
    reclaim_confirmed = bool(setup.get("reclaim_confirmed"))
    structure_confirmed = bool(setup.get("structure_confirmed"))
    volume_confirmed = bool(setup.get("volume_confirmed"))
    market_aligned = bool(setup.get("market_aligned"))

    # Level / zone quality fields. These may exist on S/R setups, supply/demand
    # setups, or enriched setup objects.
    level_quality_grade = (
        setup.get("quality_grade")
        or setup.get("level_quality_grade")
        or setup.get("sr_quality_grade")
    )
    level_quality_score = (
        setup.get("quality_score")
        or setup.get("level_quality_score")
        or setup.get("sr_quality_score")
    )

    zone_quality_grade = (
        setup.get("zone_quality_grade")
        or setup.get("supply_demand_quality_grade")
    )
    zone_quality_score = (
        setup.get("zone_quality_score")
        or setup.get("supply_demand_quality_score")
    )

    source = str(setup.get("source") or "").lower()
    kind = str(setup.get("kind") or "").lower()

    uses_zone = (
        "supply" in source
        or "demand" in source
        or kind in {"supply", "demand"}
    )
    uses_sr = (
        "support" in source
        or "resistance" in source
        or kind in {"support", "resistance"}
    )

    active_quality_grade = zone_quality_grade if uses_zone else level_quality_grade
    active_quality_score = zone_quality_score if uses_zone else level_quality_score

    if active_quality_grade is None:
        active_quality_grade = "UNKNOWN"

    def bias_matches_setup(bias, setup_direction):
        if setup_direction == "bullish":
            return bias in {"BULLISH", "UPTREND", "STRONG_BULLISH"}
        if setup_direction == "bearish":
            return bias in {"BEARISH", "DOWNTREND", "STRONG_BEARISH"}
        return False

    def bias_opposes_setup(bias, setup_direction):
        if setup_direction == "bullish":
            return bias in {"BEARISH", "DOWNTREND", "STRONG_BEARISH"}
        if setup_direction == "bearish":
            return bias in {"BULLISH", "UPTREND", "STRONG_BULLISH"}
        return False

    market_agrees = bias_matches_setup(market_confirmation, direction)
    market_opposes = bias_opposes_setup(market_confirmation, direction)

    spy_agrees = bias_matches_setup(spy_bias, direction)
    qqq_agrees = bias_matches_setup(qqq_bias, direction)

    spy_opposes = bias_opposes_setup(spy_bias, direction)
    qqq_opposes = bias_opposes_setup(qqq_bias, direction)

    relative_strength_good = (
        (direction == "bullish" and aapl_relative_strength == "STRONG")
        or (direction == "bearish" and aapl_relative_strength == "WEAK")
    )

    relative_strength_bad = (
        (direction == "bullish" and aapl_relative_strength == "WEAK")
        or (direction == "bearish" and aapl_relative_strength == "STRONG")
    )

    warnings = []

    # Hard reject / strong downgrade conditions.
    if status == "INVALIDATED" or confirmation_stage == "FAILED":
        warnings.append("Setup failed or invalidated.")
    if professional_context.get("no_trade"):
        warnings.append("Professional context says no trade.")
    if regime == "CHOP":
        warnings.append("Selected symbol is in chop.")
    if action_label == "NO_NEW_TRADES":
        warnings.append("Market regime says no new trades.")
    if action_label == "WAIT_FOR_BREAKOUT":
        warnings.append("Market regime says wait for breakout.")
    if chop_score is not None and chop_score >= 70:
        warnings.append("Chop score too high.")
    if aapl_rvol is not None and aapl_rvol < 0.35:
        warnings.append("Selected-symbol relative volume extremely low.")
    elif aapl_rvol is not None and aapl_rvol < 0.7:
        warnings.append("Selected-symbol relative volume low.")
    if atr14 is not None and atr14 < 0.12:
        warnings.append("ATR too low for clean intraday movement.")
    if market_opposes:
        warnings.append("Market confirmation is against setup direction.")
    if spy_opposes:
        warnings.append("SPY bias opposes setup.")
    if qqq_opposes:
        warnings.append("QQQ bias opposes setup.")
    if relative_strength_bad:
        warnings.append("Selected-symbol relative strength conflicts with setup direction.")
    if rr_grade == "BAD":
        warnings.append("Risk/reward is BAD.")
    elif rr_grade == "WEAK":
        warnings.append("Risk/reward is WEAK.")
    if confirmation_stage in {"WATCH", None}:
        warnings.append("Setup is watch-only.")
    if not reclaim_confirmed:
        warnings.append("No reclaim/rejection confirmation yet.")
    if not structure_confirmed:
        warnings.append("Structure not confirmed.")
    if not trend_confirmed:
        warnings.append("Selected-symbol trend filter not confirmed.")
    if not volume_confirmed:
        warnings.append("Volume not confirmed.")

    if active_quality_grade == "WEAK":
        warnings.append("Underlying level/zone quality is weak.")
    elif active_quality_grade == "C":
        warnings.append("Underlying level/zone quality is only C grade.")
    if uses_zone and setup.get("zone_previously_broken"):
        warnings.append("Zone previously failed/broken.")

    hard_no_trade = (
        status == "INVALIDATED"
        or confirmation_stage == "FAILED"
        or professional_context.get("no_trade")
        or regime == "CHOP"
        or action_label == "NO_NEW_TRADES"
        or (chop_score is not None and chop_score >= 75)
        or (aapl_rvol is not None and aapl_rvol < 0.30)
        or market_opposes
        or rr_grade == "BAD"
        or active_quality_grade == "WEAK"
        or (uses_zone and setup.get("zone_previously_broken") and confirmation_stage != "CONFIRMED")
    )

    score = 0

    # Base setup score.
    if setup_score >= 80:
        score += 14
    elif setup_score >= 65:
        score += 10
    elif setup_score >= 50:
        score += 6
    elif setup_score >= 35:
        score += 3

    # Confirmation stage score.
    if confirmation_stage == "CONFIRMED":
        score += 24
    elif confirmation_stage == "EARLY_CONFIRM":
        score += 12
    elif confirmation_stage == "WATCH":
        score += 2

    if confirmation_score is not None:
        if confirmation_score >= 80:
            score += 10
        elif confirmation_score >= 65:
            score += 7
        elif confirmation_score >= 50:
            score += 4

    # Level / zone quality.
    if active_quality_grade == "A":
        score += 14
    elif active_quality_grade == "B":
        score += 10
    elif active_quality_grade == "C":
        score += 3
    elif active_quality_grade == "WEAK":
        score -= 20

    if isinstance(active_quality_score, (int, float)):
        if active_quality_score >= 80:
            score += 5
        elif active_quality_score >= 65:
            score += 3
        elif active_quality_score < 50:
            score -= 6

    # Setup confirmations.
    if trend_confirmed:
        score += 9
    if reclaim_confirmed:
        score += 12
    if structure_confirmed:
        score += 10
    if volume_confirmed:
        score += 9

    # Market regime.
    if regime == "TREND":
        score += 10
    elif regime == "RANGE":
        score += 2
    elif regime == "CHOP":
        score -= 25

    if regime_confidence == "HIGH" and regime == "TREND":
        score += 4
    if action_label == "PULLBACKS_ALLOWED":
        score += 6
    elif action_label == "TRADE_RANGE_EDGES_ONLY":
        score += 1
    elif action_label in {"NO_NEW_TRADES", "WAIT_FOR_BREAKOUT"}:
        score -= 15

    # Volume / volatility.
    if aapl_rvol is not None:
        if aapl_rvol >= 1.5:
            score += 10
        elif aapl_rvol >= 1.0:
            score += 7
        elif aapl_rvol >= 0.7:
            score += 3
        elif aapl_rvol < 0.5:
            score -= 8

    if atr14 is not None:
        if atr14 >= 0.30:
            score += 5
        elif atr14 >= 0.18:
            score += 2
        elif atr14 < 0.12:
            score -= 5

    # SPY / QQQ / market confirmation.
    if market_agrees:
        score += 12
    elif market_confirmation in {"MIXED", "UNKNOWN"}:
        score -= 6

    if spy_agrees:
        score += 5
    elif spy_opposes:
        score -= 8

    if qqq_agrees:
        score += 5
    elif qqq_opposes:
        score -= 8

    if market_aligned:
        score += 4

    if market_confirmation_score:
        if market_confirmation_score >= 75:
            score += 5
        elif market_confirmation_score >= 55:
            score += 2
        elif market_confirmation_score < 40:
            score -= 4

    if relative_strength_good:
        score += 6
    elif relative_strength_bad:
        score -= 8

    # Risk/reward.
    if rr_grade == "GOOD":
        score += 12
    elif rr_grade == "OK":
        score += 7
    elif rr_grade == "WEAK":
        score -= 8
    elif rr_grade == "BAD":
        score -= 25

    if isinstance(rr_2, (int, float)):
        if rr_2 >= 2.5:
            score += 5
        elif rr_2 >= 2.0:
            score += 3
        elif rr_2 < 1.5:
            score -= 5
    elif isinstance(rr_1, (int, float)) and rr_1 < 1.0:
        score -= 5

    score = max(0, min(100, int(score)))

    # Grade gates.
    # A+ should be rare and require strong agreement.
    can_be_a_plus = (
        confirmation_stage == "CONFIRMED"
        and reclaim_confirmed
        and structure_confirmed
        and volume_confirmed
        and trend_confirmed
        and active_quality_grade in {"A", "B", "UNKNOWN"}
        and rr_grade == "GOOD"
        and regime == "TREND"
        and action_label == "PULLBACKS_ALLOWED"
        and (market_agrees or (spy_agrees and qqq_agrees))
        and not hard_no_trade
    )

    can_be_a = (
        confirmation_stage == "CONFIRMED"
        and reclaim_confirmed
        and structure_confirmed
        and volume_confirmed
        and active_quality_grade in {"A", "B", "C", "UNKNOWN"}
        and rr_grade in {"GOOD", "OK", None}
        and regime != "CHOP"
        and action_label not in {"NO_NEW_TRADES", "WAIT_FOR_BREAKOUT"}
        and not market_opposes
        and not hard_no_trade
    )

    can_be_b = (
        confirmation_stage in {"CONFIRMED", "EARLY_CONFIRM"}
        and reclaim_confirmed
        and (structure_confirmed or trend_confirmed)
        and active_quality_grade not in {"WEAK"}
        and rr_grade not in {"BAD"}
        and regime != "CHOP"
        and not market_opposes
        and not hard_no_trade
    )

    if hard_no_trade:
        grade = "NO_TRADE"
    elif score >= 90 and can_be_a_plus:
        grade = "A+"
    elif score >= 78 and can_be_a:
        grade = "A"
    elif score >= 62 and can_be_b:
        grade = "B"
    elif score >= 45:
        grade = "C"
    else:
        grade = "NO_TRADE"

    return {
        "grade": grade,
        "score": score,
        "warnings": list(dict.fromkeys(warnings)),
        "checks": {
            "direction": direction,
            "status": status,
            "confirmation_stage": confirmation_stage,
            "confirmation_score": confirmation_score,
            "regime": regime,
            "regime_confidence": regime_confidence,
            "action_label": action_label,
            "chop_score": chop_score,
            "aapl_rvol": aapl_rvol,
            "atr14": atr14,
            "trend_confirmed": trend_confirmed,
            "reclaim_confirmed": reclaim_confirmed,
            "structure_confirmed": structure_confirmed,
            "volume_confirmed": volume_confirmed,
            "level_quality_grade": level_quality_grade,
            "level_quality_score": level_quality_score,
            "zone_quality_grade": zone_quality_grade,
            "zone_quality_score": zone_quality_score,
            "active_quality_grade": active_quality_grade,
            "active_quality_score": active_quality_score,
            "market_confirmation": market_confirmation,
            "market_confirmation_score": market_confirmation_score,
            "market_aligned": market_aligned,
            "spy_bias": spy_bias,
            "qqq_bias": qqq_bias,
            "spy_agrees": spy_agrees,
            "qqq_agrees": qqq_agrees,
            "market_agrees": market_agrees,
            "market_opposes": market_opposes,
            "aapl_relative_strength": aapl_relative_strength,
            "relative_strength_good": relative_strength_good,
            "relative_strength_bad": relative_strength_bad,
            "rr_grade": rr_grade,
            "rr_1": rr_1,
            "rr_2": rr_2,
            "uses_zone": uses_zone,
            "uses_sr": uses_sr,
            "zone_previously_broken": bool(setup.get("zone_previously_broken")),
        },
        "read_only": True,
    }


def setup_quality_label(setup, professional_context=None):
    setup = setup or {}
    professional_context = professional_context or {}
    checks = setup.get("quality_checks") or {}
    warnings = [str(warning) for warning in (setup.get("quality_warnings") or [])]
    rr_grade = (setup.get("risk_reward") or {}).get("rr_grade")
    market_confirmation = (
        checks.get("market_confirmation")
        or professional_context.get("market_confirmation")
        or "UNKNOWN"
    )
    action_label = (
        checks.get("action_label")
        or professional_context.get("aapl", {}).get("regime", {}).get("action_label")
    )

    blocked_reasons = []
    if setup.get("zone_previously_broken") or any("failed/broken" in warning for warning in warnings):
        blocked_reasons.append("zone failed")
    if setup.get("zone_quality_grade") == "WEAK" or any("quality is weak" in warning for warning in warnings):
        blocked_reasons.append("zone weak")
    if action_label == "WAIT_FOR_BREAKOUT":
        blocked_reasons.append("market regime says WAIT_FOR_BREAKOUT")
    if rr_grade == "BAD":
        blocked_reasons.append("risk/reward is BAD")
    if market_confirmation == "UNKNOWN":
        blocked_reasons.append("market confirmation UNKNOWN")

    if setup.get("professional_grade") == "NO_TRADE" or rr_grade == "BAD":
        label = "NO TRADE"
    elif "zone failed" in blocked_reasons or "zone weak" in blocked_reasons:
        label = "RESEARCH CONTEXT"
    elif blocked_reasons or setup.get("confirmation_stage") in {"WATCH", "EARLY_CONFIRM"}:
        label = "WATCH ONLY"
    else:
        label = "ENTRY QUALITY REVIEW"

    return label, list(dict.fromkeys(blocked_reasons))



def grade_confirmation_setups_with_context(confirmation_setups, professional_context):
    """
    Applies strict professional quality grading v2 to each read-only setup.
    """
    if not confirmation_setups:
        return confirmation_setups

    setups = confirmation_setups.get("setups", [])
    if not setups:
        confirmation_setups["best_grade"] = "NO_TRADE"
        confirmation_setups["best_score"] = 0
        confirmation_setups["quality_warnings"] = []
        confirmation_setups["strict_grading"] = True
        confirmation_setups["strict_grading_version"] = 2
        confirmation_setups["read_only"] = True
        return confirmation_setups

    best_score = 0
    best_grade = "NO_TRADE"
    all_warnings = []

    grade_rank = {
        "NO_TRADE": 0,
        "C": 1,
        "B": 2,
        "A": 3,
        "A+": 4,
    }

    for setup in setups:
        strict = strict_trade_quality_grade(setup, professional_context)

        setup["professional_grade"] = strict["grade"]
        setup["professional_score"] = strict["score"]
        setup["quality_warnings"] = strict["warnings"]
        setup["quality_checks"] = strict["checks"]
        label, blocked_reasons = setup_quality_label(setup, professional_context)
        setup["setup_intent"] = label
        setup["setup_label"] = label
        setup["blocked_reasons"] = blocked_reasons
        setup["strict_grading_version"] = 2
        setup["read_only"] = True

        all_warnings.extend(strict["warnings"])

        strict_rank = grade_rank.get(strict["grade"], 0)
        best_rank = grade_rank.get(best_grade, 0)

        if strict_rank > best_rank or (
            strict_rank == best_rank and strict["score"] > best_score
        ):
            best_grade = strict["grade"]
            best_score = strict["score"]

    confirmation_setups["best_grade"] = best_grade
    confirmation_setups["best_score"] = best_score
    confirmation_setups["quality_warnings"] = list(dict.fromkeys(all_warnings))[:12]
    confirmation_setups["strict_grading"] = True
    confirmation_setups["strict_grading_version"] = 2
    confirmation_setups["read_only"] = True

    return confirmation_setups



def build_risk_reward_targets(direction, entry, levels=None, support_resistance=None, supply_demand=None, level_clusters=None):
    levels = levels or {}
    support_resistance = support_resistance or {"support": [], "resistance": []}
    supply_demand = supply_demand or {"demand": [], "supply": []}
    level_clusters = level_clusters or {"clusters": []}

    candidates = []

    def add(price, label, kind, quality_grade=None, quality_score=None):
        if price is None:
            return

        price = round_price(price)
        if price is None:
            return

        if direction == "bullish" and price <= entry:
            return
        if direction == "bearish" and price >= entry:
            return

        candidates.append({
            "price": price,
            "label": label,
            "kind": kind,
            "quality_grade": quality_grade,
            "quality_score": quality_score,
        })

    if direction == "bullish":
        add(levels.get("pmh"), "PMH", "premarket_high")
        add(levels.get("pdh"), "PDH", "previous_day_high")
        add(levels.get("pdc"), "PDC", "previous_day_close")

        for idx, level in enumerate(support_resistance.get("resistance", []) or []):
            add(level.get("price"), f"R{idx + 1}", "resistance", level.get("quality_grade"), level.get("quality_score"))

        for idx, zone in enumerate(supply_demand.get("supply", []) or []):
            add(zone.get("low"), f"Supply {idx + 1}", "supply", zone.get("zone_quality_grade"), zone.get("zone_quality_score"))

        for idx, cluster in enumerate(level_clusters.get("clusters", []) or []):
            if cluster.get("kind") == "upside":
                add(cluster.get("low"), cluster.get("label") or f"Upside Cluster {idx + 1}", "level_cluster")
    elif direction == "bearish":
        add(levels.get("pml"), "PML", "premarket_low")
        add(levels.get("pdl"), "PDL", "previous_day_low")
        add(levels.get("pdc"), "PDC", "previous_day_close")

        for idx, level in enumerate(support_resistance.get("support", []) or []):
            add(level.get("price"), f"S{idx + 1}", "support", level.get("quality_grade"), level.get("quality_score"))

        for idx, zone in enumerate(supply_demand.get("demand", []) or []):
            add(zone.get("high"), f"Demand {idx + 1}", "demand", zone.get("zone_quality_grade"), zone.get("zone_quality_score"))

        for idx, cluster in enumerate(level_clusters.get("clusters", []) or []):
            if cluster.get("kind") == "downside":
                add(cluster.get("high"), cluster.get("label") or f"Downside Cluster {idx + 1}", "level_cluster")

    unique = {}
    for candidate in candidates:
        key = candidate["price"]
        if key not in unique:
            unique[key] = candidate

    target_candidates = list(unique.values())
    preferred_candidates = [
        candidate for candidate in target_candidates
        if candidate.get("kind") not in {"support", "resistance", "supply", "demand"} or candidate.get("quality_grade") != "WEAK"
    ]
    if preferred_candidates:
        target_candidates = preferred_candidates

    return sorted(
        target_candidates,
        key=lambda candidate: candidate["price"],
        reverse=direction == "bearish",
    )


def calculate_setup_risk_reward(
    setup,
    levels=None,
    support_resistance=None,
    supply_demand=None,
    level_clusters=None,
    professional_context=None,
):
    """
    Builds read-only chart guidance for an existing confirmation setup.
    It does not place, size, or manage orders.
    """
    direction = setup.get("direction")
    level_price = setup.get("level_price")
    trigger = setup.get("trigger")
    existing_invalidation = setup.get("invalidation")
    professional_context = professional_context or {}
    atr14 = professional_context.get("aapl", {}).get("atr14")

    entry = trigger if trigger is not None else level_price
    if entry is None or direction not in {"bullish", "bearish"}:
        return {
            "suggested_entry": round_price(entry),
            "invalidation": round_price(existing_invalidation),
            "stop_distance": None,
            "target_1": None,
            "target_2": None,
            "target_3": None,
            "reward_1": None,
            "reward_2": None,
            "reward_3": None,
            "rr_1": None,
            "rr_2": None,
            "rr_3": None,
            "nearest_opposing_level": None,
            "room_to_opposing_level": None,
            "rr_grade": "BAD",
            "rr_warnings": ["no clean target found"],
            "read_only": True,
        }

    entry = round_price(entry)
    buffer = max(0.03, (atr14 or 0) * 0.10)

    if direction == "bullish":
        structural_low = setup.get("level_low")
        structural_invalidation = structural_low - buffer if structural_low is not None else None
        invalidation_candidates = [value for value in [existing_invalidation, structural_invalidation] if value is not None]
        invalidation = min(invalidation_candidates) if invalidation_candidates else entry - max(buffer, (atr14 or 0.10) * 0.50)
        stop_distance = entry - invalidation
    else:
        structural_high = setup.get("level_high")
        structural_invalidation = structural_high + buffer if structural_high is not None else None
        invalidation_candidates = [value for value in [existing_invalidation, structural_invalidation] if value is not None]
        invalidation = max(invalidation_candidates) if invalidation_candidates else entry + max(buffer, (atr14 or 0.10) * 0.50)
        stop_distance = invalidation - entry

    invalidation = round_price(invalidation)
    stop_distance = round_price(stop_distance)

    opposing_levels = build_risk_reward_targets(
        direction,
        entry,
        levels=levels,
        support_resistance=support_resistance,
        supply_demand=supply_demand,
        level_clusters=level_clusters,
    )
    nearest_opposing = opposing_levels[0] if opposing_levels else None
    room_to_opposing = (
        abs(nearest_opposing["price"] - entry)
        if nearest_opposing is not None
        else None
    )

    targets = list(opposing_levels[:3])
    if stop_distance is not None and stop_distance > 0:
        for multiple in [1, 2, 3]:
            if len(targets) >= 3:
                break
            price = entry + stop_distance * multiple if direction == "bullish" else entry - stop_distance * multiple
            price = round_price(price)
            if not any(abs(target["price"] - price) < 0.01 for target in targets):
                targets.append({
                    "price": price,
                    "label": f"{multiple}R fallback",
                    "kind": "risk_fallback",
                })

        targets = sorted(
            targets,
            key=lambda target: target["price"],
            reverse=direction == "bearish",
        )[:3]

    target_values = [target["price"] for target in targets]
    rewards = [
        round_price(abs(target - entry))
        for target in target_values
    ]
    rr_values = [
        round(reward / stop_distance, 2) if stop_distance and stop_distance > 0 else None
        for reward in rewards
    ]

    while len(target_values) < 3:
        target_values.append(None)
        rewards.append(None)
        rr_values.append(None)

    warnings = []
    min_reasonable_stop = max(0.03, (atr14 or 0) * 0.15)
    max_reasonable_stop = max(0.75, (atr14 or 0) * 2.50)

    if stop_distance is None or stop_distance <= min_reasonable_stop:
        warnings.append("stop distance too small")
    if stop_distance is not None and stop_distance > max_reasonable_stop:
        warnings.append("stop distance too wide")
    if rr_values[0] is not None and rr_values[0] < 1.0:
        warnings.append("target too close")
        warnings.append("risk/reward below 1.0")
    if room_to_opposing is not None and stop_distance and room_to_opposing < stop_distance:
        warnings.append("opposing level too close")
    if not opposing_levels:
        warnings.append("no clean target found")
    if targets and targets[0].get("quality_grade") == "WEAK":
        if targets[0].get("kind") in {"supply", "demand"}:
            warnings.append("Target zone is weak.")
            warnings.append("Opposing zone is weak.")
        else:
            warnings.append("Target level is weak.")
    if setup.get("professional_grade") == "NO_TRADE":
        warnings.append("setup is NO_TRADE")
    if setup.get("confirmation_stage", setup.get("status")) != "CONFIRMED":
        warnings.append("setup is not CONFIRMED yet")
    if professional_context.get("aapl", {}).get("regime", {}).get("regime") == "CHOP":
        warnings.append("market is CHOP")
    regime_action = professional_context.get("aapl", {}).get("regime", {}).get("action_label")
    if regime_action == "NO_NEW_TRADES":
        warnings.append("regime action is NO_NEW_TRADES")
    elif regime_action == "WAIT_FOR_BREAKOUT":
        warnings.append("regime action is WAIT_FOR_BREAKOUT")
    market_confirmation_value = professional_context.get("market_confirmation", {}).get(
        "market_confirmation",
        professional_context.get("market_alignment"),
    )
    if direction == "bullish" and market_confirmation_value == "BEARISH":
        warnings.append("market confirmation conflicts with bullish setup")
    elif direction == "bearish" and market_confirmation_value == "BULLISH":
        warnings.append("market confirmation conflicts with bearish setup")
    elif market_confirmation_value in {"MIXED", "UNKNOWN"}:
        warnings.append(f"market confirmation is {market_confirmation_value}")

    rr_1 = rr_values[0]
    rr_2 = rr_values[1]
    stop_too_small = "stop distance too small" in warnings
    stop_too_wide = "stop distance too wide" in warnings
    target_unavailable = target_values[0] is None
    opposing_too_close = "opposing level too close" in warnings

    if stop_too_wide or target_unavailable or rr_1 is None or rr_1 < 1.0:
        rr_grade = "BAD"
    elif stop_too_small or opposing_too_close or rr_2 is None:
        rr_grade = "WEAK"
    elif rr_2 >= 2.0:
        rr_grade = "GOOD"
    elif rr_1 >= 1.0 and rr_2 >= 1.5:
        rr_grade = "OK"
    else:
        rr_grade = "WEAK"

    return {
        "suggested_entry": entry,
        "invalidation": invalidation,
        "stop_distance": stop_distance,
        "target_1": target_values[0],
        "target_2": target_values[1],
        "target_3": target_values[2],
        "reward_1": rewards[0],
        "reward_2": rewards[1],
        "reward_3": rewards[2],
        "rr_1": rr_values[0],
        "rr_2": rr_values[1],
        "rr_3": rr_values[2],
        "nearest_opposing_level": nearest_opposing,
        "room_to_opposing_level": round_price(room_to_opposing),
        "rr_grade": rr_grade,
        "rr_warnings": list(dict.fromkeys(warnings)),
        "read_only": True,
    }


def enrich_confirmation_setups_with_risk_reward(
    confirmation_setups,
    levels=None,
    support_resistance=None,
    supply_demand=None,
    level_clusters=None,
    professional_context=None,
):
    if not confirmation_setups:
        return confirmation_setups

    for setup in confirmation_setups.get("setups", []):
        setup["risk_reward"] = calculate_setup_risk_reward(
            setup,
            levels=levels,
            support_resistance=support_resistance,
            supply_demand=supply_demand,
            level_clusters=level_clusters,
            professional_context=professional_context,
        )

    confirmation_setups["risk_reward_enabled"] = True
    confirmation_setups["read_only"] = True
    return confirmation_setups


def finalize_confirmation_setup_stages(confirmation_setups, professional_context=None):
    """Finalize read-only v2 stages after market context and risk/reward exist."""
    if not confirmation_setups:
        return confirmation_setups

    professional_context = professional_context or {}
    regime = professional_context.get("aapl", {}).get("regime", {}).get("regime")
    action_label = professional_context.get("aapl", {}).get("regime", {}).get("action_label")
    status_map = {
        "WATCH": "WATCH",
        "EARLY_CONFIRM": "WATCH",
        "CONFIRMED": "CONFIRMED",
        "FAILED": "INVALIDATED",
    }

    for setup in confirmation_setups.get("setups", []):
        reclaim_confirmed = bool(setup.get("reclaim_confirmed"))
        candle_closed_confirmed = bool(setup.get("candle_closed_confirmed"))
        next_candle_confirmed = bool(setup.get("next_candle_confirmed"))
        structure_confirmed = bool(setup.get("structure_confirmed"))
        volume_confirmed = bool(setup.get("volume_confirmed"))
        trend_confirmed = bool(setup.get("trend_confirmed"))
        rr_grade = setup.get("risk_reward", {}).get("rr_grade")
        failed = setup.get("status") == "INVALIDATED" or setup.get("confirmation_stage") == "FAILED"
        context_allowed = regime != "CHOP" and action_label != "NO_NEW_TRADES"
        rr_allowed = rr_grade != "BAD"
        weak_zone = setup.get("zone_quality_grade") == "WEAK"
        broken_zone = bool(setup.get("zone_previously_broken"))
        existing_warnings = setup.get("confirmation_warnings") or []

        if failed:
            stage = "FAILED"
        elif (
            reclaim_confirmed
            and candle_closed_confirmed
            and next_candle_confirmed
            and structure_confirmed
            and volume_confirmed
            and trend_confirmed
            and context_allowed
            and rr_allowed
            and not weak_zone
            and not broken_zone
        ):
            stage = "CONFIRMED"
        elif reclaim_confirmed and not weak_zone:
            stage = "EARLY_CONFIRM"
        else:
            stage = "WATCH"

        reasons = []
        warnings = list(existing_warnings)
        score = 0
        checks = [
            (reclaim_confirmed, 25, "Level reclaim/rejection detected.", "Waiting for reclaim/rejection."),
            (candle_closed_confirmed, 15, "Confirmation candle closed.", "Confirmation candle may still be forming."),
            (next_candle_confirmed, 15, "Next candle held the level.", "Waiting for next-candle confirmation."),
            (structure_confirmed, 15, "Price structure confirmed.", "Structure not confirmed."),
            (volume_confirmed, 10, "Volume confirmed.", "Volume not confirmed."),
            (trend_confirmed, 10, "Trend filter confirmed.", "Trend filter not confirmed."),
            (context_allowed, 5, "Market regime allows confirmation.", "Market regime blocks final confirmation."),
            (rr_allowed, 5, "Risk/reward allows confirmation.", "Risk/reward is BAD."),
        ]
        for passed, points, reason, warning in checks:
            if passed:
                score += points
                reasons.append(reason)
            else:
                warnings.append(warning)

        if failed:
            score = 0
            warnings.append("Setup failed its invalidation.")
        if weak_zone:
            warnings.append("Weak zone: confirmation capped at WATCH.")
            score = min(score, 35)
        if broken_zone:
            warnings.append("Zone previously failed/broken.")
            score = max(0, score - 15)

        setup["confirmation_stage"] = stage
        setup["confirmation_score"] = max(0, min(100, int(score)))
        setup["confirmation_reasons"] = list(dict.fromkeys(reasons))
        setup["confirmation_warnings"] = list(dict.fromkeys(warnings))
        setup["status"] = status_map[stage]
        setup["read_only"] = True

        risk_reward = setup.setdefault("risk_reward", {"read_only": True})
        rr_warnings = risk_reward.setdefault("rr_warnings", [])
        not_confirmed_warning = "setup is not CONFIRMED yet"
        if stage == "CONFIRMED":
            risk_reward["rr_warnings"] = [
                warning for warning in rr_warnings if warning != not_confirmed_warning
            ]
        elif not_confirmed_warning not in rr_warnings:
            rr_warnings.append(not_confirmed_warning)

    setups = confirmation_setups.get("setups", [])
    if any(setup.get("confirmation_stage") == "CONFIRMED" for setup in setups):
        confirmation_setups["status"] = "CONFIRMED"
    elif any(setup.get("confirmation_stage") in {"WATCH", "EARLY_CONFIRM"} for setup in setups):
        confirmation_setups["status"] = "WATCH"
    elif any(setup.get("confirmation_stage") == "FAILED" for setup in setups):
        confirmation_setups["status"] = "INVALIDATED"
    else:
        confirmation_setups["status"] = "NO_SETUP"

    confirmation_setups["confirmation_stages_v2"] = True
    confirmation_setups["read_only"] = True
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

    def add(side, price, name, kind, low=None, high=None, confidence=None, quality_score=None, quality_grade=None, quality_reasons=None, reaction=None):
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
            "quality_score": quality_score,
            "quality_grade": quality_grade,
            "quality_reasons": quality_reasons or [],
            "zone_reaction": reaction,
            "zone_failed": (reaction or {}).get("reaction_status") == "FAILED",
            "zone_previously_broken": (
                (reaction or {}).get("reaction_status") == "FAILED"
                or any(
                    "broken" in str(reason).lower() or "failed" in str(reason).lower()
                    for reason in (quality_reasons or [])
                )
            ),
        })

    add("upside", levels.get("pmh"), "PMH", "premarket_high")
    add("downside", levels.get("pml"), "PML", "premarket_low")
    add("upside", levels.get("pdh"), "PDH", "previous_day_high")
    add("downside", levels.get("pdl"), "PDL", "previous_day_low")

    for idx, r in enumerate(support_resistance.get("resistance", []) or []):
        add(
            "upside", r.get("price"), f"R{idx + 1}", "resistance", confidence=r.get("reliability_label"),
            quality_score=r.get("quality_score"), quality_grade=r.get("quality_grade"), quality_reasons=r.get("quality_reasons"),
        )

    for idx, s in enumerate(support_resistance.get("support", []) or []):
        add(
            "downside", s.get("price"), f"S{idx + 1}", "support", confidence=s.get("reliability_label"),
            quality_score=s.get("quality_score"), quality_grade=s.get("quality_grade"), quality_reasons=s.get("quality_reasons"),
        )

    for idx, z in enumerate(supply_demand.get("supply", []) or []):
        add(
            "upside", z.get("high"), f"Supply {idx + 1}", "supply", low=z.get("low"), high=z.get("high"), confidence=z.get("label"),
            quality_score=z.get("zone_quality_score"), quality_grade=z.get("zone_quality_grade"), quality_reasons=z.get("zone_quality_reasons"),
            reaction={key: z.get(key) for key in [
                "reaction_status", "reaction_label", "defended_edge", "failure_edge",
                "closes_holding_edge", "touches_count", "last_reaction_time",
                "reaction_confidence", "reaction_warnings", "read_only",
            ]},
        )

    for idx, z in enumerate(supply_demand.get("demand", []) or []):
        add(
            "downside", z.get("low"), f"Demand {idx + 1}", "demand", low=z.get("low"), high=z.get("high"), confidence=z.get("label"),
            quality_score=z.get("zone_quality_score"), quality_grade=z.get("zone_quality_grade"), quality_reasons=z.get("zone_quality_reasons"),
            reaction={key: z.get(key) for key in [
                "reaction_status", "reaction_label", "defended_edge", "failure_edge",
                "closes_holding_edge", "touches_count", "last_reaction_time",
                "reaction_confidence", "reaction_warnings", "read_only",
            ]},
        )

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

            kind = level.get("kind")
            zone_reaction = level.get("zone_reaction") or {}
            zone_quality_grade = level.get("quality_grade") if kind in {"supply", "demand"} else None
            zone_failed_same_direction = (
                (kind == "demand" and direction == "bullish" and zone_reaction.get("reaction_status") == "FAILED")
                or (kind == "supply" and direction == "bearish" and zone_reaction.get("reaction_status") == "FAILED")
            )
            if zone_failed_same_direction:
                continue

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
                next_candle_confirmed = next_candle is not None and (
                    next_candle["close"] > price
                    and (next_candle["low"] >= price - tolerance or next_candle["low"] > candle["low"])
                )
                trend_confirmed = trend["bullish"]
                invalidated = candle["close"] < price - tolerance
                trigger = round_price(candle["high"])
                invalidation = round_price(price - tolerance)
            else:
                structure_confirmed = lower_high or breaks_trigger_low
                next_candle_confirmed = next_candle is not None and (
                    next_candle["close"] < price
                    and (next_candle["high"] <= price + tolerance or next_candle["high"] < candle["high"])
                )
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

            candle_closed_confirmed = next_candle is not None
            if invalidated:
                confirmation_stage = "FAILED"
            elif reclaim_confirmed and candle_closed_confirmed and next_candle_confirmed and volume_confirmed and structure_confirmed and trend_confirmed:
                confirmation_stage = "CONFIRMED"
            elif reclaim_confirmed:
                confirmation_stage = "EARLY_CONFIRM"
            else:
                confirmation_stage = "WATCH"

            zone_setup_warnings = []
            if kind in {"supply", "demand"}:
                if zone_quality_grade == "WEAK" and confirmation_stage in {"EARLY_CONFIRM", "CONFIRMED"}:
                    confirmation_stage = "WATCH"
                    score = min(score, 35)
                    zone_setup_warnings.append("Weak zone: confirmation capped at WATCH.")
                if level.get("zone_previously_broken"):
                    score = max(0, score - 15)
                    zone_setup_warnings.append("Zone previously failed/broken.")

            status = {
                "WATCH": "WATCH",
                "EARLY_CONFIRM": "WATCH",
                "CONFIRMED": "CONFIRMED",
                "FAILED": "INVALIDATED",
            }[confirmation_stage]

            setups.append({
                "status": status,
                "confirmation_stage": confirmation_stage,
                "confirmation_score": score,
                "confirmation_reasons": [],
                "confirmation_warnings": zone_setup_warnings,
                "candle_closed_confirmed": bool(candle_closed_confirmed),
                "next_candle_confirmed": bool(next_candle_confirmed),
                "direction": direction,
                "source": level.get("name"),
                "kind": kind,
                "level_price": round_price(price),
                "level_low": level.get("low"),
                "level_high": level.get("high"),
                "level_quality_score": level.get("quality_score"),
                "level_quality_grade": level.get("quality_grade"),
                "level_quality_reasons": level.get("quality_reasons", []),
                "zone_quality_score": level.get("quality_score") if kind in {"supply", "demand"} else None,
                "zone_quality_grade": zone_quality_grade,
                "zone_quality_reasons": level.get("quality_reasons", []) if kind in {"supply", "demand"} else [],
                "zone_reaction": zone_reaction if kind in {"supply", "demand"} else None,
                "zone_previously_broken": bool(level.get("zone_previously_broken")) if kind in {"supply", "demand"} else False,
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
            "labels": ["WATCH", "EARLY_CONFIRM", "CONFIRMED", "FAILED"],
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


def candle_direction(candle):
    if not candle:
        return "unknown"
    if candle.get("close") > candle.get("open"):
        return "bullish"
    if candle.get("close") < candle.get("open"):
        return "bearish"
    return "neutral"


def build_chart_line_registry(snapshot, symbol, timeframe):
    snapshot = snapshot or {}
    symbol = normalize_symbol(symbol)
    current_price = snapshot.get("current_price")
    chart_lines = []
    warnings = []

    def numeric(value):
        return isinstance(value, (int, float)) and math.isfinite(value)

    def line_side(anchor, top=None, bottom=None):
        if not numeric(current_price) or not numeric(anchor):
            return "unknown"
        if numeric(top) and numeric(bottom) and bottom <= current_price <= top:
            return "overlapping"
        if anchor > current_price:
            return "above"
        if anchor < current_price:
            return "below"
        return "overlapping"

    def structure_audit_details(anchor, top=None, bottom=None):
        if not numeric(anchor) and numeric(top) and numeric(bottom):
            anchor = (top + bottom) / 2
        return {
            "selected_as_nearest_clean_mode": False,
            "distance_from_current_price": round_price(abs(anchor - current_price)) if numeric(anchor) and numeric(current_price) else None,
            "side": line_side(anchor, top=top, bottom=bottom),
            "hidden_reason": None,
        }

    def add(
        line_type,
        label,
        short_label,
        source,
        calculation_method,
        reason,
        price=None,
        top=None,
        bottom=None,
        defended_edge=None,
        failure_edge=None,
        start_time=None,
        end_time=None,
        confidence=None,
        strength="UNKNOWN",
        status="ACTIVE",
        priority=2,
        clean=True,
        full=True,
        item_warnings=None,
        extra_details=None,
    ):
        values = [value for value in [price, top, bottom] if numeric(value)]
        if not values:
            warnings.append(f"{label}: missing numeric price or range.")
            return
        if top is not None and bottom is not None and numeric(top) and numeric(bottom) and top < bottom:
            top, bottom = bottom, top
        anchor = price if numeric(price) else ((top + bottom) / 2 if numeric(top) and numeric(bottom) else values[0])
        slug = re.sub(r"[^a-z0-9]+", "-", f"{symbol}-{timeframe}-{line_type}-{label}-{anchor}".lower()).strip("-")
        chart_lines.append({
            "id": slug,
            "symbol": symbol,
            "timeframe": timeframe,
            "type": line_type,
            "label": label,
            "short_label": short_label,
            "price": round_price(price) if numeric(price) else None,
            "top": round_price(top) if numeric(top) else None,
            "bottom": round_price(bottom) if numeric(bottom) else None,
            "defended_edge": round_price(defended_edge) if numeric(defended_edge) else None,
            "failure_edge": round_price(failure_edge) if numeric(failure_edge) else None,
            "start_time": start_time,
            "end_time": end_time,
            "source": source,
            "calculation_method": calculation_method,
            "reason": reason,
            "confidence": int(max(0, min(100, confidence))) if numeric(confidence) else None,
            "strength": strength if strength in {"A", "B", "C", "WEAK", "UNKNOWN"} else "UNKNOWN",
            "status": status if status in {"ACTIVE", "PARTIALLY_FILLED", "FILLED", "INVALID", "TESTED", "HOLDING", "FAILED", "MUTED", "UNKNOWN"} else "UNKNOWN",
            "priority": priority if priority in {1, 2, 3} else 3,
            "visible_in_clean_mode": bool(clean),
            "visible_in_full_mode": bool(full),
            "warnings": list(item_warnings or []),
            "extra_details": dict(extra_details or {}),
            "read_only": True,
        })

    levels = snapshot.get("levels") or {}
    level_specs = [
        ("pmh", "PMH", "premarket_high_low", "Maximum high from selected-day premarket candles", "Selected-day premarket high", True),
        ("pml", "PML", "premarket_high_low", "Minimum low from selected-day premarket candles", "Selected-day premarket low", True),
        ("pdh", "PDH", "previous_regular_session", "Maximum high from previous regular session", "Previous regular-session high", True),
        ("pdl", "PDL", "previous_regular_session", "Minimum low from previous regular session", "Previous regular-session low", True),
        ("pdc", "PDC", "previous_regular_session", "Final close from previous regular session", "Previous regular-session close", False),
        ("hod", "HOD", "regular_session", "Maximum high from selected-day regular-session validated candles", "High of day", True),
        ("lod", "LOD", "regular_session", "Minimum low from selected-day regular-session validated candles", "Low of day", True),
        ("opening_5m_high", "OPEN 5M HIGH", "opening_range", "High from completed 09:30-09:35 ET validated 1Min candles", "First 5-minute candle high", True),
        ("opening_5m_low", "OPEN 5M LOW", "opening_range", "Low from completed 09:30-09:35 ET validated 1Min candles", "First 5-minute candle low", True),
    ]
    for key, label, source, method, reason, clean in level_specs:
        add(label, label, label, source, method, reason, price=levels.get(key), priority=1, clean=clean)

    indicators = snapshot.get("indicators") or {}
    for key, line_type, label, period in [
        ("vwap", "VWAP", "VWAP", None),
        ("ema9", "EMA9", "EMA 9", 9),
        ("ema20", "EMA20", "EMA 20", 20),
    ]:
        value = latest_indicator_value(indicators.get(key))
        method = "Session cumulative volume-weighted average price" if key == "vwap" else f"Exponential moving average over {period} candles"
        add(line_type, label, label, "session_vwap" if key == "vwap" else "ema_calculation", method, f"Latest {label} value", price=value, priority=1)

    support_resistance = snapshot.get("support_resistance") or {}
    for side, line_type, short in [("support", "SUPPORT", "Support"), ("resistance", "RESISTANCE", "Resistance")]:
        for item in support_resistance.get(side, []) or []:
            grade = item.get("quality_grade") or "UNKNOWN"
            score = item.get("quality_score")
            details = structure_audit_details(item.get("price"))
            details["hidden_reason"] = None if grade in {"A", "B"} else "weak and not nearest unless selected by Clean Mode"
            add(
                line_type, f"{short} {grade}", short, "swing_detection",
                "Weighted level quality from touches, reaction, freshness, confluence, distance, and cleanliness",
                "; ".join(item.get("quality_reasons") or []) or f"Detected {side} swing level",
                price=item.get("price"), confidence=score, strength=grade,
                status="MUTED" if grade == "WEAK" else "ACTIVE",
                priority=2 if grade in {"A", "B"} else 3, clean=grade in {"A", "B"},
                item_warnings=item.get("warnings") or [],
                extra_details=details,
            )

    supply_demand = snapshot.get("supply_demand") or {}
    for side, zone_type, short in [("demand", "DEMAND_ZONE", "Demand"), ("supply", "SUPPLY_ZONE", "Supply")]:
        for zone in supply_demand.get(side, []) or []:
            grade = zone.get("zone_quality_grade") or "UNKNOWN"
            reaction = zone.get("reaction_status")
            failed = reaction == "FAILED"
            status = "FAILED" if failed else "HOLDING" if reaction in {"HOLD", "RECLAIM", "REJECTION"} else "ACTIVE"
            zone_anchor = ((zone.get("high") or 0) + (zone.get("low") or 0)) / 2 if numeric(zone.get("high")) and numeric(zone.get("low")) else None
            details = structure_audit_details(zone_anchor, top=zone.get("high"), bottom=zone.get("low"))
            details["hidden_reason"] = "failed zone not retesting" if failed else None if grade in {"A", "B"} else "weak and not nearest unless selected by Clean Mode"
            add(
                zone_type, f"{short} Zone {grade}", short, "supply_demand_engine",
                "Deterministic supply/demand zone detection and quality scoring",
                "; ".join(zone.get("zone_quality_reasons") or []) or f"Detected {side} zone",
                top=zone.get("high"), bottom=zone.get("low"), start_time=zone.get("time"),
                defended_edge=zone.get("defended_edge"), failure_edge=zone.get("failure_edge"),
                confidence=zone.get("zone_quality_score"), strength=grade, status=status,
                priority=2 if grade in {"A", "B"} else 3,
                clean=not failed and grade in {"A", "B"} and reaction in {"HOLD", "RECLAIM", "REJECTION"},
                item_warnings=zone.get("reaction_warnings") or [],
                extra_details=details,
            )
            if reaction in {"HOLD", "RECLAIM", "REJECTION", "FAILED"}:
                reaction_type = f"{side.upper()}_{reaction}"
                reaction_label = zone.get("reaction_label") or reaction_type.replace("_", " ").title()
                add(
                    reaction_type, reaction_label, reaction_label.title().replace("Rejection", "Reject"),
                    "zone_reaction_engine", "Zone touch, close, defended-edge, and failure-edge evaluation",
                    f"{zone.get('touches_count', 0)} touches; {zone.get('closes_holding_edge', 0)} closes holding defended edge",
                    price=zone.get("defended_edge") if not failed else zone.get("failure_edge"),
                    top=zone.get("high"), bottom=zone.get("low"), start_time=zone.get("last_reaction_time"),
                    defended_edge=zone.get("defended_edge"), failure_edge=zone.get("failure_edge"),
                    confidence=zone.get("reaction_confidence"), strength=grade,
                    status="FAILED" if failed else "HOLDING", priority=1 if not failed else 3,
                    clean=not failed, item_warnings=zone.get("reaction_warnings") or [],
                )
            for field, suffix, line_type in [
                ("trigger", "Trigger", "ZONE_TRIGGER"),
                ("invalidation", "Invalidation", "ZONE_INVALIDATION"),
            ]:
                if numeric(zone.get(field)):
                    add(
                        line_type, f"{short} {suffix}", f"{short} {suffix}",
                        "supply_demand_engine", f"Deterministic {side} zone {field}",
                        f"{short} zone {field} derived from zone boundaries",
                        price=zone.get(field), confidence=zone.get("zone_quality_score"), strength=grade,
                        status="MUTED" if failed else "ACTIVE", priority=3, clean=False,
            )

    fair_value_gaps = snapshot.get("fair_value_gaps") or {}
    for gap in (fair_value_gaps.get("bullish") or []) + (fair_value_gaps.get("bearish") or []):
        gap_type = gap.get("type")
        label = "BULL FVG" if gap_type == "BULLISH_FVG" else "BEAR FVG"
        status = gap.get("status") or "UNKNOWN"
        grade = gap.get("quality_grade") or "UNKNOWN"
        active = status in {"ACTIVE", "PARTIALLY_FILLED"}
        add(
            gap_type or "FVG",
            f"{label} {grade}",
            label,
            gap.get("source") or "fvg_engine",
            "Strict three-candle imbalance using validated/rebuilt candles only",
            gap.get("reason") or f"{status} {label} from candle1/candle3 gap",
            price=gap.get("midpoint"),
            top=gap.get("top"),
            bottom=gap.get("bottom"),
            start_time=gap.get("candle3_time") or gap.get("created_at"),
            end_time=gap.get("last_touch_time"),
            confidence=gap.get("quality_score"),
            strength=grade,
            status=status,
            priority=1 if active and grade in {"A", "B"} else 2 if active and grade == "C" else 3,
            clean=bool(gap.get("visible_in_clean_mode")),
            item_warnings=gap.get("warnings") or [],
            extra_details={
                "direction": gap.get("direction"),
                "candle1_time": gap.get("candle1_time"),
                "candle2_time": gap.get("candle2_time"),
                "candle3_time": gap.get("candle3_time"),
                "candle1_open": gap.get("candle1_open"),
                "candle1_high": gap.get("candle1_high"),
                "candle1_low": gap.get("candle1_low"),
                "candle1_close": gap.get("candle1_close"),
                "candle2_open": gap.get("candle2_open"),
                "candle2_high": gap.get("candle2_high"),
                "candle2_low": gap.get("candle2_low"),
                "candle2_close": gap.get("candle2_close"),
                "candle3_open": gap.get("candle3_open"),
                "candle3_high": gap.get("candle3_high"),
                "candle3_low": gap.get("candle3_low"),
                "candle3_close": gap.get("candle3_close"),
                "rule_passed": gap.get("rule_passed"),
                "top": gap.get("top"),
                "bottom": gap.get("bottom"),
                "midpoint": gap.get("midpoint"),
                "gap_size": gap.get("gap_size"),
                "fill_percentage": gap.get("fill_percentage"),
                "displacement_score": gap.get("displacement_score"),
                "engulfing_score": gap.get("engulfing_score"),
                "middle_candle_body_percent": gap.get("middle_candle_body_percent"),
                "middle_candle_engulfed_c1": gap.get("middle_candle_engulfed_c1"),
                "middle_candle_closed_beyond_c1": gap.get("middle_candle_closed_beyond_c1"),
                "is_chop_context": gap.get("is_chop_context"),
                "visible_in_clean_mode": gap.get("visible_in_clean_mode"),
                "clean_mode_hidden_reason": gap.get("clean_mode_hidden_reason"),
                "hidden_reason": gap.get("hidden_reason"),
                "price_interacting_now": gap.get("price_interacting_now"),
                "has_key_confluence": gap.get("has_key_confluence"),
                "selected_as_primary_clean_mode_fvg": gap.get("selected_as_primary_clean_mode_fvg"),
                "quality_grade": gap.get("quality_grade"),
                "quality_reason": "; ".join(gap.get("warnings") or []) or gap.get("reason"),
                "source": gap.get("source"),
                "reason": gap.get("reason"),
            },
        )

    sweeps = snapshot.get("liquidity_sweeps") or {}
    for side in ["upside", "downside"]:
        for sweep in sweeps.get(side, []) or []:
            add(
                "LIQUIDITY_SWEEP", f"{side.title()} Liquidity Sweep", "Sweep",
                "liquidity_sweep_engine", "Watch area around deterministic level or zone boundary",
                sweep.get("source") or f"{side} liquidity sweep watch",
                price=sweep.get("price"), top=sweep.get("high"), bottom=sweep.get("low"),
                confidence=60, status="TESTED", priority=3, clean=False,
                item_warnings=[sweeps.get("note")] if sweeps.get("note") else [],
            )

    structure_reactions = snapshot.get("structure_reactions") or {}
    for side, short in [("support_watch", "Support Watch"), ("resistance_watch", "Resistance Watch")]:
        for reaction in structure_reactions.get(side, []) or []:
            add(
                "STRUCTURE_REACTION", short, short, "structure_reaction_engine",
                "Recent wick, touch, reaction, and follow-through watch-area detection",
                reaction.get("reason") or short,
                top=reaction.get("high"), bottom=reaction.get("low"), start_time=reaction.get("time"),
                confidence=reaction.get("score"), status="TESTED", priority=3, clean=False,
            )

    for cluster in (snapshot.get("level_clusters") or {}).get("clusters", []) or []:
        add(
            "LEVEL_CLUSTER", cluster.get("label") or "Level Cluster", "Cluster", "level_cluster_engine",
            "Merged nearby deterministic levels, zones, and liquidity watch areas",
            cluster.get("cluster_type") or "Multiple nearby chart references",
            top=cluster.get("high"), bottom=cluster.get("low"), confidence=cluster.get("score"),
            status="ACTIVE", priority=3, clean=False,
        )

    for setup in (snapshot.get("confirmation_setups") or {}).get("setups", []) or []:
        direction = setup.get("direction")
        stage = setup.get("confirmation_stage") or setup.get("status") or "UNKNOWN"
        intent = setup.get("setup_intent") or setup.get("setup_label")
        short = "Breakout Trigger" if direction == "bullish" else "Breakdown Trigger"
        add(
            "CONFIRMATION_TRIGGER", f"{intent + ' · ' if intent else ''}{short} · {stage}", short, "confirmation_setup_engine",
            "Confirmation setup trigger/reclaim/rejection level",
            "; ".join(setup.get("confirmation_reasons") or []) or f"{stage} {direction or ''} confirmation setup",
            price=setup.get("trigger") if numeric(setup.get("trigger")) else setup.get("level_price"),
            start_time=setup.get("candle_time"), confidence=setup.get("confirmation_score"),
            strength=setup.get("professional_grade") if setup.get("professional_grade") in {"A", "B", "C"} else "UNKNOWN",
            status="FAILED" if stage == "FAILED" or setup.get("status") == "INVALIDATED" else "ACTIVE",
            priority=1 if stage == "CONFIRMED" else 2, clean=stage == "CONFIRMED",
            item_warnings=setup.get("confirmation_warnings") or [],
        )
        if numeric(setup.get("invalidation")):
            add(
                "CONFIRMATION_INVALIDATION", f"{short} Invalidation", "Setup Invalid",
                "confirmation_setup_engine", "Confirmation setup invalidation level",
                "Price beyond this level invalidates the confirmation setup.",
                price=setup.get("invalidation"), start_time=setup.get("candle_time"),
                confidence=setup.get("confirmation_score"), status="ACTIVE", priority=3, clean=False,
                item_warnings=setup.get("confirmation_warnings") or [],
            )

    deduplicated = {}
    for line in chart_lines:
        key = (line["label"], line["price"], line["top"], line["bottom"])
        existing = deduplicated.get(key)
        if not existing or (line["priority"], -(line["confidence"] or 0)) < (existing["priority"], -(existing["confidence"] or 0)):
            deduplicated[key] = line
        if line["type"] == "UNKNOWN":
            warnings.append(f"{line['label']}: unknown type.")
    chart_lines = list(deduplicated.values())

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "line_count": len(chart_lines),
        "chart_lines": chart_lines,
        "warnings": list(dict.fromkeys(warnings)),
        "read_only": True,
    }


def price_relation(price, reference):
    if price is None or reference is None:
        return "unknown"
    if price > reference:
        return "above"
    if price < reference:
        return "below"
    return "at"


def compact_risk_reward(risk_reward):
    if not risk_reward:
        return None
    keys = [
        "suggested_entry",
        "invalidation",
        "stop_distance",
        "target_1",
        "target_2",
        "target_3",
        "rr_1",
        "rr_2",
        "rr_3",
        "nearest_opposing_level",
        "room_to_opposing_level",
        "rr_grade",
        "rr_warnings",
        "read_only",
    ]
    return {key: risk_reward.get(key) for key in keys}


def compact_ai_setup(setup, timeframe=None):
    if not setup:
        return None
    keys = [
        "direction",
        "source",
        "kind",
        "level_price",
        "trigger",
        "invalidation",
        "professional_grade",
        "professional_score",
        "confirmation_stage",
        "confirmation_score",
        "status",
        "setup_intent",
        "setup_label",
        "blocked_reasons",
        "volume_ratio",
        "quality_warnings",
        "quality_checks",
        "confirmation_warnings",
        "confirmation_reasons",
        "candle_time",
        "read_only",
        "zone_quality_grade",
        "zone_previously_broken",
        "zone_reaction",
    ]
    compact = {key: setup.get(key) for key in keys}
    compact["timeframe"] = timeframe
    compact["risk_reward"] = compact_risk_reward(setup.get("risk_reward"))
    return compact


def nearest_price_item(items, current_price, price_keys):
    if current_price is None:
        return None

    candidates = []
    for item in items or []:
        prices = [item.get(key) for key in price_keys if item.get(key) is not None]
        if not prices:
            continue
        nearest_price = min(prices, key=lambda value: abs(value - current_price))
        candidates.append((abs(nearest_price - current_price), nearest_price, item))

    if not candidates:
        return None

    distance, nearest_price, item = min(candidates, key=lambda candidate: candidate[0])
    return {
        "price": round_price(nearest_price),
        "distance": round_price(distance),
        "quality_grade": item.get("quality_grade") or item.get("zone_quality_grade"),
        "quality_score": item.get("quality_score") or item.get("zone_quality_score"),
        "read_only": True,
    }


def select_best_setup_from_timeframe(timeframe, confirmation_setups):
    setups = confirmation_setups.get("setups", []) if confirmation_setups else []
    if not setups:
        return None

    grade_rank = {"NO_TRADE": 0, "C": 1, "B": 2, "A": 3, "A+": 4}
    stage_rank = {"FAILED": 0, "WATCH": 1, "EARLY_CONFIRM": 2, "CONFIRMED": 3}
    rr_rank = {"BAD": 0, "WEAK": 1, "OK": 2, "GOOD": 3}

    return max(
        setups,
        key=lambda setup: (
            grade_rank.get(setup.get("professional_grade"), 0),
            stage_rank.get(setup.get("confirmation_stage"), 0),
            rr_rank.get((setup.get("risk_reward") or {}).get("rr_grade"), 0),
            setup.get("professional_score") or 0,
        ),
    )


def select_best_ai_setup(timeframe_contexts):
    grade_rank = {"NO_TRADE": 0, "C": 1, "B": 2, "A": 3, "A+": 4}
    stage_rank = {"FAILED": 0, "WATCH": 1, "EARLY_CONFIRM": 2, "CONFIRMED": 3}
    rr_rank = {"BAD": 0, "WEAK": 1, "OK": 2, "GOOD": 3}
    timeframe_rank = {"1Min": 1, "15Min": 2, "5Min": 3}
    candidates = []

    for timeframe in ["1Min", "5Min", "15Min"]:
        setup = (timeframe_contexts.get(timeframe) or {}).get("best_setup")
        if setup:
            candidates.append(setup)

    if not candidates:
        return None

    return max(
        candidates,
        key=lambda setup: (
            grade_rank.get(setup.get("professional_grade"), 0),
            stage_rank.get(setup.get("confirmation_stage"), 0),
            rr_rank.get((setup.get("risk_reward") or {}).get("rr_grade"), 0),
            timeframe_rank.get(setup.get("timeframe"), 0),
            setup.get("professional_score") or 0,
        ),
    )


def build_ai_volume_context(candles, best_setup=None):
    candles = candles or []
    unknown = {
        "latest_volume": None,
        "previous_volume": None,
        "average_volume_20": None,
        "relative_volume_20": None,
        "rvol_20": None,
        "volume_trend": "unknown",
        "volume_strength": "unknown",
        "volume_spike": False,
        "low_volume_warning": False,
        "breakout_volume_confirmed": None,
        "read_only": True,
    }
    if not candles:
        return unknown

    latest = candles[-1]
    previous = candles[-2] if len(candles) >= 2 else None
    latest_volume = latest.get("volume")
    previous_volume = previous.get("volume") if previous else None
    sample = [
        candle.get("volume")
        for candle in candles[max(0, len(candles) - 21):-1]
        if isinstance(candle.get("volume"), (int, float)) and candle.get("volume") >= 0
    ]
    average = sum(sample) / len(sample) if sample else None
    rvol = (
        latest_volume / average
        if isinstance(latest_volume, (int, float)) and average and average > 0
        else None
    )

    if isinstance(latest_volume, (int, float)) and isinstance(previous_volume, (int, float)):
        if previous_volume <= 0:
            volume_trend = "rising" if latest_volume > 0 else "flat"
        elif latest_volume > previous_volume * 1.05:
            volume_trend = "rising"
        elif latest_volume < previous_volume * 0.95:
            volume_trend = "falling"
        else:
            volume_trend = "flat"
    else:
        volume_trend = "unknown"

    if rvol is None:
        volume_strength = "unknown"
    elif rvol >= 1.5:
        volume_strength = "strong"
    elif rvol >= 0.8:
        volume_strength = "normal"
    else:
        volume_strength = "weak"

    breakout_confirmed = None
    if best_setup and rvol is not None:
        reference = best_setup.get("trigger")
        if reference is None:
            reference = best_setup.get("level_price")
        latest_close = latest.get("close")
        direction = best_setup.get("direction")
        if isinstance(reference, (int, float)) and isinstance(latest_close, (int, float)):
            if direction == "bullish":
                breakout_confirmed = latest_close > reference and rvol >= 1.2
            elif direction == "bearish":
                breakout_confirmed = latest_close < reference and rvol >= 1.2

    return {
        "latest_volume": latest_volume,
        "previous_volume": previous_volume,
        "average_volume_20": round(average, 2) if average is not None else None,
        "relative_volume_20": round(rvol, 2) if rvol is not None else None,
        "rvol_20": round(rvol, 2) if rvol is not None else None,
        "volume_trend": volume_trend,
        "volume_strength": volume_strength,
        "volume_spike": bool(rvol is not None and rvol >= 2.0),
        "low_volume_warning": bool(rvol is not None and rvol < 0.7),
        "breakout_volume_confirmed": breakout_confirmed,
        "read_only": True,
    }


def compact_intraday_ai_context(payload):
    timeframe = payload.get("timeframe")
    candles = payload.get("candles") or []
    indicators = payload.get("indicators") or {}
    confirmation_setups = payload.get("confirmation_setups") or {}
    professional_context = payload.get("professional_context") or {}
    current_price = payload.get("current_price")
    latest_close = candles[-1].get("close") if candles else None
    latest_vwap = latest_indicator_value(indicators.get("vwap"))
    latest_ema9 = latest_indicator_value(indicators.get("ema9"))
    latest_ema20 = latest_indicator_value(indicators.get("ema20"))
    best_setup = select_best_setup_from_timeframe(timeframe, confirmation_setups)
    most_recent_setup = max(
        confirmation_setups.get("setups", []),
        key=lambda setup: setup.get("candle_time") or 0,
        default=None,
    )
    support_resistance = payload.get("support_resistance") or {}
    supply_demand = payload.get("supply_demand") or {}
    fair_value_gaps = payload.get("fair_value_gaps") or {}
    all_fvgs = (fair_value_gaps.get("bullish") or []) + (fair_value_gaps.get("bearish") or [])
    chart_lines = payload.get("chart_lines") or []
    zone_reactions = [
        {
            "type": zone.get("type"),
            "low": zone.get("low"),
            "high": zone.get("high"),
            "reaction_status": zone.get("reaction_status"),
            "reaction_label": zone.get("reaction_label"),
            "defended_edge": zone.get("defended_edge"),
            "failure_edge": zone.get("failure_edge"),
            "closes_holding_edge": zone.get("closes_holding_edge"),
            "touches_count": zone.get("touches_count"),
            "last_reaction_time": zone.get("last_reaction_time"),
            "reaction_confidence": zone.get("reaction_confidence"),
            "reaction_warnings": zone.get("reaction_warnings") or [],
            "read_only": True,
        }
        for side in ["demand", "supply"]
        for zone in supply_demand.get(side, []) or []
        if zone.get("reaction_status") in {"HOLD", "RECLAIM", "REJECTION", "FAILED"}
    ][:8]

    return {
        "timeframe": timeframe,
        "data_status": payload.get("data_status"),
        "data_quality_status": payload.get("data_quality_status", "DEGRADED"),
        "candle_accuracy_mode": payload.get("candle_accuracy_mode", "RAW_PROVIDER"),
        "suspicious_candle_count": payload.get("suspicious_candle_count", 0),
        "rejected_candle_count": payload.get("rejected_candle_count", 0),
        "corrected_candle_count": payload.get("corrected_candle_count", 0),
        "candle_data_warnings": payload.get("candle_data_warnings") or [],
        "latest_close": round_price(latest_close),
        "latest_candle_time": candles[-1].get("time") if candles else None,
        "latest_candle_direction": candle_direction(candles[-1] if candles else None),
        "previous_candle_direction": candle_direction(candles[-2] if len(candles) >= 2 else None),
        "vwap": round_price(latest_vwap),
        "ema9": round_price(latest_ema9),
        "ema20": round_price(latest_ema20),
        "price_relation_to_vwap": price_relation(current_price, latest_vwap),
        "price_relation_to_ema9": price_relation(current_price, latest_ema9),
        "price_relation_to_ema20": price_relation(current_price, latest_ema20),
        "trend_state": confirmation_setups.get("trend", {}).get("label"),
        "most_recent_confirmation_setup": compact_ai_setup(most_recent_setup, timeframe),
        "best_setup": compact_ai_setup(best_setup, timeframe),
        "professional_grade": best_setup.get("professional_grade") if best_setup else confirmation_setups.get("best_grade"),
        "professional_score": best_setup.get("professional_score") if best_setup else confirmation_setups.get("best_score"),
        "confirmation_stage": best_setup.get("confirmation_stage") if best_setup else None,
        "setup_status": best_setup.get("status") if best_setup else confirmation_setups.get("status"),
        "risk_reward": compact_risk_reward(best_setup.get("risk_reward")) if best_setup else None,
        "volume_context": build_ai_volume_context(candles, best_setup),
        "zone_reactions": zone_reactions,
        "fair_value_gaps": [
            {
                "direction": gap.get("direction"),
                "type": gap.get("type"),
                "top": gap.get("top"),
                "bottom": gap.get("bottom"),
                "midpoint": gap.get("midpoint"),
                "candle1_time": gap.get("candle1_time"),
                "candle3_time": gap.get("candle3_time"),
                "source": gap.get("source"),
                "reason": gap.get("reason"),
                "status": gap.get("status"),
                "quality_grade": gap.get("quality_grade"),
                "quality_score": gap.get("quality_score"),
                "fill_percentage": gap.get("fill_percentage"),
                "displacement_score": gap.get("displacement_score"),
                "engulfing_score": gap.get("engulfing_score"),
                "middle_candle_engulfed_c1": gap.get("middle_candle_engulfed_c1"),
                "middle_candle_closed_beyond_c1": gap.get("middle_candle_closed_beyond_c1"),
                "read_only": True,
            }
            for gap in all_fvgs
            if gap.get("status") in {"ACTIVE", "PARTIALLY_FILLED"} and gap.get("quality_grade") in {"A", "B"} and gap.get("visible_in_clean_mode")
        ],
        "fair_value_gap_counts": {
            "active_meaningful": len([
                gap for gap in all_fvgs
                if gap.get("status") in {"ACTIVE", "PARTIALLY_FILLED"} and gap.get("quality_grade") in {"A", "B"} and gap.get("visible_in_clean_mode")
            ]),
            "weak_or_filled_audit_only": len([
                gap for gap in all_fvgs
                if gap.get("quality_grade") == "WEAK" or gap.get("status") in {"FILLED", "INVALID"} or not gap.get("visible_in_clean_mode")
            ]),
        },
        "chart_line_audit": [
            {
                "type": line.get("type"),
                "short_label": line.get("short_label"),
                "price": line.get("price"),
                "top": line.get("top"),
                "bottom": line.get("bottom"),
                "source": line.get("source"),
                "reason": line.get("reason"),
                "confidence": line.get("confidence"),
                "strength": line.get("strength"),
                "status": line.get("status"),
                "priority": line.get("priority"),
                "read_only": True,
            }
            for line in chart_lines
            if line.get("priority") in {1, 2}
        ][:16],
        "warnings": list(dict.fromkeys(
            (confirmation_setups.get("quality_warnings") or [])
            + (professional_context.get("warnings") or [])
            + (payload.get("candle_data_warnings") or [])
        ))[:12],
        "checks": best_setup.get("quality_checks") if best_setup else None,
        "nearest_support": nearest_price_item(support_resistance.get("support"), current_price, ["price"]),
        "nearest_resistance": nearest_price_item(support_resistance.get("resistance"), current_price, ["price"]),
        "nearest_demand_zone": nearest_price_item(supply_demand.get("demand"), current_price, ["low", "high"]),
        "nearest_supply_zone": nearest_price_item(supply_demand.get("supply"), current_price, ["low", "high"]),
        "read_only": True,
    }


def unknown_daily_ai_context(error=None):
    return {
        "timeframe": "Daily",
        "data_status": "unknown",
        "latest_close": None,
        "latest_candle_direction": "unknown",
        "previous_candle_direction": "unknown",
        "vwap": None,
        "ema9": None,
        "ema20": None,
        "price_relation_to_vwap": "not_applicable",
        "price_relation_to_ema9": "unknown",
        "price_relation_to_ema20": "unknown",
        "trend_state": "UNKNOWN",
        "daily_bias": "unknown",
        "daily_structure": "unknown",
        "previous_daily_high": None,
        "previous_daily_low": None,
        "previous_daily_close": None,
        "price_relation_to_previous_daily_high": "unknown",
        "price_relation_to_previous_daily_low": "unknown",
        "price_relation_to_previous_daily_close": "unknown",
        "most_recent_confirmation_setup": None,
        "best_setup": None,
        "professional_grade": None,
        "professional_score": None,
        "confirmation_stage": None,
        "setup_status": "NO_SETUP",
        "risk_reward": None,
        "volume_context": {
            "latest_volume": None,
            "previous_volume": None,
            "average_volume_20": None,
            "relative_volume_20": None,
            "rvol_20": None,
            "volume_trend": "unknown",
            "volume_strength": "unknown",
            "volume_spike": False,
            "low_volume_warning": False,
            "breakout_volume_confirmed": None,
            "read_only": True,
        },
        "warnings": [f"Daily context unavailable: {error}"] if error else ["Daily context unavailable."],
        "checks": None,
        "nearest_support": None,
        "nearest_resistance": None,
        "nearest_demand_zone": None,
        "nearest_supply_zone": None,
        "read_only": True,
    }


def build_daily_ai_context(current_price=None, symbol=SYMBOL):
    try:
        now = datetime.now(ET)
        start = now - timedelta(days=180)
        bars = fetch_bars(normalize_symbol(symbol), start, now, timeframe="1Day", limit=250)
        candles = normalize_candles(bars)
        if not candles:
            return unknown_daily_ai_context("no daily bars returned")

        ema9 = latest_indicator_value(calc_ema(candles, 9))
        ema20 = latest_indicator_value(calc_ema(candles, 20))
        latest = candles[-1]
        previous = candles[-2] if len(candles) >= 2 else None
        price = current_price if current_price is not None else latest.get("close")

        if price is not None and ema9 is not None and ema20 is not None and price > ema9 > ema20:
            bias = "bullish"
            trend_state = "BULLISH"
        elif price is not None and ema9 is not None and ema20 is not None and price < ema9 < ema20:
            bias = "bearish"
            trend_state = "BEARISH"
        else:
            bias = "neutral"
            trend_state = "MIXED"

        recent = candles[-4:]
        if len(recent) >= 3 and all(
            recent[i]["high"] > recent[i - 1]["high"] and recent[i]["low"] > recent[i - 1]["low"]
            for i in range(1, len(recent))
        ):
            structure = "bullish"
        elif len(recent) >= 3 and all(
            recent[i]["high"] < recent[i - 1]["high"] and recent[i]["low"] < recent[i - 1]["low"]
            for i in range(1, len(recent))
        ):
            structure = "bearish"
        else:
            structure = "neutral"

        return {
            "timeframe": "Daily",
            "data_status": "ok",
            "latest_close": round_price(latest.get("close")),
            "latest_candle_direction": candle_direction(latest),
            "previous_candle_direction": candle_direction(previous),
            "vwap": None,
            "ema9": round_price(ema9),
            "ema20": round_price(ema20),
            "price_relation_to_vwap": "not_applicable",
            "price_relation_to_ema9": price_relation(price, ema9),
            "price_relation_to_ema20": price_relation(price, ema20),
            "trend_state": trend_state,
            "daily_bias": bias,
            "daily_structure": structure,
            "previous_daily_high": round_price(previous.get("high")) if previous else None,
            "previous_daily_low": round_price(previous.get("low")) if previous else None,
            "previous_daily_close": round_price(previous.get("close")) if previous else None,
            "price_relation_to_previous_daily_high": price_relation(price, previous.get("high") if previous else None),
            "price_relation_to_previous_daily_low": price_relation(price, previous.get("low") if previous else None),
            "price_relation_to_previous_daily_close": price_relation(price, previous.get("close") if previous else None),
            "most_recent_confirmation_setup": None,
            "best_setup": None,
            "professional_grade": None,
            "professional_score": None,
            "confirmation_stage": None,
            "setup_status": "BIAS_ONLY",
            "risk_reward": None,
            "volume_context": {
                "latest_volume": latest.get("volume"),
                "previous_volume": previous.get("volume") if previous else None,
                "average_volume_20": None,
                "relative_volume_20": None,
                "rvol_20": None,
                "volume_trend": "unknown",
                "volume_strength": "unknown",
                "volume_spike": False,
                "low_volume_warning": False,
                "breakout_volume_confirmed": None,
                "read_only": True,
            },
            "warnings": [],
            "checks": {"daily_bars": len(candles)},
            "nearest_support": None,
            "nearest_resistance": None,
            "nearest_demand_zone": None,
            "nearest_supply_zone": None,
            "read_only": True,
        }
    except Exception as e:
        return unknown_daily_ai_context(str(e))


def chart_payload_for_ai(timeframe, symbol=SYMBOL):
    response = chart_data(timeframe_override=timeframe, include_logging=False, symbol_override=symbol)
    if isinstance(response, tuple):
        response = response[0]
    return response.get_json()


def option_number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def option_contract_type(contract):
    contract_type = str(contract.get("type") or contract.get("option_type") or "").lower()
    return contract_type if contract_type in {"call", "put"} else None


def select_ai_option_contracts(symbol, underlying_price, setup_direction, contracts):
    candidates = [
        contract for contract in contracts or []
        if (contract.get("underlying_symbol") or contract.get("root_symbol")) == symbol
        and option_contract_type(contract) in {"call", "put"}
        and option_number(contract.get("strike_price") or contract.get("strike")) is not None
        and (contract.get("expiration_date") or contract.get("expiration"))
    ]
    if not candidates or not valid_number(underlying_price):
        return {"selected_expiration": None, "call": None, "put": None}

    expirations = sorted({str(contract.get("expiration_date") or contract.get("expiration")) for contract in candidates})
    selected_expiration = expirations[0]
    expiration_contracts = [
        contract for contract in candidates
        if str(contract.get("expiration_date") or contract.get("expiration")) == selected_expiration
    ]

    def selection_score(contract):
        strike = option_number(contract.get("strike_price") or contract.get("strike"))
        contract_type = option_contract_type(contract)
        distance = abs(strike - underlying_price)
        direction_match = (
            (setup_direction == "bullish" and contract_type == "call")
            or (setup_direction == "bearish" and contract_type == "put")
        )
        directionally_itm = direction_match and (
            (contract_type == "call" and strike <= underlying_price)
            or (contract_type == "put" and strike >= underlying_price)
        )
        return (distance, 0 if directionally_itm else 1)

    selected = {}
    for contract_type in ["call", "put"]:
        side = [contract for contract in expiration_contracts if option_contract_type(contract) == contract_type]
        selected[contract_type] = min(side, key=selection_score) if side else None
    selected["selected_expiration"] = selected_expiration
    return selected


def grade_option_contract_quality(contract_snapshot):
    bid = option_number(contract_snapshot.get("bid"))
    ask = option_number(contract_snapshot.get("ask"))
    volume = option_number(contract_snapshot.get("volume"))
    open_interest = option_number(contract_snapshot.get("open_interest"))
    implied_volatility = option_number(contract_snapshot.get("implied_volatility"))
    dte = contract_snapshot.get("dte")
    warnings = []

    if bid is None or ask is None or bid < 0 or ask <= 0 or ask < bid:
        grade = "BAD" if bid is not None or ask is not None else "UNKNOWN"
        warnings.append("Bid/ask quote is missing or invalid.")
    else:
        mid = (bid + ask) / 2
        spread_percent = ((ask - bid) / mid * 100) if mid > 0 else None
        if spread_percent is None:
            grade = "UNKNOWN"
        elif spread_percent <= 5:
            grade = "GOOD"
        elif spread_percent <= 10:
            grade = "OK"
        elif spread_percent <= 20:
            grade = "WEAK"
            warnings.append("Wide bid/ask spread may increase slippage.")
        else:
            grade = "BAD"
            warnings.append("Wide bid/ask spread increases slippage and bad-fill risk.")

    if volume is not None and volume < 100:
        warnings.append("Low option volume may reduce liquidity.")
    if open_interest is not None and open_interest < 100:
        warnings.append("Low open interest may reduce liquidity.")
    if isinstance(dte, int) and dte <= 1:
        warnings.append(f"{dte}DTE contract has fast theta-decay risk.")
    if implied_volatility is not None and implied_volatility >= 0.75:
        warnings.append("High implied volatility increases premium and IV-compression risk.")

    return {"liquidity_grade": grade, "contract_quality_warnings": list(dict.fromkeys(warnings))}


def compact_option_contract(contract, raw_snapshot, underlying_price, dte):
    contract_type = option_contract_type(contract)
    strike = option_number(contract.get("strike_price") or contract.get("strike"))
    expiration = contract.get("expiration_date") or contract.get("expiration")
    quote = raw_snapshot.get("latestQuote") or raw_snapshot.get("latest_quote") or {}
    trade = raw_snapshot.get("latestTrade") or raw_snapshot.get("latest_trade") or {}
    greeks = raw_snapshot.get("greeks") or {}
    daily_bar = raw_snapshot.get("dailyBar") or raw_snapshot.get("daily_bar") or {}
    bid = option_number(quote.get("bp") if "bp" in quote else quote.get("bid_price"))
    ask = option_number(quote.get("ap") if "ap" in quote else quote.get("ask_price"))
    mid = (bid + ask) / 2 if bid is not None and ask is not None and ask >= bid else None
    spread = ask - bid if bid is not None and ask is not None and ask >= bid else None
    spread_percent = spread / mid * 100 if spread is not None and mid and mid > 0 else None
    distance = strike - underlying_price if strike is not None and valid_number(underlying_price) else None

    if distance is None or contract_type is None:
        moneyness = "UNKNOWN"
    elif abs(distance) <= max(0.5, underlying_price * 0.0025):
        moneyness = "ATM"
    elif (contract_type == "call" and distance < 0) or (contract_type == "put" and distance > 0):
        moneyness = "ITM"
    else:
        moneyness = "OTM"

    compact = {
        "symbol": contract.get("symbol"),
        "expiration": str(expiration) if expiration else None,
        "strike": round_price(strike),
        "type": contract_type,
        "bid": round_price(bid),
        "ask": round_price(ask),
        "mid": round_price(mid),
        "spread": round_price(spread),
        "spread_percent": round(spread_percent, 2) if spread_percent is not None else None,
        "last_price": round_price(option_number(trade.get("p") if "p" in trade else trade.get("price"))),
        "delta": option_number(greeks.get("delta")),
        "theta": option_number(greeks.get("theta")),
        "implied_volatility": option_number(
            raw_snapshot.get("impliedVolatility")
            if "impliedVolatility" in raw_snapshot
            else raw_snapshot.get("implied_volatility")
        ),
        "volume": option_number(daily_bar.get("v") if "v" in daily_bar else daily_bar.get("volume")),
        "open_interest": option_number(contract.get("open_interest")),
        "moneyness": moneyness,
        "distance_from_underlying": round_price(distance),
        "dte": dte,
        "read_only": True,
    }
    compact.update(grade_option_contract_quality(compact))
    return compact


def unavailable_option_chain_context(snapshot, reason):
    setup = snapshot.get("best_setup") or {}
    return {
        "symbol": snapshot.get("symbol") or SYMBOL,
        "available": False,
        "source": "unavailable",
        "reason": str(reason),
        "underlying_price": snapshot.get("current_price"),
        "selected_expiration": None,
        "dte": None,
        "setup_direction": setup.get("direction") if setup.get("direction") in {"bullish", "bearish"} else "neutral",
        "contracts": {"call": None, "put": None},
        "liquidity_summary": {
            "best_available_side": "none",
            "overall_quality": "UNKNOWN",
            "warnings": [str(reason)],
        },
        "warnings": [str(reason)],
        "read_only": True,
    }


def build_ai_option_chain_context(snapshot):
    session = snapshot.get("market_session_status") or build_market_session_status()
    if not session.get("is_market_open_for_trading"):
        return unavailable_option_chain_context(snapshot, "Market is closed; live option snapshots may be stale.")
    underlying_price = snapshot.get("current_price")
    if not valid_number(underlying_price):
        return unavailable_option_chain_context(snapshot, "Underlying price is unavailable.")

    setup = snapshot.get("best_setup") or {}
    direction = setup.get("direction") if setup.get("direction") in {"bullish", "bearish"} else "neutral"
    try:
        contracts = fetch_alpaca_option_contracts(snapshot.get("symbol") or SYMBOL)
        selected = select_ai_option_contracts(snapshot.get("symbol") or SYMBOL, underlying_price, direction, contracts)
        expiration = selected.get("selected_expiration")
        if not expiration:
            return unavailable_option_chain_context(snapshot, "No active Alpaca option contracts were returned.")
        expiration_date = datetime.fromisoformat(expiration).date()
        dte = max(0, (expiration_date - datetime.now(ET).date()).days)
        symbols = [
            contract.get("symbol")
            for contract in [selected.get("call"), selected.get("put")]
            if contract and contract.get("symbol")
        ]
        snapshots = fetch_alpaca_option_snapshots(symbols)
        compact_contracts = {
            side: (
                compact_option_contract(contract, snapshots.get(contract.get("symbol")) or {}, underlying_price, dte)
                if contract and snapshots.get(contract.get("symbol")) else None
            )
            for side, contract in [("call", selected.get("call")), ("put", selected.get("put"))]
        }
        available_sides = [side for side, contract in compact_contracts.items() if contract]
        grades = [compact_contracts[side]["liquidity_grade"] for side in available_sides]
        grade_rank = {"BAD": 0, "WEAK": 1, "UNKNOWN": 2, "OK": 3, "GOOD": 4}
        overall_quality = max(grades, key=lambda grade: grade_rank.get(grade, 0)) if grades else "UNKNOWN"
        best_sides = [
            side for side in available_sides
            if compact_contracts[side]["liquidity_grade"] == overall_quality
        ]
        warnings = list(dict.fromkeys(
            warning
            for contract in compact_contracts.values()
            if contract
            for warning in contract.get("contract_quality_warnings", [])
        ))
        best_available_side = "both" if len(best_sides) == 2 else (best_sides[0] if best_sides else "none")
        return {
            "symbol": snapshot.get("symbol") or SYMBOL,
            "available": bool(available_sides),
            "source": "alpaca" if available_sides else "unavailable",
            "reason": None if available_sides else "Selected contract snapshots were unavailable.",
            "underlying_price": underlying_price,
            "selected_expiration": expiration,
            "dte": dte,
            "setup_direction": direction,
            "contracts": compact_contracts,
            "liquidity_summary": {
                "best_available_side": best_available_side,
                "overall_quality": overall_quality,
                "warnings": warnings,
            },
            "warnings": warnings,
            "read_only": True,
        }
    except Exception as error:
        return unavailable_option_chain_context(snapshot, f"Alpaca options data unavailable: {error}")


def build_ai_chart_snapshot(requested_timeframe="5Min", symbol=SYMBOL):
    symbol = normalize_symbol(symbol)
    requested_timeframe = requested_timeframe if requested_timeframe in {*TIMEFRAMES, "Daily"} else "5Min"
    now = datetime.now(ET)

    with _ai_snapshot_lock:
        cache_key = (symbol, requested_timeframe)
        cached = _ai_snapshot_cache.get(cache_key) or {}
        cached_at = cached.get("built_at")
        cached_snapshot = cached.get("snapshot")
        if cached_at and cached_snapshot and (now - cached_at).total_seconds() < AI_SNAPSHOT_CACHE_SECONDS:
            snapshot = dict(cached_snapshot)
            snapshot["cache_status"] = "hit"
            snapshot["market_session_status"] = build_market_session_status(now)
            snapshot["ai_event"] = current_ai_event_metadata()
            return snapshot

    intraday_payloads = {
        timeframe: chart_payload_for_ai(timeframe, symbol)
        for timeframe in ["1Min", "5Min", "15Min"]
    }
    timeframe_contexts = {
        timeframe: compact_intraday_ai_context(payload)
        for timeframe, payload in intraday_payloads.items()
    }
    current_price = intraday_payloads.get(requested_timeframe, {}).get("current_price")
    if current_price is None:
        current_price = next(
            (payload.get("current_price") for payload in intraday_payloads.values() if payload.get("current_price") is not None),
            None,
        )
    timeframe_contexts["Daily"] = build_daily_ai_context(current_price=current_price, symbol=symbol)
    if current_price is None:
        current_price = timeframe_contexts["Daily"].get("latest_close")

    requested_payload = intraday_payloads.get(requested_timeframe) or intraday_payloads.get("5Min") or {}
    professional_context = requested_payload.get("professional_context") or {}
    regime = professional_context.get("aapl", {}).get("regime", {})
    market_confirmation = professional_context.get("market_confirmation") or {}
    snapshot = {
        "symbol": symbol,
        "active_symbol": symbol,
        "related_market_symbols": get_related_market_symbols(symbol),
        "timestamp": now.isoformat(),
        "current_price": round_price(current_price),
        "requested_timeframe": requested_timeframe,
        "market_session_status": build_market_session_status(now),
        "data_quality_status": requested_payload.get("data_quality_status", "DEGRADED"),
        "candle_accuracy_mode": requested_payload.get("candle_accuracy_mode", "RAW_PROVIDER"),
        "suspicious_candle_count": requested_payload.get("suspicious_candle_count", 0),
        "rejected_candle_count": requested_payload.get("rejected_candle_count", 0),
        "corrected_candle_count": requested_payload.get("corrected_candle_count", 0),
        "candle_data_warnings": requested_payload.get("candle_data_warnings") or [],
        "data_quality_summary": (
            f"{requested_payload.get('data_quality_status', 'DEGRADED')} · "
            f"{requested_payload.get('candle_accuracy_mode', 'RAW_PROVIDER')} · "
            f"{requested_payload.get('rejected_candle_count', 0)} rejected · "
            f"{requested_payload.get('suspicious_candle_count', 0)} suspicious"
        ),
        "timeframes": timeframe_contexts,
        "market_context": {
            "market_regime": regime.get("regime"),
            "action_label": regime.get("action_label"),
            "trend_score": regime.get("trend_score"),
            "range_score": regime.get("range_score"),
            "chop_score": regime.get("chop_score"),
            "regime_confidence": regime.get("regime_confidence"),
            "market_confirmation": market_confirmation.get(
                "market_confirmation",
                professional_context.get("market_alignment"),
            ),
            "market_confirmation_score": market_confirmation.get(
                "market_confirmation_score",
                professional_context.get("market_confirmation_score"),
            ),
            "spy_bias": market_confirmation.get("spy_bias", professional_context.get("spy_bias")),
            "qqq_bias": market_confirmation.get("qqq_bias", professional_context.get("qqq_bias")),
            "aapl_relative_strength": market_confirmation.get(
                "aapl_relative_strength",
                professional_context.get("aapl_relative_strength"),
            ),
            "warnings": list(dict.fromkeys(
                (professional_context.get("warnings") or [])
                + (market_confirmation.get("market_warnings") or [])
            )),
            "read_only": True,
        },
        "best_setup": select_best_ai_setup(timeframe_contexts),
        "cache_status": "miss",
        "read_only": True,
    }
    snapshot["option_chain_context"] = build_ai_option_chain_context(snapshot)
    snapshot["ai_event"] = detect_ai_review_event(snapshot)

    with _ai_snapshot_lock:
        _ai_snapshot_cache[cache_key] = {
            "built_at": now,
            "snapshot": snapshot,
        }

    return snapshot


def valid_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def evaluate_ai_entry_marker_gates(snapshot, best_setup):
    """
    Deterministic read-only marker gates. AI commentary cannot override them.
    """
    snapshot = snapshot or {}
    best_setup = best_setup or {}
    market_context = snapshot.get("market_context") or {}
    risk_reward = best_setup.get("risk_reward") or {}
    direction = best_setup.get("direction")
    current_price = snapshot.get("current_price")
    suggested_entry = risk_reward.get("suggested_entry")
    invalidation = risk_reward.get("invalidation")
    stop_distance = risk_reward.get("stop_distance")
    checks = best_setup.get("quality_checks") or {}
    atr14 = checks.get("atr14")
    warnings = []
    gate_checks = {}

    def gate(name, passed, warning):
        gate_checks[name] = bool(passed)
        if not passed:
            warnings.append(warning)

    gate("setup_exists", bool(best_setup), "No best setup exists.")
    gate(
        "confirmation_stage_confirmed",
        best_setup.get("confirmation_stage") == "CONFIRMED",
        "Setup confirmation stage is not CONFIRMED.",
    )
    gate(
        "professional_grade_allowed",
        best_setup.get("professional_grade") in {"A", "A+"},
        "Professional grade must be A or A+.",
    )
    gate(
        "risk_reward_allowed",
        risk_reward.get("rr_grade") in {"GOOD", "OK"},
        "Risk/reward grade must be GOOD or OK.",
    )
    gate(
        "setup_not_invalidated",
        best_setup.get("status") != "INVALIDATED",
        "Setup is INVALIDATED.",
    )
    gate(
        "setup_not_failed",
        best_setup.get("confirmation_stage") != "FAILED",
        "Setup confirmation stage is FAILED.",
    )
    gate(
        "regime_not_chop",
        market_context.get("market_regime") != "CHOP",
        "Market regime is CHOP.",
    )
    gate(
        "action_allows_new_trades",
        market_context.get("action_label") != "NO_NEW_TRADES",
        "Market action label is NO_NEW_TRADES.",
    )

    market_confirmation = market_context.get("market_confirmation")
    market_opposes = (
        (direction == "bullish" and market_confirmation == "BEARISH")
        or (direction == "bearish" and market_confirmation == "BULLISH")
    )
    gate(
        "market_not_directly_against",
        not market_opposes,
        f"Market confirmation {market_confirmation} directly opposes the {direction or 'unknown'} setup.",
    )
    gate(
        "valid_suggested_entry",
        valid_number(suggested_entry),
        "Suggested entry is unavailable or invalid.",
    )
    gate(
        "valid_invalidation",
        valid_number(invalidation),
        "Invalidation is unavailable or invalid.",
    )
    gate(
        "valid_current_price",
        valid_number(current_price),
        "Current price is unavailable or invalid.",
    )

    extension_distance = (
        abs(current_price - suggested_entry)
        if valid_number(current_price) and valid_number(suggested_entry)
        else None
    )
    extension_limit = None
    extension_basis = "unavailable"
    if valid_number(suggested_entry) and suggested_entry > 0 and valid_number(stop_distance) and stop_distance > 0:
        extension_limit = max(stop_distance * 0.75, suggested_entry * 0.0015)
        extension_basis = "stop_distance"
    elif valid_number(suggested_entry) and suggested_entry > 0 and valid_number(atr14) and atr14 > 0:
        extension_limit = max(atr14 * 0.75, suggested_entry * 0.0015)
        extension_basis = "atr14"
    elif valid_number(suggested_entry) and suggested_entry > 0:
        extension_limit = suggested_entry * 0.003
        extension_basis = "percentage"

    extension_ok = (
        extension_distance is not None
        and extension_limit is not None
        and extension_distance <= extension_limit
    )
    gate(
        "price_not_too_extended",
        extension_ok,
        (
            f"Current price is too extended from suggested entry "
            f"({round_price(extension_distance)} away; limit {round_price(extension_limit)} using {extension_basis})."
            if extension_distance is not None and extension_limit is not None
            else "Price extension cannot be validated."
        ),
    )

    return {
        "allow_entry_marker": all(gate_checks.values()),
        "checks": gate_checks,
        "warnings": list(dict.fromkeys(warnings)),
        "extension": {
            "distance": round_price(extension_distance),
            "limit": round_price(extension_limit),
            "basis": extension_basis,
            "read_only": True,
        },
        "read_only": True,
    }


def build_ai_event_fingerprint(snapshot):
    snapshot = snapshot or {}
    setup = snapshot.get("best_setup") or {}
    risk_reward = setup.get("risk_reward") or {}
    market_context = snapshot.get("market_context") or {}
    gates = evaluate_ai_entry_marker_gates(snapshot, setup)
    five_min = (snapshot.get("timeframes") or {}).get("5Min") or {}

    return {
        "setup_exists": bool(setup),
        "setup_identity": "|".join(str(value) for value in [
            setup.get("timeframe"),
            setup.get("source"),
            setup.get("level_price"),
            setup.get("candle_time"),
        ]) if setup else None,
        "direction": setup.get("direction"),
        "confirmation_stage": setup.get("confirmation_stage"),
        "status": setup.get("status"),
        "professional_grade": setup.get("professional_grade"),
        "rr_grade": risk_reward.get("rr_grade"),
        "market_regime": market_context.get("market_regime"),
        "action_label": market_context.get("action_label"),
        "market_confirmation": market_context.get("market_confirmation"),
        "price_too_extended": not gates.get("checks", {}).get("price_not_too_extended", False),
        "five_min_candle_time": five_min.get("latest_candle_time"),
        "read_only": True,
    }


def compare_ai_event_fingerprints(previous, current):
    if not previous:
        return []

    reasons = []
    active_quality = current.get("professional_grade") in {"A+", "A", "B"}

    if not previous.get("setup_exists") and current.get("setup_exists"):
        reasons.append("New best setup appeared.")
    elif previous.get("setup_identity") != current.get("setup_identity") and current.get("setup_exists"):
        reasons.append("Best setup changed.")

    if previous.get("direction") and previous.get("direction") != current.get("direction"):
        reasons.append(f"Best setup direction changed to {current.get('direction')}.")

    previous_stage = previous.get("confirmation_stage")
    current_stage = current.get("confirmation_stage")
    if previous_stage == "WATCH" and current_stage == "EARLY_CONFIRM":
        reasons.append("Setup changed from WATCH to EARLY_CONFIRM.")
    if previous_stage == "EARLY_CONFIRM" and current_stage == "CONFIRMED":
        reasons.append("Setup changed from EARLY_CONFIRM to CONFIRMED.")
    if current_stage == "FAILED" and previous_stage != "FAILED":
        reasons.append("Setup changed to FAILED.")

    if current.get("status") == "INVALIDATED" and previous.get("status") != "INVALIDATED":
        reasons.append("Setup became INVALIDATED.")

    if current.get("professional_grade") in {"A", "A+"} and previous.get("professional_grade") not in {"A", "A+"}:
        reasons.append(f"Setup became {current.get('professional_grade')}.")

    if current.get("rr_grade") in {"GOOD", "OK"} and previous.get("rr_grade") not in {"GOOD", "OK"} and current.get("professional_grade") in {"A", "A+"}:
        reasons.append(f"Risk/reward improved to {current.get('rr_grade')} for an A/A+ setup.")
    if current.get("rr_grade") in {"WEAK", "BAD"} and previous.get("rr_grade") not in {"WEAK", "BAD"}:
        reasons.append(f"Risk/reward weakened to {current.get('rr_grade')}.")

    if previous.get("market_regime") != current.get("market_regime"):
        reasons.append(f"Market regime changed to {current.get('market_regime')}.")
    if current.get("action_label") == "NO_NEW_TRADES" and previous.get("action_label") != "NO_NEW_TRADES":
        reasons.append("Action label changed to NO_NEW_TRADES.")
    if previous.get("market_confirmation") != current.get("market_confirmation"):
        reasons.append(f"SPY/QQQ market confirmation changed to {current.get('market_confirmation')}.")
    if current.get("price_too_extended") and not previous.get("price_too_extended"):
        reasons.append("Price became too extended from the suggested entry.")

    if (
        previous.get("five_min_candle_time") != current.get("five_min_candle_time")
        and current.get("five_min_candle_time") is not None
        and active_quality
    ):
        reasons.append(f"New 5Min candle closed with an active {current.get('professional_grade')} setup.")

    return list(dict.fromkeys(reasons))


def current_ai_event_metadata():
    with _ai_event_lock:
        return {
            "ai_review_recommended": bool(_ai_event_state.get("ai_review_recommended")),
            "latest_event_reason": _ai_event_state.get("latest_event_reason"),
            "latest_event_time": _ai_event_state.get("latest_event_time"),
            "ai_auto_review_enabled": ENABLE_AI_AUTO_REVIEW,
            "read_only": True,
        }


def detect_ai_review_event(snapshot):
    fingerprint = build_ai_event_fingerprint(snapshot)
    now = datetime.now(ET).isoformat()

    with _ai_event_lock:
        previous = _ai_event_state.get("fingerprint")
        reasons = compare_ai_event_fingerprints(previous, fingerprint)
        _ai_event_state["fingerprint"] = fingerprint
        if reasons:
            _ai_event_state["ai_review_recommended"] = True
            _ai_event_state["latest_event_reason"] = " ".join(reasons)
            _ai_event_state["latest_event_time"] = now

        return {
            "ai_review_recommended": bool(_ai_event_state.get("ai_review_recommended")),
            "latest_event_reason": _ai_event_state.get("latest_event_reason"),
            "latest_event_time": _ai_event_state.get("latest_event_time"),
            "ai_auto_review_enabled": ENABLE_AI_AUTO_REVIEW,
            "read_only": True,
        }


def apply_ai_event_metadata(review, acknowledge=False):
    with _ai_event_lock:
        if acknowledge:
            _ai_event_state["ai_review_recommended"] = False
        review["ai_review_recommended"] = bool(_ai_event_state.get("ai_review_recommended"))
        review["latest_event_reason"] = _ai_event_state.get("latest_event_reason")
        review["latest_event_time"] = _ai_event_state.get("latest_event_time")
        review["ai_auto_review_enabled"] = ENABLE_AI_AUTO_REVIEW
    return review


def build_fallback_direct_answer(user_message, chart_summary, market_session_status=None):
    if not user_message:
        return "No specific question was asked. The current chart review is summarized below."

    question = str(user_message).strip()
    question_lower = question.lower()
    market_session_status = market_session_status or {}
    market_open_phrases = [
        "is the market open",
        "market open today",
        "is trading open",
        "can i trade today",
    ]
    if any(phrase in question_lower for phrase in market_open_phrases):
        session_label = market_session_status.get("session_label")
        if market_session_status.get("is_weekend"):
            return "The regular U.S. stock market session is closed today because it is the weekend."
        if market_session_status.get("market_closed_reason") == "Market holiday":
            return "The U.S. stock market is closed today because it is a market holiday."
        if session_label == "REGULAR":
            answer = "The regular U.S. stock market session is open now."
        elif session_label == "PREMARKET":
            answer = "The regular U.S. stock market session is not open yet; the supported premarket session is open."
        elif session_label == "AFTER_HOURS":
            answer = "The regular U.S. stock market session is closed; the supported after-hours session is open."
        else:
            answer = "The U.S. stock market is closed now because it is outside supported session hours."
        if not market_session_status.get("holiday_calendar_enabled", False):
            answer += " Holiday calendar not implemented; verify exchange holidays manually."
        return answer

    source_terms = ["source", "doctrine", "education", "occ", "finra", "cboe", "investor.gov"]
    options_terms = ["option", "0dte", "short-dated", "theta", "volatility", "iv"]

    if any(term in question_lower for term in source_terms) and any(term in question_lower for term in options_terms):
        return (
            "The options-risk doctrine is grounded in summarized educational principles from the OCC Options "
            "Disclosure Document, FINRA options investor education, Cboe Options Institute, and SEC / Investor.gov. "
            "The playbook summarizes reliable concepts and does not copy or replace the full source documents. "
            "For short-dated and 0DTE setups, that doctrine requires attention to theta decay, implied-volatility "
            "and volatility risk, bid-ask spread, liquidity and slippage, and the need for speed plus closed-candle "
            "confirmation. A correct stock direction can still produce a poor option result if the move stalls or the "
            "contract is expensive or illiquid. Chop, weak risk/reward, missing invalidation, and chasing an extended "
            "entry are no-go conditions."
        )

    return (
        f"Direct answer based on the current deterministic chart review: {chart_summary} "
        "The backend snapshot and hard gates remain authoritative."
    )


def build_current_setup_application(
    decision,
    summary,
    setup,
    risk_reward,
    market_context,
    gates,
    volume_context=None,
    option_chain_context=None,
):
    volume_context = volume_context or {}
    option_chain_context = option_chain_context or {}
    grade = setup.get("professional_grade") or "unrated"
    stage = setup.get("confirmation_stage") or setup.get("status") or "no setup"
    rr_grade = risk_reward.get("rr_grade") or "unavailable"
    regime = market_context.get("market_regime") or "unknown"
    market_confirmation = market_context.get("market_confirmation") or "unknown"
    marker_status = "allowed" if gates.get("allow_entry_marker") else "not allowed"
    volume_summary = (
        f"Volume strength is {volume_context.get('volume_strength') or 'unknown'} "
        f"with RVOL20 {volume_context.get('rvol_20') if volume_context.get('rvol_20') is not None else 'unknown'}."
    )
    option_summary = (
        f"Option contract quality is "
        f"{(option_chain_context.get('liquidity_summary') or {}).get('overall_quality') or 'UNKNOWN'}."
        if option_chain_context.get("available")
        else "Option contract quality is unavailable; this review remains chart-only."
    )
    return (
        f"Current chart decision: {decision}. Entry marker is {marker_status}. "
        f"Setup quality is {grade} with stage {stage}; risk/reward is {rr_grade}; "
        f"market regime is {regime}; related-market confirmation is {market_confirmation}. "
        f"{volume_summary} {option_summary} {summary}"
    )


def select_review_volume_context(snapshot, setup):
    timeframes = snapshot.get("timeframes") or {}
    setup_timeframe = setup.get("timeframe")
    requested_timeframe = snapshot.get("requested_timeframe")
    for timeframe in [setup_timeframe, requested_timeframe, "5Min", "15Min", "1Min"]:
        volume_context = (timeframes.get(timeframe) or {}).get("volume_context")
        if volume_context:
            return volume_context
    return {}


def build_fallback_ai_review(snapshot, user_message=None):
    snapshot = snapshot or {}
    best_setup = snapshot.get("best_setup")
    market_context = snapshot.get("market_context") or {}
    gates = evaluate_ai_entry_marker_gates(snapshot, best_setup)
    setup = best_setup or {}
    risk_reward = setup.get("risk_reward") or {}
    grade = setup.get("professional_grade")
    stage = setup.get("confirmation_stage")
    direction = setup.get("direction") if setup.get("direction") in {"bullish", "bearish"} else "neutral"
    volume_context = select_review_volume_context(snapshot, setup)
    requested_context = (snapshot.get("timeframes") or {}).get(snapshot.get("requested_timeframe")) or {}
    zone_reactions = requested_context.get("zone_reactions") or []
    market_session_status = snapshot.get("market_session_status") or build_market_session_status()
    option_chain_context = snapshot.get("option_chain_context") or unavailable_option_chain_context(
        snapshot,
        "Option chain context is unavailable.",
    )

    warnings = list(gates.get("warnings") or [])
    warnings.extend(risk_reward.get("rr_warnings") or [])
    warnings.extend(market_context.get("warnings") or [])
    if snapshot.get("data_quality_status") != "CLEAN":
        warnings.append(
            f"Candle data quality is {snapshot.get('data_quality_status', 'DEGRADED')}; rejected candles were excluded from analysis."
        )
        warnings.extend(snapshot.get("candle_data_warnings") or [])
    warnings.extend(
        warning
        for reaction in zone_reactions
        for warning in reaction.get("reaction_warnings") or []
    )
    if volume_context.get("low_volume_warning"):
        warnings.append("Low RVOL: setup may lack participation; short-dated options are more vulnerable if price stalls.")
    elif volume_context.get("volume_strength") == "weak":
        warnings.append("Weak RVOL: setup participation is below recent activity.")
    if option_chain_context.get("available"):
        warnings.extend(option_chain_context.get("warnings") or [])
        option_quality = (option_chain_context.get("liquidity_summary") or {}).get("overall_quality")
        if option_quality in {"WEAK", "BAD"}:
            warnings.append(f"Chart setup quality and option contract quality differ: selected contract quality is {option_quality}.")
    warnings = list(dict.fromkeys(warnings))

    if not best_setup:
        decision = "WAIT"
        confidence = 10
        summary = "No current confirmation setup is available. Wait for a structured setup."
    elif gates["allow_entry_marker"]:
        decision = "PLAN_READY"
        confidence = min(95, max(75, int(setup.get("professional_score") or 75)))
        summary = (
            f"{grade} {direction} setup is confirmed and passes all strict read-only marker gates. "
            "Confirm manually before taking any action."
        )
    elif grade in {"NO_TRADE", "C"} or stage == "FAILED" or setup.get("status") == "INVALIDATED":
        decision = "AVOID" if grade == "NO_TRADE" or stage == "FAILED" or setup.get("status") == "INVALIDATED" else "WAIT"
        confidence = min(45, int(setup.get("professional_score") or 20))
        summary = f"{grade or 'Unrated'} setup does not meet strict trade-review quality requirements."
    elif grade == "B" or stage == "EARLY_CONFIRM":
        decision = "WATCH"
        confidence = min(65, max(30, int(setup.get("professional_score") or 40)))
        summary = "Setup is forming but still needs stronger confirmation before it can be plan ready."
    elif grade in {"A", "A+"}:
        decision = "WATCH" if stage in {"EARLY_CONFIRM", "CONFIRMED"} else "WAIT"
        confidence = min(75, max(40, int(setup.get("professional_score") or 55)))
        summary = f"{grade} setup exists, but one or more hard marker gates are blocking entry guidance."
    else:
        decision = "WAIT"
        confidence = min(50, int(setup.get("professional_score") or 20))
        summary = "Current setup evidence is incomplete. Wait for clearer confirmation."

    if best_setup and volume_context.get("volume_strength") == "strong":
        confidence = min(100, confidence + 5)
    elif volume_context.get("low_volume_warning"):
        confidence = max(0, confidence - 10)
    elif volume_context.get("volume_strength") == "weak":
        confidence = max(0, confidence - 5)

    direct_answer = build_fallback_direct_answer(user_message, summary, market_session_status)
    application_to_current_setup = build_current_setup_application(
        decision,
        summary,
        setup,
        risk_reward,
        market_context,
        gates,
        volume_context,
        option_chain_context,
    )

    entry_conditions = []
    if best_setup:
        if stage != "CONFIRMED":
            entry_conditions.append("Wait for confirmation_stage = CONFIRMED.")
        if grade not in {"A", "A+"}:
            entry_conditions.append("Wait for professional grade A or A+.")
        if risk_reward.get("rr_grade") not in {"GOOD", "OK"}:
            entry_conditions.append("Require GOOD or OK risk/reward.")
        if market_context.get("market_regime") == "CHOP":
            entry_conditions.append("Wait for the selected symbol to leave CHOP.")
        if market_context.get("action_label") == "NO_NEW_TRADES":
            entry_conditions.append("Wait until the action label allows new trades.")
        if not gates["checks"].get("market_not_directly_against"):
            entry_conditions.append("Wait for broader-market confirmation to stop opposing the setup.")
        if not gates["checks"].get("price_not_too_extended"):
            entry_conditions.append("Wait for price to return near the suggested entry without chasing.")

    allow_marker = gates["allow_entry_marker"]
    marker_price = risk_reward.get("suggested_entry") if allow_marker else None
    marker_label = (
        "ENTER TRADE SETUP — POSSIBLE ENTRY — NOT AN ORDER"
        if allow_marker
        else ""
    )
    snapshot_summary = {
        "symbol": snapshot.get("symbol"),
        "timestamp": snapshot.get("timestamp"),
        "requested_timeframe": snapshot.get("requested_timeframe"),
        "current_price": snapshot.get("current_price"),
        "best_setup_timeframe": setup.get("timeframe") if best_setup else None,
        "professional_grade": grade,
        "professional_score": setup.get("professional_score") if best_setup else None,
        "confirmation_stage": stage,
        "setup_status": setup.get("status") if best_setup else None,
        "rr_grade": risk_reward.get("rr_grade") if best_setup else None,
        "market_regime": market_context.get("market_regime"),
        "action_label": market_context.get("action_label"),
        "market_confirmation": market_context.get("market_confirmation"),
        "gate_checks": gates.get("checks"),
        "extension": gates.get("extension"),
        "volume_context": volume_context,
        "market_session_status": market_session_status,
        "option_chain_context": option_chain_context,
        "zone_reactions": zone_reactions,
        "user_message": user_message,
        "read_only": True,
    }

    review = {
        "decision": decision,
        "bias": direction,
        "confidence": max(0, min(100, int(confidence))),
        "summary": f"{summary} {AI_REVIEW_SAFETY_TEXT}",
        "direct_answer": direct_answer,
        "application_to_current_setup": application_to_current_setup,
        "what_ai_sees": summary,
        "professional_reasoning": (
            "The deterministic chart engines, strict grade, confirmation stage, market context, "
            "risk/reward, and hard marker gates remain the source of truth."
        ),
        "entry_conditions": entry_conditions,
        "trap_warnings": [
            warning for warning in warnings
            if any(term in warning.lower() for term in ["trap", "sweep", "failed", "invalid", "oppos", "chop"])
        ],
        "options_risk_notes": list(dict.fromkeys([
            "Short-dated and 0DTE options can decay quickly if the setup stalls or chops.",
            "A correct stock direction can still disappoint because of timing, implied volatility, spreads, and slippage.",
            *(
                ["Low RVOL increases stall and chop risk for short-dated options."]
                if volume_context.get("low_volume_warning")
                else []
            ),
            *(
                ["Volume spike confirms participation alongside the setup direction."]
                if volume_context.get("volume_spike") and volume_context.get("breakout_volume_confirmed")
                else []
            ),
            *(
                option_chain_context.get("warnings") or []
                if option_chain_context.get("available")
                else ["Alpaca option contract data is unavailable; review is based on chart context only."]
            ),
        ])),
        "exit_plan": {
            "invalidation": risk_reward.get("invalidation"),
            "target_1": risk_reward.get("target_1"),
            "target_2": risk_reward.get("target_2"),
            "target_3": risk_reward.get("target_3"),
        },
        "allow_entry_marker": allow_marker,
        "entry_marker": {
            "price": marker_price,
            "label": marker_label,
            "direction": direction if allow_marker else "neutral",
        },
        "warnings": warnings,
        "do_not_chase": AI_REVIEW_SAFETY_TEXT,
        "manual_confirmation_checklist": [
            "Confirm the setup and direction manually.",
            "Confirm entry, invalidation, and realistic targets.",
            "Confirm related-market context and market regime are not opposing the setup.",
            "Confirm price has not extended away from the suggested entry.",
        ],
        "read_only": True,
        "not_financial_advice": True,
        "not_an_order": True,
        "source": "fallback",
        "snapshot_summary": snapshot_summary,
    }
    return review


def ai_trade_review_json_schema():
    string_list = {"type": "array", "items": {"type": "string"}}
    nullable_number = {"type": ["number", "null"]}
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "decision": {"type": "string", "enum": ["WAIT", "AVOID", "WATCH", "PLAN_READY"]},
            "bias": {"type": "string", "enum": ["bullish", "bearish", "neutral"]},
            "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
            "summary": {"type": "string"},
            "direct_answer": {"type": "string"},
            "application_to_current_setup": {"type": "string"},
            "what_ai_sees": {"type": "string"},
            "professional_reasoning": {"type": "string"},
            "entry_conditions": string_list,
            "trap_warnings": string_list,
            "options_risk_notes": string_list,
            "exit_plan": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "invalidation": nullable_number,
                    "target_1": nullable_number,
                    "target_2": nullable_number,
                    "target_3": nullable_number,
                },
                "required": ["invalidation", "target_1", "target_2", "target_3"],
            },
            "allow_entry_marker": {"type": "boolean"},
            "entry_marker": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "price": nullable_number,
                    "label": {"type": "string"},
                    "direction": {"type": "string", "enum": ["bullish", "bearish", "neutral"]},
                },
                "required": ["price", "label", "direction"],
            },
            "warnings": string_list,
            "do_not_chase": {"type": "string"},
            "manual_confirmation_checklist": string_list,
            "read_only": {"type": "boolean"},
            "not_financial_advice": {"type": "boolean"},
            "not_an_order": {"type": "boolean"},
            "source": {"type": "string", "enum": ["openai", "fallback"]},
        },
        "required": [
            "decision",
            "bias",
            "confidence",
            "summary",
            "direct_answer",
            "application_to_current_setup",
            "what_ai_sees",
            "professional_reasoning",
            "entry_conditions",
            "trap_warnings",
            "options_risk_notes",
            "exit_plan",
            "allow_entry_marker",
            "entry_marker",
            "warnings",
            "do_not_chase",
            "manual_confirmation_checklist",
            "read_only",
            "not_financial_advice",
            "not_an_order",
            "source",
        ],
    }


def load_ai_trading_playbook():
    try:
        with open(AI_PLAYBOOK_PATH, "r", encoding="utf-8") as playbook:
            return playbook.read()
    except Exception as e:
        return f"Playbook unavailable. Preserve strict read-only safety doctrine. Error: {e}"


def extract_openai_response_text(response_payload):
    for item in response_payload.get("output", []) or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            if content.get("type") == "output_text" and content.get("text"):
                return content["text"]
            if content.get("type") == "refusal":
                raise RuntimeError(f"OpenAI refused the review: {content.get('refusal')}")
    raise RuntimeError("OpenAI response did not contain structured output text.")


def call_openai_trade_review(snapshot, user_message=None):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OpenAI API key not configured.")

    model = os.getenv("OPENAI_MODEL") or DEFAULT_OPENAI_MODEL
    active_symbol = snapshot.get("symbol") or SYMBOL
    playbook = load_ai_trading_playbook()
    prompt_payload = {
        "user_message": user_message,
        "snapshot": snapshot,
        "hard_gates": evaluate_ai_entry_marker_gates(snapshot, snapshot.get("best_setup")),
    }
    system_prompt = (
        f"You are a strict professional intraday {active_symbol} stock/options review assistant. "
        "Use the trading playbook as reliable educational doctrine, not as a trade-calling signal service. "
        "Do not invent rules or facts outside the structured backend snapshot. "
        "The deterministic chart engine, snapshot, and backend hard gates are the source of truth. "
        "If educational doctrine and current chart data appear to conflict, chart data and backend gates win. "
        "Use each intraday timeframe's volume_context as confirmation and risk context only. Mention strong or weak "
        "volume when relevant, but never treat volume as a standalone trade signal or let it override marker gates. "
        "For short-dated and 0DTE options, low RVOL increases stall and chop risk. "
        "Use option_chain_context to separate chart setup quality from option contract quality. Warn about wide "
        "spreads, weak liquidity, low volume/open interest, fast theta decay, and high implied volatility when "
        "present. Option data is risk context only: it cannot create marker eligibility or override backend gates. "
        "Use each timeframe's zone_reactions as early reversal-watch context only. HOLD, RECLAIM, and REJECTION "
        "must never create an entry or PLAN_READY decision by themselves; require confirmation, risk/reward, "
        "related-market context, and backend gates. Treat FAILED zones as invalidated context. "
        "If the user asks whether the market is open, answer using market_session_status only and never guess from "
        "general knowledge. If it says CLOSED, say the market is closed. If it is a weekend, clearly say the regular "
        "U.S. stock market session is closed because it is the weekend. When the session status says Market holiday, "
        "say the market is closed for that holiday. "
        "When user_message is provided, answer the user's exact question first in direct_answer, then explain how "
        f"it applies to the current {active_symbol} snapshot in application_to_current_setup. If the user asks about "
        f"a different symbol, clarify that the active chart symbol is {active_symbol}. If asked about reliable sources, "
        "explicitly mention the OCC Options Disclosure Document, FINRA options investor education, Cboe Options "
        "Institute, and SEC / Investor.gov, and explain that the playbook summarizes rather than copies them. "
        "You cannot override gates, place trades, tell the user to buy or sell now, claim certainty, "
        "or remove risk warnings. Explain uncertainty professionally and prefer WAIT over a forced setup. "
        "Return only the required JSON. "
        f"Every review must preserve this exact safety text: {AI_REVIEW_SAFETY_TEXT}\n\n"
        f"TRADING PLAYBOOK:\n{playbook}"
    )
    response = requests.post(
        OPENAI_RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "store": False,
            "input": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": (
                        "If user_message is present, answer it directly first. Then apply that answer to the current "
                        "chart and review the compact multi-timeframe snapshot, including traps, no-go conditions, "
                        "what must happen before entry, confidence, and options risk if the setup stalls. "
                        "Backend gates cannot be overridden.\n"
                        + json.dumps(prompt_payload, separators=(",", ":"), sort_keys=True)
                    ),
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "ai_trade_review",
                    "strict": True,
                    "schema": ai_trade_review_json_schema(),
                }
            },
        },
        timeout=45,
    )
    if not response.ok:
        try:
            detail = response.json().get("error", {}).get("message")
        except Exception:
            detail = None
        raise RuntimeError(f"OpenAI request failed ({response.status_code}): {detail or response.text[:200]}")

    return json.loads(extract_openai_response_text(response.json()))


def normalize_openai_trade_review(review, snapshot, fallback_review):
    if not isinstance(review, dict):
        raise ValueError("OpenAI review is not a JSON object.")

    forbidden = [
        "buy now",
        "sell now",
        "guaranteed",
        "this will work",
        "ignore your stop",
        "enter without confirmation",
        "i placed the trade",
    ]
    review_text = json.dumps(review).lower()
    if any(phrase in review_text for phrase in forbidden):
        raise ValueError("OpenAI review contained forbidden certainty or order language.")

    gates = evaluate_ai_entry_marker_gates(snapshot, snapshot.get("best_setup"))
    final = dict(fallback_review)
    for key in [
        "decision",
        "bias",
        "confidence",
        "summary",
        "direct_answer",
        "application_to_current_setup",
        "what_ai_sees",
        "professional_reasoning",
        "entry_conditions",
        "trap_warnings",
        "options_risk_notes",
        "exit_plan",
        "warnings",
        "do_not_chase",
        "manual_confirmation_checklist",
    ]:
        if key in review:
            final[key] = review[key]

    final["confidence"] = max(0, min(100, int(final.get("confidence") or 0)))
    final["warnings"] = list(dict.fromkeys(
        (final.get("warnings") or [])
        + (fallback_review.get("warnings") or [])
        + (gates.get("warnings") or [])
    ))
    final["summary"] = f"{str(final.get('summary') or '').strip()} {AI_REVIEW_SAFETY_TEXT}".strip()
    final["do_not_chase"] = AI_REVIEW_SAFETY_TEXT
    final["read_only"] = True
    final["not_financial_advice"] = True
    final["not_an_order"] = True
    final["source"] = "openai"
    final["snapshot_summary"] = fallback_review.get("snapshot_summary", {})

    if gates["allow_entry_marker"]:
        best_setup = snapshot.get("best_setup") or {}
        risk_reward = best_setup.get("risk_reward") or {}
        final["allow_entry_marker"] = True
        final["decision"] = "PLAN_READY"
        final["entry_marker"] = {
            "price": risk_reward.get("suggested_entry"),
            "label": "ENTER TRADE SETUP — POSSIBLE ENTRY — NOT AN ORDER",
            "direction": best_setup.get("direction") if best_setup.get("direction") in {"bullish", "bearish"} else "neutral",
        }
    else:
        final["allow_entry_marker"] = False
        final["entry_marker"] = {"price": None, "label": "", "direction": "neutral"}
        final["decision"] = fallback_review.get("decision") if fallback_review.get("decision") in {"WAIT", "WATCH", "AVOID"} else "WAIT"

    return final


def build_ai_trade_review(snapshot, user_message=None):
    fallback_review = build_fallback_ai_review(snapshot, user_message=user_message)
    if not os.getenv("OPENAI_API_KEY"):
        fallback_review["warnings"] = list(dict.fromkeys(
            (fallback_review.get("warnings") or []) + ["OpenAI API key not configured."]
        ))
        return fallback_review

    try:
        review = call_openai_trade_review(snapshot, user_message=user_message)
        return normalize_openai_trade_review(review, snapshot, fallback_review)
    except Exception as e:
        fallback_review["warnings"] = list(dict.fromkeys(
            (fallback_review.get("warnings") or []) + [f"OpenAI review unavailable: {e}"]
        ))
        return fallback_review


@APP.route("/")
def home():
    return send_from_directory("static", "index_stream.html")


@APP.route("/app_stream.js")
def app_stream_js():
    return send_from_directory("static", "app_stream.js")


@APP.route("/api/chart")
def generic_chart_data():
    raw_symbol = request.args.get("symbol", SYMBOL)
    if not str(raw_symbol).strip():
        return jsonify({"data_status": "error", "errors": ["Symbol is required."], "read_only": True}), 400
    return chart_data(symbol_override=raw_symbol)


@APP.route("/api/chart/aapl")
def chart_data(timeframe_override=None, include_logging=True, symbol_override=SYMBOL):
    try:
        active_symbol = normalize_symbol(symbol_override)
    except ValueError as error:
        return jsonify({"data_status": "error", "errors": [str(error)], "read_only": True}), 400
    tf = timeframe_override or request.args.get("timeframe", "1Min")
    timeframe = tf if tf in TIMEFRAMES else "1Min"

    now = datetime.now(ET)
    day = now.date()
    prev_day = previous_weekday(day)

    today_start = et_datetime(day, 4, 0)
    today_end = now

    prev_start = et_datetime(prev_day, 9, 30)
    prev_end = et_datetime(prev_day, 16, 5)

    try:
        today_bars_1min = fetch_bars(active_symbol, today_start, today_end, timeframe="1Min")
        prev_bars_1min = fetch_bars(active_symbol, prev_start, prev_end, timeframe="1Min")
        chart_session_day = day
        chart_session_historical = False
        chart_session_reason = None

        if not today_bars_1min:
            chart_session_day = previous_weekday(day)
            chart_session_historical = chart_session_day != day
            if chart_session_historical:
                chart_session_reason = "No current-session candles available; showing the previous trading session."
                today_start = et_datetime(chart_session_day, 4, 0)
                today_end = et_datetime(chart_session_day, 20, 0)
                prior_session_day = previous_weekday(chart_session_day)
                prev_start = et_datetime(prior_session_day, 9, 30)
                prev_end = et_datetime(prior_session_day, 16, 5)
                today_bars_1min = fetch_bars(active_symbol, today_start, today_end, timeframe="1Min")
                prev_bars_1min = fetch_bars(active_symbol, prev_start, prev_end, timeframe="1Min")

        provider_bars = (
            today_bars_1min
            if timeframe == "1Min"
            else fetch_bars(active_symbol, today_start, today_end, timeframe=timeframe)
        )
        integrity = build_candle_integrity_bundle(today_bars_1min, active_symbol, timeframe, provider_bars)
        previous_integrity = build_candle_integrity_bundle(prev_bars_1min, active_symbol, "1Min", prev_bars_1min)
        candles = integrity["validated_candles"]
        levels = calc_levels(
            integrity["validated_1min_candles"],
            previous_integrity["validated_1min_candles"],
            session_day=chart_session_day,
        )
        current_price = candles[-1]["close"] if candles else None

        with candle_audit_lock:
            latest_candle_audits[(active_symbol, timeframe)] = integrity
            recent_validated_candles[active_symbol] = integrity["validated_1min_candles"][-40:]

        regular_candles = []
        for c in candles:
            dt_utc = datetime.fromtimestamp(c["time"], tz=timezone.utc)
            if is_regular_dt(dt_utc):
                regular_candles.append(c)

        indicators_source = regular_candles if regular_candles else candles
        indicators = {
            "vwap": calc_vwap(indicators_source),
            "ema9": calc_ema(indicators_source, 9),
            "ema20": calc_ema(indicators_source, 20),
            "atr14": calc_atr14(indicators_source, 14),
        }
        latest_vwap = latest_indicator_value(indicators.get("vwap"))
        latest_atr14 = latest_indicator_value(indicators.get("atr14"))

        raw_support_resistance = detect_support_resistance(indicators_source, current_price=current_price)
        support_resistance = filter_and_score_support_resistance(
            raw_support_resistance,
            indicators_source,
            current_price=current_price,
            levels=levels,
            vwap=latest_vwap,
            atr14=latest_atr14,
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
                htf_candles = aggregate_validated_candles(
                    integrity["validated_1min_candles"],
                    htf,
                    integrity["audited_1min_candles"],
                )
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

        support_resistance = filter_and_score_support_resistance(
            raw_support_resistance,
            indicators_source,
            current_price=current_price,
            levels=levels,
            vwap=latest_vwap,
            supply_demand=supply_demand,
            level_clusters=level_clusters,
            atr14=latest_atr14,
        )

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

        supply_demand = filter_reliable_supply_demand(
            enhanced_supply_demand,
            levels=levels,
            vwap=latest_vwap,
            support_resistance=support_resistance,
            level_clusters=level_clusters,
            atr14=latest_atr14,
        )

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

        fair_value_gaps = detect_fair_value_gaps(
            indicators_source,
            symbol=active_symbol,
            timeframe=timeframe,
            levels=levels,
            supply_demand=supply_demand,
            support_resistance=support_resistance,
            atr14=latest_atr14,
        )

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
            symbol=active_symbol,
        )

        for setup in confirmation_setups.get("setups", []):
            setup["market_regime"] = dict(professional_context.get("aapl", {}).get("regime", {}))
            setup["market_confirmation"] = dict(professional_context.get("market_confirmation", {}))

        confirmation_setups = grade_confirmation_setups_with_context(
            confirmation_setups,
            professional_context,
        )

        confirmation_setups = enrich_confirmation_setups_with_risk_reward(
            confirmation_setups,
            levels=levels,
            support_resistance=support_resistance,
            supply_demand=supply_demand,
            level_clusters=level_clusters,
            professional_context=professional_context,
        )

        confirmation_setups = finalize_confirmation_setup_stages(
            confirmation_setups,
            professional_context,
        )

        confirmation_setups = grade_confirmation_setups_with_context(
            confirmation_setups,
            professional_context,
        )

        if include_logging and not chart_session_historical:
            logged_setups = log_confirmation_setups(
                active_symbol,
                timeframe,
                confirmation_setups,
                professional_context,
                current_price=current_price,
            )

            setup_outcomes = evaluate_setup_outcomes(
                active_symbol,
                timeframe,
                indicators_source,
                confirmation_setups,
            )
        else:
            logged_setups = 0
            setup_outcomes = []

        chart_snapshot = {
            "symbol": active_symbol,
            "active_symbol": active_symbol,
            "related_market_symbols": get_related_market_symbols(active_symbol),
            "read_only": True,
            "timeframe": timeframe,
            "timestamp": now.isoformat(),
            "chart_session": {
                "date": chart_session_day.isoformat(),
                "is_historical": chart_session_historical,
                "label": "PREVIOUS SESSION" if chart_session_historical else "CURRENT SESSION",
                "reason": chart_session_reason,
                "read_only": True,
            },
            "current_price": current_price,
            "latest_trade": get_latest_trade(active_symbol),
            "stream_status": get_stream_status(active_symbol),
            "candles": candles,
            "levels": levels,
            "support_resistance": support_resistance,
            "structure_reactions": structure_reactions,
            "supply_demand": supply_demand,
            "fair_value_gaps": fair_value_gaps,
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
            "data_quality_status": integrity["data_quality_status"],
            "candle_accuracy_mode": integrity["candle_accuracy_mode"],
            "raw_candle_count": integrity["raw_candle_count"],
            "validated_candle_count": integrity["validated_candle_count"],
            "suspicious_candle_count": integrity["suspicious_candle_count"],
            "rejected_candle_count": integrity["rejected_candle_count"],
            "corrected_candle_count": integrity["corrected_candle_count"],
            "candle_data_warnings": integrity["candle_data_warnings"],
            "bad_print_filter_enabled": True,
            "data_status": "ok",
            "errors": [],
        }
        chart_line_audit = build_chart_line_registry(chart_snapshot, active_symbol, timeframe)
        chart_snapshot["chart_lines"] = chart_line_audit["chart_lines"]
        chart_snapshot["chart_line_warnings"] = list(dict.fromkeys(
            chart_line_audit["warnings"] + integrity["candle_data_warnings"]
        ))
        return jsonify(chart_snapshot)
    except Exception as e:
        return jsonify({
            "symbol": active_symbol,
            "active_symbol": active_symbol,
            "read_only": True,
            "timeframe": timeframe,
            "timestamp": now.isoformat(),
            "current_price": None,
            "latest_trade": get_latest_trade(active_symbol),
            "stream_status": get_stream_status(active_symbol),
            "candles": [],
            "levels": {},
            "support_resistance": {"support": [], "resistance": []},
            "structure_reactions": {"support_watch": [], "resistance_watch": [], "meta": {"reason": "error"}},
            "supply_demand": {"demand": [], "supply": []},
            "fair_value_gaps": {"bullish": [], "bearish": [], "all": [], "meta": {"reason": "error", "read_only": True}},
            "liquidity_sweeps": {"upside": [], "downside": [], "status": "ERROR"},
            "level_clusters": {"clusters": [], "note": "error"},
            "confirmation_setups": {"status": "ERROR", "trend": {}, "setups": [], "meta": {"read_only": True}},
            "professional_context": {"professional_grade": "ERROR", "warnings": ["chart error"], "read_only": True},
            "indicators": {},
            "chart_lines": [],
            "chart_line_warnings": ["Chart data unavailable; no plotted lines were registered."],
            "data_quality_status": "DEGRADED",
            "candle_accuracy_mode": "RAW_PROVIDER",
            "raw_candle_count": 0,
            "validated_candle_count": 0,
            "suspicious_candle_count": 0,
            "rejected_candle_count": 0,
            "corrected_candle_count": 0,
            "candle_data_warnings": ["Candle validation could not complete."],
            "bad_print_filter_enabled": True,
            "data_status": "error",
            "errors": [str(e)],
        }), 500


@APP.route("/api/chart/rebuild", methods=["POST"])
def rebuild_chart_data():
    try:
        symbol = normalize_symbol(request.args.get("symbol", SYMBOL))
    except ValueError as error:
        return jsonify({"data_status": "error", "errors": [str(error)], "read_only": True}), 400

    with candle_audit_lock:
        for key in [key for key in latest_candle_audits if key[0] == symbol]:
            latest_candle_audits.pop(key, None)
        recent_validated_candles.pop(symbol, None)

    with _ai_snapshot_lock:
        for key in [key for key in _ai_snapshot_cache if key[0] == symbol]:
            _ai_snapshot_cache.pop(key, None)

    reset_live_symbol_state(symbol)

    rebuilt = {}
    warnings = []
    for timeframe in TIMEFRAMES:
        response = chart_data(timeframe_override=timeframe, include_logging=False, symbol_override=symbol)
        status_code = 200
        if isinstance(response, tuple):
            response, status_code = response
        payload = response.get_json() or {}
        if status_code >= 400 or payload.get("data_status") != "ok":
            return jsonify({
                "symbol": symbol,
                "data_status": "error",
                "errors": payload.get("errors") or [f"{timeframe} rebuild failed."],
                "read_only": True,
            }), status_code if status_code >= 400 else 500
        rebuilt[timeframe] = {
            "data_quality_status": payload.get("data_quality_status"),
            "candle_accuracy_mode": payload.get("candle_accuracy_mode"),
            "raw_candle_count": payload.get("raw_candle_count"),
            "validated_candle_count": payload.get("validated_candle_count"),
            "suspicious_candle_count": payload.get("suspicious_candle_count"),
            "rejected_candle_count": payload.get("rejected_candle_count"),
            "corrected_candle_count": payload.get("corrected_candle_count"),
            "candle_data_warnings": payload.get("candle_data_warnings") or [],
            "chart_line_count": len(payload.get("chart_lines") or []),
            "read_only": True,
        }
        warnings.extend(payload.get("candle_data_warnings") or [])

    ai_snapshot = build_ai_chart_snapshot("5Min", symbol)
    statuses = [item.get("data_quality_status") for item in rebuilt.values()]
    overall_status = "DEGRADED" if "DEGRADED" in statuses else ("WARNING" if "WARNING" in statuses else "CLEAN")
    return jsonify({
        "symbol": symbol,
        "data_status": "ok",
        "data_quality_status": overall_status,
        "candle_data_warnings": list(dict.fromkeys(warnings)),
        "timeframes": rebuilt,
        "ai_snapshot_context_rebuilt": bool(ai_snapshot),
        "message": "Chart data rebuilt from validated candles",
        "read_only": True,
    })


@APP.route("/api/ai/snapshot")
def ai_chart_snapshot():
    timeframe = request.args.get("timeframe", "5Min")
    symbol = request.args.get("symbol", SYMBOL)
    return jsonify(build_ai_chart_snapshot(timeframe, symbol))


@APP.route("/api/debug/chart-lines")
def debug_chart_lines():
    symbol = request.args.get("symbol", SYMBOL)
    timeframe = request.args.get("timeframe", "5Min")
    response = chart_data(timeframe_override=timeframe, include_logging=False, symbol_override=symbol)
    if isinstance(response, tuple):
        response = response[0]
    payload = response.get_json() or {}
    return jsonify({
        "symbol": payload.get("symbol", normalize_symbol(symbol)),
        "timeframe": payload.get("timeframe", timeframe),
        "line_count": len(payload.get("chart_lines") or []),
        "chart_lines": payload.get("chart_lines") or [],
        "warnings": payload.get("chart_line_warnings") or [],
        "data_quality_status": payload.get("data_quality_status"),
        "candle_accuracy_mode": payload.get("candle_accuracy_mode"),
        "candle_data_warnings": payload.get("candle_data_warnings") or [],
        "read_only": True,
    })


@APP.route("/api/debug/candles")
def debug_candles():
    symbol = normalize_symbol(request.args.get("symbol", SYMBOL))
    timeframe = request.args.get("timeframe", "5Min")
    timeframe = timeframe if timeframe in TIMEFRAMES else "5Min"
    response = chart_data(timeframe_override=timeframe, include_logging=False, symbol_override=symbol)
    if isinstance(response, tuple):
        response = response[0]
    with candle_audit_lock:
        audit = dict(latest_candle_audits.get((symbol, timeframe)) or {})
    limit = 500
    return jsonify({
        "symbol": symbol,
        "timeframe": timeframe,
        "data_quality_status": audit.get("data_quality_status", "DEGRADED"),
        "candle_accuracy_mode": audit.get("candle_accuracy_mode", "RAW_PROVIDER"),
        "raw_candles": (audit.get("raw_candles") or [])[-limit:],
        "validated_candles": (audit.get("validated_candles") or [])[-limit:],
        "suspicious_candles": (audit.get("suspicious_candles") or [])[-limit:],
        "rejected_candles": (audit.get("rejected_candles") or [])[-limit:],
        "candle_data_warnings": audit.get("candle_data_warnings") or [],
        "cross_timeframe_validation": (audit.get("cross_timeframe_validation") or [])[-limit:],
        "read_only": True,
    })


@APP.route("/api/debug/candle-compare")
def debug_candle_compare():
    symbol = normalize_symbol(request.args.get("symbol", SYMBOL))
    timeframe = request.args.get("timeframe", "5Min")
    timeframe = timeframe if timeframe in TIMEFRAMES else "5Min"
    response = chart_data(timeframe_override=timeframe, include_logging=False, symbol_override=symbol)
    if isinstance(response, tuple):
        response, status_code = response
        if status_code >= 400:
            return response, status_code
    with candle_audit_lock:
        audit = dict(latest_candle_audits.get((symbol, timeframe)) or {})
    comparison = build_candle_comparison(audit, symbol, timeframe)
    limit = 500
    for key in (
        "raw_alpaca_1min_bars",
        "raw_alpaca_provider_bars",
        "validated_1min_candles",
        "rebuilt_timeframe_candles",
        "displayed_candles",
        "rejected_candles",
        "suspicious_candles",
        "comparisons",
        "mismatches",
    ):
        comparison[key] = (comparison.get(key) or [])[-limit:]
    return jsonify(comparison)


@APP.route("/api/ai/latest-review")
def latest_ai_review():
    timeframe = request.args.get("timeframe", "5Min")
    symbol = request.args.get("symbol", SYMBOL)
    try:
        build_ai_chart_snapshot(timeframe, symbol)
    except Exception:
        pass

    with _latest_ai_review_lock:
        review = dict(LATEST_AI_REVIEW)
    if review.get("symbol") != normalize_symbol(symbol):
        review = empty_ai_review()
        review["symbol"] = normalize_symbol(symbol)
    return jsonify(apply_ai_event_metadata(review))


@APP.route("/api/ai/review-current-chart", methods=["GET", "POST"])
def review_current_chart():
    global LATEST_AI_REVIEW

    timeframe = request.args.get("timeframe", "5Min")
    symbol = request.args.get("symbol", SYMBOL)
    user_message = request.args.get("message")
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        timeframe = payload.get("timeframe", timeframe)
        symbol = payload.get("symbol", symbol)
        user_message = payload.get("user_message", payload.get("message", user_message))

    snapshot = build_ai_chart_snapshot(timeframe, symbol)
    review = build_ai_trade_review(snapshot, user_message=user_message)
    review["symbol"] = snapshot.get("symbol")
    review["active_symbol"] = snapshot.get("symbol")
    review = apply_ai_event_metadata(review, acknowledge=True)

    with _latest_ai_review_lock:
        LATEST_AI_REVIEW = review

    return jsonify(review)


@APP.route("/api/stream")
@APP.route("/api/stream/aapl")
def stream_chart():
    try:
        active_symbol = normalize_symbol(SYMBOL if request.path.endswith("/aapl") else request.args.get("symbol", SYMBOL))
    except ValueError as error:
        return jsonify({"error": str(error), "read_only": True}), 400
    tf = request.args.get("timeframe", "1Min")
    timeframe = tf if tf in TIMEFRAMES else "1Min"

    q = queue.Queue(maxsize=100)
    with stream_lock:
        requested_stream_symbols.add(active_symbol)
        live_candles.setdefault(active_symbol, {frame: None for frame in TIMEFRAMES})
        stream_status_by_symbol.setdefault(active_symbol, empty_stream_status(active_symbol))
        subscribers.setdefault((active_symbol, timeframe), []).append(q)
    ensure_stream_symbol_subscribed(active_symbol)

    def event_stream():
        try:
            while True:
                try:
                    event = q.get(timeout=15)
                    yield f"data: {json.dumps(event)}\n\n"
                except queue.Empty:
                    heartbeat = {
                        "type": "heartbeat",
                        "symbol": active_symbol,
                        "timeframe": timeframe,
                        "stream_status": get_stream_status(active_symbol),
                        "latest_trade": get_latest_trade(active_symbol),
                    }
                    yield f"data: {json.dumps(heartbeat)}\n\n"
        finally:
            with stream_lock:
                queues = subscribers.get((active_symbol, timeframe), [])
                if q in queues:
                    queues.remove(q)
                if not queues:
                    subscribers.pop((active_symbol, timeframe), None)

    return Response(event_stream(), mimetype="text/event-stream")






@APP.route("/performance")
def performance_dashboard():
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Professional Trading Dashboard</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #080b10;
      --panel: #101722;
      --panel-raised: #121c29;
      --border: #273549;
      --text: #e9eef6;
      --muted: #8e9caf;
      --accent: #6da8ff;
      --good: #70d99d;
      --warn: #e7bd68;
      --bad: #ef8585;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 28px 30px 34px;
      background: radial-gradient(circle at 50% -18%, #162339 0, var(--bg) 38%);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      min-width: 1100px;
    }
    h1 {
      margin: 0 0 7px;
      font-size: 28px;
      letter-spacing: -.03em;
    }
    .sub {
      color: var(--muted);
      margin-bottom: 24px;
      font-size: 13px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(140px, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }
    .card {
      background: linear-gradient(145deg, #121b28, #0e151f);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 15px;
      box-shadow: 0 10px 24px rgba(0,0,0,.18);
    }
    .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .04em;
    }
    .value {
      font-size: 24px;
      margin-top: 6px;
      font-weight: bold;
    }
    .warn {
      color: var(--warn);
    }
    .good {
      color: var(--good);
    }
    .bad {
      color: var(--bad);
    }
    .neutral {
      color: #b7c4d8;
    }
    .filter-panel {
      margin: 0 0 18px;
      padding: 15px;
      background: rgba(14, 21, 31, .88);
      border: 1px solid var(--border);
      border-radius: 13px;
    }
    .panel-heading {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      margin-bottom: 12px;
    }
    .panel-heading h2 {
      margin: 0;
      font-size: 15px;
      letter-spacing: -.01em;
    }
    .panel-heading span {
      color: var(--muted);
      font-size: 11px;
    }
    .controls {
      display: grid;
      grid-template-columns: repeat(6, minmax(130px, 1fr));
      gap: 10px;
      align-items: end;
      margin-bottom: 11px;
    }
    .control {
      display: flex;
      flex-direction: column;
      gap: 5px;
    }
    .control label {
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .04em;
    }
    button, select, input {
      background: #121c29;
      color: var(--text);
      border: 1px solid #304158;
      border-radius: 7px;
      padding: 8px 10px;
    }
    button {
      cursor: pointer;
      font-weight: 650;
      transition: background .15s ease, border-color .15s ease, opacity .15s ease;
    }
    button:hover, select:hover {
      background: #1a2a3d;
      border-color: #526c8b;
    }
    .status-row {
      display: flex;
      gap: 12px;
      align-items: center;
      margin: 10px 0 16px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 13px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      background: #0e1420;
      border: 1px solid var(--border);
      border-radius: 12px;
      overflow: hidden;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid #222d3d;
      text-align: left;
      font-size: 13px;
    }
    th {
      background: #131b28;
      color: #aebbd0;
      position: sticky;
      top: 0;
      z-index: 2;
    }
    tr:hover {
      background: #111b2a;
    }
    .pill {
      display: inline-block;
      padding: 4px 7px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid #34445b;
      background: #141d2b;
      white-space: nowrap;
    }
    .footer {
      color: #8f9bad;
      margin-top: 16px;
      font-size: 12px;
    }
    .empty {
      padding: 38px 24px;
      background: linear-gradient(145deg, #111a26, #0c121b);
      border: 1px solid var(--border);
      border-radius: 12px;
      color: var(--muted);
      text-align: center;
      line-height: 1.55;
    }
    .empty strong {
      display: block;
      color: #d9e2ee;
      font-size: 15px;
      margin-bottom: 5px;
    }
    .quick-buttons {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin: 0;
    }
    .view-tabs {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin: 0 0 12px;
    }
    .view-tabs button {
      padding: 7px 11px;
      font-size: 12px;
      background: #101927;
      color: #9fb0c5;
      border-color: #2b3b50;
    }
    .view-tabs button.active {
      background: #1d3149;
      color: #e2edf9;
      border-color: #5d83ad;
    }
    .quick-buttons button {
      padding: 6px 9px;
      font-size: 12px;
      background: transparent;
      color: #9fb0c5;
    }
    .ai-panel {
      background: linear-gradient(145deg, rgba(17, 29, 43, .98), rgba(10, 17, 27, .98));
      border: 1px solid #36506e;
      border-radius: 16px;
      padding: 20px;
      margin-bottom: 24px;
      box-shadow: 0 16px 36px rgba(0,0,0,.28), inset 0 1px rgba(255,255,255,.025);
    }
    .ai-panel-header {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 12px;
    }
    .ai-panel h2 {
      margin: 0;
      font-size: 20px;
      letter-spacing: -.02em;
    }
    #aiStatus {
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      padding: 5px 9px;
      border: 1px solid #34465e;
      border-radius: 999px;
      background: #111b29;
      font-size: 11px;
      font-weight: 700;
    }
    .ai-safety {
      color: #8493a7;
      font-size: 11px;
      margin: 8px 0 15px;
    }
    .ai-recommendation {
      color: var(--muted);
      font-size: 12px;
      margin: -6px 0 14px;
    }
    .ai-export-reminder {
      color: #b6a16a;
      font-size: 11px;
      margin: -7px 0 14px;
      padding: 7px 9px;
      border: 1px solid rgba(201, 166, 93, .26);
      border-radius: 9px;
      background: rgba(80, 63, 29, .14);
    }
    .ai-recommendation.active {
      color: var(--warn);
      font-weight: bold;
    }
    .ai-controls {
      display: grid;
      grid-template-columns: 100px 120px minmax(160px, auto) minmax(260px, 1fr) minmax(100px, auto);
      gap: 10px;
      align-items: end;
    }
    .ai-question {
      display: flex;
      flex-direction: column;
      gap: 5px;
    }
    .ai-question label {
      color: #9aa7b8;
      font-size: 11px;
      text-transform: uppercase;
    }
    .ai-review {
      margin-top: 18px;
      display: none;
    }
    .ai-review.visible {
      display: block;
    }
    .ai-review-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(140px, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }
    .ai-detail-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(240px, 1fr));
      gap: 10px;
    }
    .ai-section {
      background: rgba(16, 26, 39, .9);
      border: 1px solid #2a3b51;
      border-radius: 11px;
      padding: 13px;
    }
    .ai-section h3 {
      margin: 0 0 7px;
      color: #91a3b9;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .04em;
    }
    .ai-section p {
      margin: 0;
      line-height: 1.55;
      white-space: pre-wrap;
    }
    .ai-section ul {
      margin: 0;
      padding-left: 18px;
      line-height: 1.55;
    }
    .ai-decision {
      font-size: 22px;
      font-weight: bold;
    }
    .ai-decision.plan-ready { color: var(--good); }
    .ai-decision.watch { color: #80b8ed; }
    .ai-decision.wait { color: #b7c4d8; }
    .ai-decision.avoid { color: var(--bad); }
    .ai-marker-allowed { color: var(--good); }
    .ai-marker-blocked { color: #b7c4d8; }
    @media (max-width: 1000px) {
      .grid {
        grid-template-columns: repeat(2, minmax(140px, 1fr));
      }
      .controls {
        grid-template-columns: repeat(2, minmax(130px, 1fr));
      }
      .ai-controls, .ai-review-grid, .ai-detail-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <h1>Professional Trading Dashboard</h1>
  <div class="sub">
    Read-only setup intelligence, AI-assisted chart review, and measured performance context.
  </div>

  <section class="ai-panel" aria-labelledby="aiReviewTitle">
    <div class="ai-panel-header">
      <h2 id="aiReviewTitle">AI Trade Review</h2>
      <span id="aiStatus" class="neutral">Ready for review</span>
    </div>
    <div class="ai-safety">
      Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase.
    </div>
    <div id="aiRecommendation" class="ai-recommendation">No new AI review recommendation.</div>
    <div class="ai-export-reminder">Click AI Review before exporting if you want a fresh OpenAI review saved.</div>
    <div class="ai-controls">
      <div class="control">
        <label for="aiSymbol">Symbol</label>
        <input id="aiSymbol" type="text" value="AAPL" maxlength="10" />
      </div>
      <div class="control">
        <label for="aiTimeframe">Timeframe</label>
        <select id="aiTimeframe">
          <option value="1Min">1Min</option>
          <option value="5Min" selected>5Min</option>
          <option value="15Min">15Min</option>
          <option value="Daily">Daily</option>
        </select>
      </div>
      <button id="reviewCurrentChartButton" type="button" onclick="reviewCurrentChart()">Review Current Chart</button>
      <div class="ai-question">
        <label for="aiQuestion">Question</label>
        <input id="aiQuestion" type="text" placeholder="Ask the AI what it sees…" />
      </div>
      <button id="askAiButton" type="button" onclick="askAi()">Ask AI</button>
    </div>
    <div id="aiReview" class="ai-review" aria-live="polite">
      <div class="ai-review-grid">
        <div class="ai-section"><h3>Decision</h3><div id="aiDecision" class="ai-decision wait">WAIT</div></div>
        <div class="ai-section"><h3>Bias</h3><div id="aiBias">neutral</div></div>
        <div class="ai-section"><h3>Confidence</h3><div id="aiConfidence">0%</div></div>
        <div class="ai-section"><h3>Entry Marker</h3><div id="aiMarker" class="ai-marker-blocked">Not allowed</div></div>
      </div>
      <div class="ai-detail-grid">
        <div class="ai-section"><h3>Direct Answer</h3><p id="aiDirectAnswer"></p></div>
        <div class="ai-section"><h3>Application To Current Setup</h3><p id="aiCurrentApplication"></p></div>
        <div class="ai-section"><h3>Summary</h3><p id="aiSummary"></p></div>
        <div class="ai-section"><h3>What AI Sees</h3><p id="aiSees"></p></div>
        <div class="ai-section"><h3>Professional Reasoning</h3><p id="aiReasoning"></p></div>
        <div class="ai-section"><h3>Entry Conditions</h3><div id="aiEntryConditions"></div></div>
        <div class="ai-section"><h3>Trap Warnings</h3><div id="aiTrapWarnings"></div></div>
        <div class="ai-section"><h3>Options Risk Notes</h3><div id="aiOptionsRisk"></div></div>
        <div class="ai-section"><h3>Exit Plan</h3><div id="aiExitPlan"></div></div>
        <div class="ai-section"><h3>Manual Confirmation Checklist</h3><div id="aiChecklist"></div></div>
        <div class="ai-section"><h3>Warnings</h3><div id="aiWarnings"></div></div>
        <div class="ai-section"><h3>Do Not Chase</h3><p id="aiDoNotChase"></p></div>
        <div class="ai-section"><h3>Source</h3><p id="aiSource"></p></div>
      </div>
    </div>
  </section>

  <section class="filter-panel" aria-labelledby="performanceFiltersTitle">
    <div class="panel-heading">
      <h2 id="performanceFiltersTitle">Performance Filters</h2>
      <span id="viewDescription">Tradable Setups excludes NO_TRADE research rows by default.</span>
    </div>
    <div class="view-tabs" role="tablist" aria-label="Performance view">
      <button id="viewTradable" class="active" type="button" onclick="setPerformanceView('tradable')">Tradable Setups</button>
      <button id="viewResearch" type="button" onclick="setPerformanceView('research')">Research / NO_TRADE</button>
      <button id="viewAll" type="button" onclick="setPerformanceView('all')">All</button>
    </div>
    <div class="controls">
    <div class="control">
      <label>Log Limit</label>
      <select id="limit" onchange="loadData()">
        <option value="100">Last 100</option>
        <option value="500" selected>Last 500</option>
        <option value="1000">Last 1000</option>
        <option value="2500">Last 2500</option>
        <option value="5000">Last 5000</option>
      </select>
    </div>

    <div class="control">
      <label>Timeframe</label>
      <select id="timeframeFilter" onchange="render()">
        <option value="ALL">All</option>
      </select>
    </div>

    <div class="control">
      <label>Direction</label>
      <select id="directionFilter" onchange="render()">
        <option value="ALL">All</option>
      </select>
    </div>

    <div class="control">
      <label>Grade</label>
      <select id="gradeFilter" onchange="render()">
        <option value="ALL">All</option>
      </select>
    </div>

    <div class="control">
      <label>Source</label>
      <select id="sourceFilter" onchange="render()">
        <option value="ALL">All</option>
      </select>
    </div>

    <div class="control">
      <label>Horizon</label>
      <select id="horizonFilter" onchange="render()">
        <option value="ALL">All</option>
      </select>
    </div>

    <div class="control">
      <label>Min Count</label>
      <input id="minCountFilter" type="number" min="0" step="1" value="5" oninput="render()" />
    </div>

    <div class="control">
      <label>Sort By</label>
      <select id="sortBy" onchange="render()">
        <option value="bestEdge" selected>Best Edge</option>
        <option value="avgFav">Avg Favorable</option>
        <option value="avgAdv">Lowest Adverse</option>
        <option value="invalidRate">Lowest Invalidation</option>
        <option value="count">Most Samples</option>
        <option value="grade">Best Grade</option>
      </select>
    </div>

    <div class="control">
      <label>Action</label>
      <button onclick="loadData()">Refresh</button>
    </div>

    <div class="control">
      <label>Reset</label>
      <button onclick="resetFilters()">Clear Filters</button>
    </div>
    </div>

    <div class="quick-buttons">
      <button onclick="presetFiveMinQuality()">5Min A/B Only</button>
      <button onclick="presetNoTrade()">NO_TRADE Review</button>
      <button onclick="presetBullish()">Bullish Only</button>
      <button onclick="presetBearish()">Bearish Only</button>
    </div>
  </section>

  <div class="status-row">
    <span id="status" class="neutral">Loading...</span>
    <span id="filterStatus">Showing 0 rows</span>
  </div>

  <div class="grid">
    <div class="card">
      <div class="label">Total Outcomes</div>
      <div id="totalOutcomes" class="value">-</div>
    </div>
    <div class="card">
      <div class="label">Filtered Rows</div>
      <div id="filteredRows" class="value neutral">-</div>
    </div>
    <div class="card">
      <div class="label">Best Avg Favorable</div>
      <div id="bestFav" class="value good">-</div>
    </div>
    <div class="card">
      <div class="label">Best Edge</div>
      <div id="bestEdge" class="value good">-</div>
    </div>
  </div>

  <div id="empty" class="empty" style="display:none;">
    <strong>No performance data matches this view</strong>
    No logs may be available yet, or the current filters are too narrow. Let the chart run during market hours, then refresh this page.
  </div>

  <table id="table" style="display:none;">
    <thead>
      <tr>
        <th>Timeframe</th>
        <th>Horizon</th>
        <th>Direction</th>
        <th>Grade</th>
        <th>Intent</th>
        <th>Source</th>
        <th>Count</th>
        <th>Avg Favorable</th>
        <th>Avg Adverse</th>
        <th>Edge</th>
        <th>Invalidation</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>

  <div class="footer">
    Tip: Favorable/adverse are stock-price movement values, not option-contract profit/loss. Use count before trusting a result.
  </div>

  <script>
    let rawSummary = [];
    let rawData = null;
    let performanceView = "tradable";

    const gradeRank = {
      "A+": 5,
      "A": 4,
      "B": 3,
      "C": 2,
      "NO_TRADE": 1,
      "-": 0
    };

    function fmtNum(value) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
      return Number(value).toFixed(3).replace(/\.?0+$/, "");
    }

    function pct(value) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
      return `${(Number(value) * 100).toFixed(1)}%`;
    }

    function gradeClass(grade) {
      if (grade === "A+" || grade === "A") return "good";
      if (grade === "NO_TRADE") return "bad";
      if (grade === "B" || grade === "C") return "warn";
      return "neutral";
    }

    function setText(id, value) {
      document.getElementById(id).textContent = value === null || value === undefined || value === "" ? "-" : String(value);
    }

    function renderAiList(id, values) {
      const element = document.getElementById(id);
      const items = Array.isArray(values) ? values.filter(Boolean) : [];
      element.replaceChildren();

      if (!items.length) {
        element.textContent = "None";
        return;
      }

      const list = document.createElement("ul");
      for (const value of items) {
        const item = document.createElement("li");
        item.textContent = String(value);
        list.appendChild(item);
      }
      element.appendChild(list);
    }

    function renderAiReview(review) {
      document.getElementById("aiReview").classList.add("visible");
      updateAiRecommendation(review);

      const decision = review.decision || "WAIT";
      const decisionElement = document.getElementById("aiDecision");
      decisionElement.textContent = decision;
      decisionElement.className = `ai-decision ${String(decision).toLowerCase().replace("_", "-")}`;

      setText("aiBias", review.bias || "neutral");
      setText("aiConfidence", `${Number(review.confidence || 0)}%`);
      setText("aiDirectAnswer", review.direct_answer);
      setText("aiCurrentApplication", review.application_to_current_setup);
      setText("aiSummary", review.summary);
      setText("aiSees", review.what_ai_sees);
      setText("aiReasoning", review.professional_reasoning);
      setText("aiDoNotChase", review.do_not_chase);
      setText("aiSource", review.source || "fallback");

      const marker = document.getElementById("aiMarker");
      marker.textContent = review.allow_entry_marker ? "Allowed" : "Not allowed";
      marker.className = review.allow_entry_marker ? "ai-marker-allowed" : "ai-marker-blocked";

      renderAiList("aiEntryConditions", review.entry_conditions);
      renderAiList("aiTrapWarnings", review.trap_warnings);
      renderAiList("aiOptionsRisk", review.options_risk_notes);
      renderAiList("aiChecklist", review.manual_confirmation_checklist);
      renderAiList("aiWarnings", review.warnings);

      const exit = review.exit_plan || {};
      renderAiList("aiExitPlan", [
        `Invalidation: ${fmtNum(exit.invalidation)}`,
        `Target 1: ${fmtNum(exit.target_1)}`,
        `Target 2: ${fmtNum(exit.target_2)}`,
        `Target 3: ${fmtNum(exit.target_3)}`,
      ]);
    }

    function updateAiRecommendation(review) {
      const recommendation = document.getElementById("aiRecommendation");
      if (!recommendation) return;

      if (review && review.ai_review_recommended) {
        recommendation.textContent = `AI review recommended because: ${review.latest_event_reason || "actionable chart state changed."}`;
        recommendation.classList.add("active");
        return;
      }

      recommendation.classList.remove("active");
      recommendation.textContent = review && review.latest_event_reason
        ? `Last AI event: ${review.latest_event_reason}`
        : "No new AI review recommendation.";
    }

    async function loadLatestAiReview() {
      try {
        const response = await fetch("/api/ai/latest-review");
        if (!response.ok) return;
        updateAiRecommendation(await response.json());
      } catch (error) {
        console.warn("Unable to load latest AI review status", error);
      }
    }

    async function requestAiReview(userMessage = null) {
      const timeframe = document.getElementById("aiTimeframe").value;
      const symbol = document.getElementById("aiSymbol").value.trim().toUpperCase() || "AAPL";
      const status = document.getElementById("aiStatus");
      const reviewButton = document.getElementById("reviewCurrentChartButton");
      const askButton = document.getElementById("askAiButton");

      status.textContent = userMessage ? "Asking AI..." : "Reviewing current chart...";
      status.className = "neutral";
      reviewButton.disabled = true;
      askButton.disabled = true;

      try {
        const options = userMessage
          ? {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ symbol, timeframe, user_message: userMessage }),
            }
          : { method: "GET" };
        const url = userMessage
          ? "/api/ai/review-current-chart"
          : `/api/ai/review-current-chart?symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}`;
        const response = await fetch(url, options);
        const review = await response.json();
        if (!response.ok) throw new Error(review.error || `Review failed (${response.status})`);

        renderAiReview(review);
        status.textContent = `Updated ${new Date().toLocaleTimeString()} · ${review.source || "fallback"}`;
        status.className = review.decision === "AVOID" ? "bad" : review.decision === "PLAN_READY" ? "good" : "warn";
      } catch (error) {
        console.error(error);
        status.textContent = `AI review failed: ${error.message}`;
        status.className = "bad";
      } finally {
        reviewButton.disabled = false;
        askButton.disabled = false;
      }
    }

    function reviewCurrentChart() {
      return requestAiReview();
    }

    function askAi() {
      const input = document.getElementById("aiQuestion");
      const question = input.value.trim();
      if (!question) {
        const status = document.getElementById("aiStatus");
        status.textContent = "Enter a question first.";
        status.className = "warn";
        input.focus();
        return;
      }
      return requestAiReview(question);
    }

    document.getElementById("aiQuestion").addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        askAi();
      }
    });
    loadLatestAiReview();
    setInterval(loadLatestAiReview, 30000);

    function uniqueValues(rows, key) {
      return [...new Set(rows.map(row => row[key]).filter(v => v !== null && v !== undefined && v !== ""))]
        .sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true }));
    }

    function fillSelect(id, values, formatter = v => v) {
      const select = document.getElementById(id);
      const previous = select.value;
      select.innerHTML = `<option value="ALL">All</option>`;

      for (const value of values) {
        const option = document.createElement("option");
        option.value = String(value);
        option.textContent = formatter(value);
        select.appendChild(option);
      }

      if ([...select.options].some(opt => opt.value === previous)) {
        select.value = previous;
      }
    }

    function hydrateFilters(summary) {
      fillSelect("timeframeFilter", uniqueValues(summary, "timeframe"));
      fillSelect("directionFilter", uniqueValues(summary, "direction"));
      fillSelect("gradeFilter", uniqueValues(summary, "professional_grade"));
      fillSelect("sourceFilter", uniqueValues(summary, "source"));
      fillSelect("horizonFilter", uniqueValues(summary, "horizon_candles"), v => `${v} candles`);
    }

    function currentFilters() {
      return {
        timeframe: document.getElementById("timeframeFilter").value,
        direction: document.getElementById("directionFilter").value,
        grade: document.getElementById("gradeFilter").value,
        source: document.getElementById("sourceFilter").value,
        horizon: document.getElementById("horizonFilter").value,
        minCount: Number(document.getElementById("minCountFilter").value || 0),
        sortBy: document.getElementById("sortBy").value,
      };
    }

    function setPerformanceView(view) {
      performanceView = view;
      document.getElementById("viewTradable").classList.toggle("active", view === "tradable");
      document.getElementById("viewResearch").classList.toggle("active", view === "research");
      document.getElementById("viewAll").classList.toggle("active", view === "all");
      const description = document.getElementById("viewDescription");
      if (view === "tradable") {
        description.textContent = "Tradable Setups: A/B/C only, NO_TRADE excluded, minimum count defaults to 5.";
        if (document.getElementById("minCountFilter").value === "0") document.getElementById("minCountFilter").value = 5;
      } else if (view === "research") {
        description.textContent = "Research / NO_TRADE: weak zones, failed zones, and no-trade outcomes for study only.";
      } else {
        description.textContent = "All logged chart outcomes, including tradable and research/no-trade rows.";
      }
      render();
    }

    function rowEdge(row) {
      return Number(row.avg_favorable_move || 0) - Number(row.avg_adverse_move || 0);
    }

    function applyFilters(summary) {
      const f = currentFilters();

      return summary.filter(row => {
        const grade = String(row.professional_grade || "");
        const intent = String(row.setup_intent || "");
        const rowView = row.performance_view || (grade === "NO_TRADE" || intent === "NO TRADE" || intent === "RESEARCH CONTEXT" ? "research" : "tradable");
        if (performanceView === "tradable" && (rowView !== "tradable" || !["A+", "A", "B", "C"].includes(grade))) return false;
        if (performanceView === "research" && rowView !== "research") return false;
        if (f.timeframe !== "ALL" && String(row.timeframe) !== f.timeframe) return false;
        if (f.direction !== "ALL" && String(row.direction) !== f.direction) return false;
        if (f.grade !== "ALL" && String(row.professional_grade) !== f.grade) return false;
        if (f.source !== "ALL" && String(row.source) !== f.source) return false;
        if (f.horizon !== "ALL" && String(row.horizon_candles) !== f.horizon) return false;
        if (Number(row.count || 0) < f.minCount) return false;
        return true;
      });
    }

    function sortRows(rows) {
      const sortBy = document.getElementById("sortBy").value;
      const copy = [...rows];

      copy.sort((a, b) => {
        const timeframeRank = row => row.timeframe === "5Min" ? 0 : row.timeframe === "15Min" ? 1 : row.timeframe === "1Min" ? 2 : 3;
        const timeframeDelta = timeframeRank(a) - timeframeRank(b);
        if (performanceView === "tradable" && timeframeDelta !== 0) return timeframeDelta;
        if (sortBy === "avgFav") {
          return Number(b.avg_favorable_move || 0) - Number(a.avg_favorable_move || 0);
        }
        if (sortBy === "avgAdv") {
          return Number(a.avg_adverse_move || 0) - Number(b.avg_adverse_move || 0);
        }
        if (sortBy === "invalidRate") {
          return Number(a.invalidation_rate || 0) - Number(b.invalidation_rate || 0);
        }
        if (sortBy === "count") {
          return Number(b.count || 0) - Number(a.count || 0);
        }
        if (sortBy === "grade") {
          return (gradeRank[b.professional_grade] || 0) - (gradeRank[a.professional_grade] || 0);
        }

        // Best edge default.
        return rowEdge(b) - rowEdge(a);
      });

      return copy;
    }

    function renderCards(rows) {
      document.getElementById("filteredRows").textContent = rows.length;

      if (!rows.length) {
        document.getElementById("bestFav").textContent = "-";
        document.getElementById("bestEdge").textContent = "-";
        return;
      }

      const bestFav = [...rows].sort((a, b) => Number(b.avg_favorable_move || 0) - Number(a.avg_favorable_move || 0))[0];
      const bestEdge = [...rows].sort((a, b) => rowEdge(b) - rowEdge(a))[0];

      document.getElementById("bestFav").textContent =
        `${fmtNum(bestFav.avg_favorable_move)} ${bestFav.timeframe || ""}`;

      document.getElementById("bestEdge").textContent =
        `${fmtNum(rowEdge(bestEdge))} ${bestEdge.timeframe || ""}`;
    }

    function render() {
      const filtered = sortRows(applyFilters(rawSummary));
      const tbody = document.getElementById("tbody");
      const empty = document.getElementById("empty");
      const table = document.getElementById("table");

      document.getElementById("filterStatus").textContent =
        `Showing ${filtered.length} of ${rawSummary.length} grouped rows`;

      renderCards(filtered);

      if (!filtered.length) {
        empty.style.display = "block";
        table.style.display = "none";
        tbody.innerHTML = "";
        return;
      }

      empty.style.display = "none";
      table.style.display = "table";
      tbody.innerHTML = "";

      for (const row of filtered) {
        const tr = document.createElement("tr");
        const grade = row.professional_grade || "-";
        const edge = rowEdge(row);

        tr.innerHTML = `
          <td>${row.timeframe || "-"}</td>
          <td>${row.horizon_candles || "-"} candles</td>
          <td>${row.direction || "-"}</td>
          <td><span class="pill ${gradeClass(grade)}">${grade}</span></td>
          <td><span class="pill ${gradeClass(row.setup_intent === "NO TRADE" ? "NO_TRADE" : grade)}">${row.setup_intent || "-"}</span></td>
          <td>${row.source || "-"}</td>
          <td>${row.count || 0}</td>
          <td class="good">${fmtNum(row.avg_favorable_move)}</td>
          <td class="bad">${fmtNum(row.avg_adverse_move)}</td>
          <td class="${edge >= 0 ? "good" : "bad"}">${fmtNum(edge)}</td>
          <td>${pct(row.invalidation_rate)}</td>
        `;

        tbody.appendChild(tr);
      }
    }

    async function loadData() {
      const limit = document.getElementById("limit").value;
      const status = document.getElementById("status");
      status.textContent = "Loading...";
      status.className = "neutral";

      try {
        const res = await fetch(`/api/debug/setup-performance?limit=${limit}`, { cache: "no-store" });
        const data = await res.json();

        rawData = data;
        rawSummary = data.summary || [];

        document.getElementById("totalOutcomes").textContent = data.total_outcomes ?? 0;

        hydrateFilters(rawSummary);
        setPerformanceView(performanceView);
        render();

        status.textContent = `Updated ${new Date().toLocaleTimeString()}`;
        status.className = "good";
      } catch (err) {
        console.error(err);
        status.textContent = "Failed to load performance data";
        status.className = "bad";
      }
    }

    function resetFilters() {
      document.getElementById("timeframeFilter").value = "ALL";
      document.getElementById("directionFilter").value = "ALL";
      document.getElementById("gradeFilter").value = "ALL";
      document.getElementById("sourceFilter").value = "ALL";
      document.getElementById("horizonFilter").value = "ALL";
      document.getElementById("minCountFilter").value = performanceView === "tradable" ? 5 : 0;
      document.getElementById("sortBy").value = "bestEdge";
      render();
    }

    function presetFiveMinQuality() {
      resetFilters();
      if ([...document.getElementById("timeframeFilter").options].some(o => o.value === "5Min")) {
        document.getElementById("timeframeFilter").value = "5Min";
      }
      document.getElementById("sortBy").value = "grade";
      render();
    }

    function presetNoTrade() {
      resetFilters();
      setPerformanceView("research");
      if ([...document.getElementById("gradeFilter").options].some(o => o.value === "NO_TRADE")) {
        document.getElementById("gradeFilter").value = "NO_TRADE";
      }
      document.getElementById("sortBy").value = "count";
      render();
    }

    function presetBullish() {
      resetFilters();
      if ([...document.getElementById("directionFilter").options].some(o => o.value === "bullish")) {
        document.getElementById("directionFilter").value = "bullish";
      }
      render();
    }

    function presetBearish() {
      resetFilters();
      if ([...document.getElementById("directionFilter").options].some(o => o.value === "bearish")) {
        document.getElementById("directionFilter").value = "bearish";
      }
      render();
    }

    loadData();
    setInterval(loadData, 30000);
  </script>
</body>
</html>
    """



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
    symbol_arg = request.args.get("symbol")
    try:
        symbol = normalize_symbol(symbol_arg, SYMBOL) if symbol_arg else None
    except ValueError as error:
        return jsonify({"error": str(error), "read_only": True}), 400
    with stream_lock:
        requested = sorted(requested_stream_symbols)
        subscribed = sorted(subscribed_stream_symbols)
        statuses = {sym: dict(status) for sym, status in stream_status_by_symbol.items()}
        trades = {sym: dict(trade) for sym, trade in latest_trades.items()}
        candles = {
            sym: {tf: dict(candle) if isinstance(candle, dict) else None for tf, candle in frames.items()}
            for sym, frames in live_candles.items()
        }
        counts = _subscriber_count_unlocked(symbol)
    return jsonify({
        "stream_status": get_stream_status(symbol or SYMBOL) if symbol else statuses,
        "latest_trade": get_latest_trade(symbol or SYMBOL) if symbol else trades,
        "live_candles": get_live_candles_snapshot(symbol) if symbol else candles,
        "subscriber_counts": counts,
        "requested_symbols": requested,
        "subscribed_symbols": subscribed,
        "feed": FEED,
        "symbol": symbol or SYMBOL,
        "read_only": True,
    })


if __name__ == "__main__":
    loaded_log_keys = load_existing_setup_log_keys()
    print(f"Loaded setup log keys: {loaded_log_keys}")
    thread = threading.Thread(target=stream_worker, daemon=True)
    thread.start()

    APP.run(host="127.0.0.1", port=8900, debug=False, threaded=True)
