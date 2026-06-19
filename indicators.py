"""Pure price-series helpers used by chart and analysis engines."""


def calc_ema(candles, period):
    values = []
    multiplier = 2 / (period + 1)
    ema = None
    for candle in candles:
        close = candle["close"]
        ema = close if ema is None else (close - ema) * multiplier + ema
        values.append({"time": candle["time"], "value": round(ema, 4)})
    return values


def calc_vwap(candles):
    values = []
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    for candle in candles:
        typical = (candle["high"] + candle["low"] + candle["close"]) / 3
        volume = candle.get("volume") or 0
        cumulative_pv += typical * volume
        cumulative_volume += volume
        if cumulative_volume > 0:
            values.append({"time": candle["time"], "value": round(cumulative_pv / cumulative_volume, 4)})
    return values


def round_price(value):
    return None if value is None else round(float(value), 2)


def cluster_levels(levels, max_gap=0.08):
    if not levels:
        return []
    clusters = []
    for level in sorted(round_price(value) for value in levels if value is not None):
        if not clusters or abs(level - sum(clusters[-1]) / len(clusters[-1])) > max_gap:
            clusters.append([level])
        else:
            clusters[-1].append(level)
    return [{"price": round_price(sum(cluster) / len(cluster)), "touches": len(cluster)} for cluster in clusters]
