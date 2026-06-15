import unittest

import server_stream


def candle(time, open_price, high, low, close, volume=1000):
    return {
        "time": time,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


class SetupNoiseTests(unittest.TestCase):
    def bullish_indicators(self):
        return {
            "vwap": [{"time": 3, "value": 99.0}],
            "ema9": [{"time": 3, "value": 100.1}],
            "ema20": [{"time": 3, "value": 99.5}],
        }

    def demand_zone(self, grade="A", reaction_status="HOLD"):
        return {
            "low": 100.0,
            "high": 100.4,
            "label": f"{grade} Zone / test",
            "zone_quality_score": 85 if grade == "A" else 40,
            "zone_quality_grade": grade,
            "zone_quality_reasons": [],
            "reaction_status": reaction_status,
            "reaction_label": "DEMAND FAILED" if reaction_status == "FAILED" else "DEMAND HOLD",
            "read_only": True,
        }

    def test_failed_demand_does_not_create_bullish_setup(self):
        candles = [
            candle(1, 100.6, 100.8, 100.2, 100.5, 900),
            candle(2, 100.5, 100.7, 99.8, 100.25, 1800),
            candle(3, 100.25, 100.9, 100.2, 100.7, 2000),
        ]
        setups = server_stream.detect_confirmation_setups(
            candles,
            current_price=100.7,
            levels={},
            supply_demand={"demand": [self.demand_zone(reaction_status="FAILED")], "supply": []},
            indicators=self.bullish_indicators(),
            lookback=3,
        )

        self.assertFalse(
            any(setup["kind"] == "demand" and setup["direction"] == "bullish" for setup in setups["setups"])
        )

    def test_weak_demand_reclaim_is_capped_at_watch(self):
        candles = [
            candle(1, 100.6, 100.8, 100.2, 100.5, 900),
            candle(2, 100.5, 100.7, 99.8, 100.25, 1800),
            candle(3, 100.25, 100.9, 100.2, 100.7, 2000),
        ]
        setups = server_stream.detect_confirmation_setups(
            candles,
            current_price=100.7,
            levels={},
            supply_demand={"demand": [self.demand_zone(grade="WEAK")], "supply": []},
            indicators=self.bullish_indicators(),
            lookback=3,
        )

        self.assertTrue(setups["setups"])
        self.assertTrue(all(setup["confirmation_stage"] == "WATCH" for setup in setups["setups"]))
        self.assertTrue(any("Weak zone" in " ".join(setup["confirmation_warnings"]) for setup in setups["setups"]))


if __name__ == "__main__":
    unittest.main()
