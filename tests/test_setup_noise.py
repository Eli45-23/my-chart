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

    def test_chart_line_audit_keeps_sr_and_zones_available_for_clean_selection(self):
        snapshot = {
            "levels": {},
            "indicators": {},
            "support_resistance": {
                "support": [{"price": 99.8, "quality_grade": "WEAK", "quality_score": 42, "quality_reasons": ["nearest weak support"]}],
                "resistance": [{"price": 101.2, "quality_grade": "B", "quality_score": 72, "quality_reasons": ["nearest resistance"]}],
            },
            "supply_demand": {
                "demand": [self.demand_zone(grade="WEAK")],
                "supply": [{
                    "type": "supply",
                    "low": 101.0,
                    "high": 101.4,
                    "label": "B Zone / test",
                    "zone_quality_score": 72,
                    "zone_quality_grade": "B",
                    "zone_quality_reasons": ["nearest supply"],
                    "reaction_status": "ACTIVE",
                    "read_only": True,
                }],
            },
        }

        audit = server_stream.build_chart_line_registry(snapshot, "AAPL", "5Min")
        lines_by_type = {}
        for line in audit["chart_lines"]:
            lines_by_type.setdefault(line["type"], []).append(line)

        self.assertTrue(lines_by_type["SUPPORT"])
        self.assertTrue(lines_by_type["RESISTANCE"])
        self.assertTrue(lines_by_type["DEMAND_ZONE"])
        self.assertTrue(lines_by_type["SUPPLY_ZONE"])
        self.assertFalse(lines_by_type["SUPPORT"][0]["visible_in_clean_mode"])
        self.assertFalse(lines_by_type["DEMAND_ZONE"][0]["visible_in_clean_mode"])


if __name__ == "__main__":
    unittest.main()
