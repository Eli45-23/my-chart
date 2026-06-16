import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

import server_stream


ET = ZoneInfo("America/New_York")


def epoch_et(hour, minute):
    return int(datetime(2026, 6, 15, hour, minute, tzinfo=ET).timestamp())


def candle(time, open_price, high, low, close, volume=1000):
    return {
        "time": time,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


class FvgEngineTests(unittest.TestCase):
    def test_detects_bullish_fvg_and_partial_fill(self):
        candles = [
            candle(1, 100.0, 100.2, 99.8, 100.1),
            candle(2, 100.15, 101.4, 100.1, 101.2, 2000),
            candle(3, 101.5, 101.9, 100.8, 101.7, 1800),
            candle(4, 101.7, 101.8, 100.5, 101.0, 1500),
        ]

        gaps = server_stream.detect_fair_value_gaps(candles, symbol="AAPL", timeframe="1Min", atr14=1.0)
        gap = gaps["bullish"][0]

        self.assertEqual(gap["type"], "BULLISH_FVG")
        self.assertEqual(gap["direction"], "bullish")
        self.assertEqual(gap["bottom"], 100.2)
        self.assertEqual(gap["top"], 100.8)
        self.assertEqual(gap["midpoint"], 100.5)
        self.assertEqual(gap["candle1_time"], 1)
        self.assertEqual(gap["candle2_time"], 2)
        self.assertEqual(gap["candle3_time"], 3)
        self.assertEqual(gap["candle1_high"], 100.2)
        self.assertEqual(gap["candle3_low"], 100.8)
        self.assertTrue(gap["rule_passed"])
        self.assertEqual(gap["source"], "fvg_engine")
        self.assertEqual(gap["reason"], "strict_3_candle_imbalance")
        self.assertEqual(gap["status"], "PARTIALLY_FILLED")
        self.assertGreater(gap["fill_percentage"], 0)
        self.assertTrue(gap["read_only"])

    def test_detects_bearish_fvg_and_filled_status(self):
        candles = [
            candle(1, 101.0, 101.3, 100.8, 101.0),
            candle(2, 100.9, 101.0, 99.3, 99.5, 2200),
            candle(3, 99.2, 100.1, 98.9, 99.0, 1900),
            candle(4, 99.0, 100.9, 98.8, 100.5, 1700),
        ]

        gaps = server_stream.detect_fair_value_gaps(candles, symbol="AAPL", timeframe="5Min", atr14=1.0)
        gap = gaps["bearish"][0]

        self.assertEqual(gap["type"], "BEARISH_FVG")
        self.assertEqual(gap["direction"], "bearish")
        self.assertEqual(gap["bottom"], 100.1)
        self.assertEqual(gap["top"], 100.8)
        self.assertEqual(gap["midpoint"], 100.45)
        self.assertEqual(gap["candle1_low"], 100.8)
        self.assertEqual(gap["candle3_high"], 100.1)
        self.assertTrue(gap["rule_passed"])
        self.assertEqual(gap["status"], "FILLED")
        self.assertIn("FVG is filled", " ".join(gap["warnings"]))

    def test_no_bullish_fvg_when_candle_one_high_overlaps_candle_three_low(self):
        candles = [
            candle(1, 100.0, 100.8, 99.8, 100.4),
            candle(2, 100.4, 101.2, 100.2, 101.0),
            candle(3, 101.0, 101.4, 100.8, 101.2),
        ]

        gaps = server_stream.detect_fair_value_gaps(candles, symbol="AAPL", timeframe="1Min", atr14=1.0)

        self.assertEqual(gaps["bullish"], [])

    def test_no_bearish_fvg_when_candle_one_low_overlaps_candle_three_high(self):
        candles = [
            candle(1, 101.0, 101.4, 100.2, 100.6),
            candle(2, 100.6, 100.9, 99.7, 99.9),
            candle(3, 99.9, 100.2, 99.5, 99.7),
        ]

        gaps = server_stream.detect_fair_value_gaps(candles, symbol="AAPL", timeframe="1Min", atr14=1.0)

        self.assertEqual(gaps["bearish"], [])

    def test_demand_base_without_three_candle_imbalance_is_not_fvg(self):
        candles = [
            candle(1, 100.0, 100.4, 99.9, 100.2),
            candle(2, 100.2, 100.5, 100.0, 100.3),
            candle(3, 100.3, 100.6, 100.2, 100.5),
            candle(4, 100.5, 101.0, 100.3, 100.9),
        ]

        gaps = server_stream.detect_fair_value_gaps(candles, symbol="AAPL", timeframe="1Min", atr14=1.0)

        self.assertEqual(gaps["all"], [])

    def test_chart_line_audit_includes_fvg_midpoint_and_validation_proof(self):
        gaps = server_stream.detect_fair_value_gaps([
            candle(1, 100.0, 100.2, 99.8, 100.1),
            candle(2, 100.15, 101.4, 100.1, 101.2, 2000),
            candle(3, 101.5, 101.9, 100.8, 101.7, 1800),
        ], symbol="AAPL", timeframe="5Min", atr14=1.0)
        snapshot = {"levels": {}, "indicators": {}, "fair_value_gaps": gaps}

        audit = server_stream.build_chart_line_registry(snapshot, "AAPL", "5Min")
        fvg_line = next(line for line in audit["chart_lines"] if line["type"] == "BULLISH_FVG")

        self.assertEqual(fvg_line["price"], 100.5)
        self.assertEqual(fvg_line["top"], 100.8)
        self.assertEqual(fvg_line["bottom"], 100.2)
        self.assertEqual(fvg_line["source"], "fvg_engine")
        self.assertEqual(fvg_line["reason"], "strict_3_candle_imbalance")
        self.assertTrue(fvg_line["extra_details"]["rule_passed"])
        self.assertEqual(fvg_line["extra_details"]["midpoint"], 100.5)

    def test_clean_mode_hides_weak_and_filled_fvgs_in_audit_metadata(self):
        snapshot = {
            "levels": {},
            "indicators": {},
            "fair_value_gaps": {
                "bullish": [
                    {
                        "type": "BULLISH_FVG",
                        "top": 100.02,
                        "bottom": 100.0,
                        "midpoint": 100.01,
                        "status": "ACTIVE",
                        "quality_grade": "WEAK",
                        "quality_score": 40,
                        "worth_showing": False,
                        "source": "fvg_engine",
                        "reason": "strict_3_candle_imbalance",
                        "rule_passed": True,
                    },
                    {
                        "type": "BULLISH_FVG",
                        "top": 101.0,
                        "bottom": 100.5,
                        "midpoint": 100.75,
                        "status": "FILLED",
                        "quality_grade": "B",
                        "quality_score": 70,
                        "worth_showing": False,
                        "source": "fvg_engine",
                        "reason": "strict_3_candle_imbalance",
                        "rule_passed": True,
                    },
                ],
                "bearish": [],
            },
        }

        audit = server_stream.build_chart_line_registry(snapshot, "AAPL", "5Min")
        fvg_lines = [line for line in audit["chart_lines"] if line["type"] == "BULLISH_FVG"]

        self.assertEqual(len(fvg_lines), 2)
        self.assertTrue(all(not line["visible_in_clean_mode"] for line in fvg_lines))

    def test_opening_five_minute_levels_from_validated_minutes(self):
        today = [
            candle(epoch_et(9, 30), 100.0, 100.3, 99.9, 100.2),
            candle(epoch_et(9, 31), 100.2, 100.5, 100.1, 100.4),
            candle(epoch_et(9, 32), 100.4, 100.6, 100.2, 100.5),
            candle(epoch_et(9, 33), 100.5, 100.7, 100.3, 100.6),
            candle(epoch_et(9, 34), 100.6, 100.8, 100.4, 100.7),
            candle(epoch_et(9, 35), 100.7, 101.0, 100.6, 100.9),
        ]

        levels = server_stream.calc_levels(today, [], session_day=datetime(2026, 6, 15).date())

        self.assertTrue(levels["opening_5m_complete"])
        self.assertEqual(levels["opening_5m_high"], 100.8)
        self.assertEqual(levels["opening_5m_low"], 99.9)
        self.assertEqual(levels["hod"], 101.0)
        self.assertEqual(levels["lod"], 99.9)

    def test_core_key_levels_are_clean_mode_audit_visible(self):
        snapshot = {
            "levels": {
                "pmh": 103.0,
                "pml": 98.5,
                "pdh": 102.5,
                "pdl": 97.8,
                "hod": 101.2,
                "lod": 99.1,
                "opening_5m_high": 100.8,
                "opening_5m_low": 99.9,
            },
            "indicators": {},
        }

        audit = server_stream.build_chart_line_registry(snapshot, "AAPL", "5Min")
        lines_by_type = {line["type"]: line for line in audit["chart_lines"]}

        for line_type in ["PMH", "PML", "PDH", "PDL", "HOD", "LOD", "OPEN 5M HIGH", "OPEN 5M LOW"]:
            self.assertIn(line_type, lines_by_type)
            self.assertTrue(lines_by_type[line_type]["visible_in_clean_mode"])
            self.assertEqual(lines_by_type[line_type]["priority"], 1)
            self.assertEqual(lines_by_type[line_type]["status"], "ACTIVE")


if __name__ == "__main__":
    unittest.main()
