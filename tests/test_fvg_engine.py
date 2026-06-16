import unittest
from datetime import datetime, timedelta, timezone
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


def alpaca_bar(timestamp, open_price, high, low, close, volume=1000):
    return {
        "t": timestamp.isoformat().replace("+00:00", "Z"),
        "o": open_price,
        "h": high,
        "l": low,
        "c": close,
        "v": volume,
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
        self.assertEqual(gap["reason"], "strict_3_candle_imbalance_with_displacement")
        self.assertGreaterEqual(gap["displacement_score"], 70)
        self.assertTrue(gap["middle_candle_closed_beyond_c1"])
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
        self.assertGreaterEqual(gap["displacement_score"], 70)
        self.assertTrue(gap["middle_candle_closed_beyond_c1"])
        self.assertEqual(gap["status"], "FILLED")
        self.assertIn("FVG is filled", " ".join(gap["warnings"]))

    def test_bullish_fvg_without_middle_displacement_is_weak_and_hidden(self):
        candles = [
            candle(1, 100.0, 100.2, 99.8, 100.1),
            candle(2, 100.25, 100.7, 100.15, 100.3),
            candle(3, 100.9, 101.1, 100.8, 101.0),
        ]

        gaps = server_stream.detect_fair_value_gaps(candles, symbol="AAPL", timeframe="1Min", atr14=1.0)
        gap = gaps["bullish"][0]

        self.assertEqual(gap["quality_grade"], "WEAK")
        self.assertFalse(gap["visible_in_clean_mode"])
        self.assertFalse(gap["worth_showing"])
        self.assertIn("insufficient displacement", gap["hidden_reason"])

    def test_bearish_fvg_without_middle_displacement_is_weak_and_hidden(self):
        candles = [
            candle(1, 101.0, 101.3, 100.8, 101.2),
            candle(2, 100.75, 100.9, 100.2, 100.7),
            candle(3, 99.8, 100.1, 99.4, 99.6),
        ]

        gaps = server_stream.detect_fair_value_gaps(candles, symbol="AAPL", timeframe="1Min", atr14=1.0)
        gap = gaps["bearish"][0]

        self.assertEqual(gap["quality_grade"], "WEAK")
        self.assertFalse(gap["visible_in_clean_mode"])
        self.assertFalse(gap["worth_showing"])
        self.assertIn("insufficient displacement", gap["hidden_reason"])

    def test_strong_bullish_displacement_becomes_clean_mode_candidate(self):
        candles = [
            candle(1, 100.0, 100.2, 99.8, 100.05),
            candle(2, 99.95, 101.7, 99.9, 101.45, 2500),
            candle(3, 101.1, 101.8, 100.85, 101.5, 2200),
        ]

        gaps = server_stream.detect_fair_value_gaps(
            candles, symbol="AAPL", timeframe="5Min", atr14=3.0,
            levels={"hod": 100.9},
        )
        gap = gaps["bullish"][0]

        self.assertIn(gap["quality_grade"], {"A", "B"})
        self.assertTrue(gap["middle_candle_closed_beyond_c1"])
        self.assertTrue(gap["middle_candle_engulfed_c1"])
        self.assertTrue(gap["visible_in_clean_mode"])

    def test_strong_bearish_displacement_becomes_clean_mode_candidate(self):
        candles = [
            candle(1, 101.0, 101.3, 100.8, 101.2),
            candle(2, 101.25, 101.35, 99.4, 99.65, 2500),
            candle(3, 99.8, 100.1, 99.2, 99.4, 2200),
        ]

        gaps = server_stream.detect_fair_value_gaps(
            candles, symbol="AAPL", timeframe="5Min", atr14=1.0,
            levels={"lod": 100.1},
        )
        gap = gaps["bearish"][0]

        self.assertIn(gap["quality_grade"], {"A", "B"})
        self.assertTrue(gap["middle_candle_closed_beyond_c1"])
        self.assertTrue(gap["middle_candle_engulfed_c1"])
        self.assertTrue(gap["visible_in_clean_mode"])

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
        self.assertEqual(fvg_line["reason"], "strict_3_candle_imbalance_with_displacement")
        self.assertTrue(fvg_line["extra_details"]["rule_passed"])
        self.assertEqual(fvg_line["extra_details"]["midpoint"], 100.5)
        self.assertGreaterEqual(fvg_line["extra_details"]["displacement_score"], 70)
        self.assertTrue(fvg_line["extra_details"]["middle_candle_closed_beyond_c1"])

    def test_duplicate_overlapping_fvgs_suppress_lower_priority_clean_display(self):
        candles = [
            candle(1, 100.0, 100.2, 99.8, 100.05),
            candle(2, 99.95, 100.7, 99.9, 100.6, 2500),
            candle(3, 100.5, 101.6, 100.5, 101.5, 2200),
            candle(4, 101.2, 101.7, 100.9, 101.5, 1800),
            candle(5, 101.5, 101.9, 101.2, 101.7, 1700),
        ]

        gaps = server_stream.detect_fair_value_gaps(
            candles, symbol="AAPL", timeframe="5Min", atr14=1.5,
            levels={"hod": 100.9},
        )
        clean_gaps = [gap for gap in gaps["bullish"] if gap["visible_in_clean_mode"]]
        hidden_duplicates = [gap for gap in gaps["bullish"] if gap.get("hidden_reason") and "duplicate" in gap["hidden_reason"]]

        self.assertEqual(len(clean_gaps), 1)
        self.assertGreaterEqual(len(hidden_duplicates), 1)

    def test_bad_aapl_print_cannot_create_validated_fvg(self):
        start = datetime(2026, 6, 12, 11, 45, tzinfo=timezone.utc)
        bars = []
        price = 292.85
        for index in range(20):
            timestamp = start + timedelta(minutes=index)
            close = price + (0.01 if index % 2 == 0 else -0.005)
            bars.append(alpaca_bar(timestamp, price, price + 0.04, price - 0.04, close, 1200))
            price = close
        bad_time = datetime(2026, 6, 12, 12, 4, tzinfo=timezone.utc)
        bars.append(alpaca_bar(bad_time, 292.9067, 293.0, 290.403, 290.403, 824))
        bars.append(alpaca_bar(bad_time + timedelta(minutes=1), 292.82, 292.9, 292.78, 292.84, 1300))
        bars.sort(key=lambda item: item["t"])
        bundle = server_stream.build_candle_integrity_bundle(bars, "AAPL", "1Min", bars)
        gaps = server_stream.detect_fair_value_gaps(bundle["validated_candles"], symbol="AAPL", timeframe="1Min", atr14=1.0)

        self.assertTrue(any(item["raw_values"]["low"] == 290.403 for item in bundle["rejected_candles"]))
        self.assertTrue(all(gap["bottom"] != 290.403 and gap["top"] != 290.403 for gap in gaps["all"]))

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
