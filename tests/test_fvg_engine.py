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
        self.assertEqual(gap["bottom"], 100.2)
        self.assertEqual(gap["top"], 100.8)
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
        self.assertEqual(gap["bottom"], 100.1)
        self.assertEqual(gap["top"], 100.8)
        self.assertEqual(gap["status"], "FILLED")
        self.assertIn("FVG is filled", " ".join(gap["warnings"]))

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


if __name__ == "__main__":
    unittest.main()
