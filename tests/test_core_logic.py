import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

import indicators
import market_time
import server_stream


ET = ZoneInfo("America/New_York")


def candle(time, open_price, high, low, close, volume=100):
    return {
        "time": time,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


class CoreLogicTests(unittest.TestCase):
    def test_normalize_symbol_and_invalid_rejection(self):
        self.assertEqual(server_stream.normalize_symbol("  msft "), "MSFT")
        self.assertEqual(server_stream.normalize_symbol("brk.b"), "BRK.B")
        with self.assertRaises(ValueError):
            server_stream.normalize_symbol("not a symbol")

    def test_previous_weekday_skips_weekend(self):
        self.assertEqual(market_time.previous_weekday(date(2026, 6, 15)), date(2026, 6, 12))
        self.assertEqual(market_time.previous_weekday(date(2026, 6, 16)), date(2026, 6, 15))

    def test_market_session_status_is_deterministic(self):
        weekend = market_time.build_market_session_status(datetime(2026, 6, 14, 10, tzinfo=ET))
        self.assertEqual(weekend["session_label"], "CLOSED")
        self.assertEqual(weekend["market_closed_reason"], "Weekend")
        regular = market_time.build_market_session_status(datetime(2026, 6, 15, 10, tzinfo=ET))
        self.assertEqual(regular["session_label"], "REGULAR")
        self.assertTrue(regular["is_regular_session_open"])
        premarket = market_time.build_market_session_status(datetime(2026, 6, 15, 8, tzinfo=ET))
        self.assertEqual(premarket["session_label"], "PREMARKET")

    def test_ema_and_vwap_use_candle_values(self):
        candles = [
            candle(1, 10, 11, 9, 10, 100),
            candle(2, 12, 13, 11, 12, 100),
            candle(3, 14, 15, 13, 14, 100),
        ]
        self.assertEqual([point["value"] for point in indicators.calc_ema(candles, 3)], [10, 11.0, 12.5])
        self.assertEqual([point["value"] for point in indicators.calc_vwap(candles)], [10.0, 11.0, 12.0])

    def test_cluster_levels_and_simple_support_resistance(self):
        clusters = indicators.cluster_levels([100.00, 100.04, 101.00])
        self.assertEqual(clusters, [{"price": 100.02, "touches": 2}, {"price": 101.0, "touches": 1}])
        candles = [
            candle(index, 100, high, low, 100)
            for index, (high, low) in enumerate([
                (101, 99), (102, 98), (103, 97), (105, 95), (103, 97), (102, 98), (101, 99),
            ])
        ]
        levels = server_stream.detect_support_resistance(candles, current_price=100, lookback=3)
        self.assertEqual(levels["support"], [{"price": 95.0, "touches": 1}])
        self.assertEqual(levels["resistance"], [{"price": 105.0, "touches": 1}])

    def test_fallback_review_stays_read_only_without_setup(self):
        review = server_stream.build_fallback_ai_review({
            "symbol": "AAPL",
            "requested_timeframe": "5Min",
            "data_quality_status": "CLEAN",
            "market_context": {},
            "timeframes": {},
            "market_session_status": market_time.build_market_session_status(datetime(2026, 6, 15, 10, tzinfo=ET)),
        })
        self.assertEqual(review["decision"], "WAIT")
        self.assertFalse(review["allow_entry_marker"])
        self.assertTrue(review["read_only"])
        self.assertTrue(review["not_financial_advice"])
        self.assertTrue(review["not_an_order"])
        self.assertIn("Not an order", review["summary"])


if __name__ == "__main__":
    unittest.main()
