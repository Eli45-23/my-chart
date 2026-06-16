import queue
import unittest
from datetime import datetime, timezone

import server_stream


class StreamSymbolTests(unittest.TestCase):
    def setUp(self):
        with server_stream.stream_lock:
            server_stream.subscribers.clear()
            server_stream.live_candles.clear()
            server_stream.latest_trades.clear()
            server_stream.stream_status_by_symbol.clear()
            server_stream.requested_stream_symbols.clear()
            server_stream.requested_stream_symbols.add(server_stream.SYMBOL)
            server_stream.subscribed_stream_symbols.clear()

    def test_normalize_symbol_accepts_generic_stock_and_etf_symbols(self):
        for symbol in ["AAPL", "NVDA", "TSLA", "BRK.B", "IWM", "XLK", "ABC-DEF"]:
            self.assertEqual(server_stream.normalize_symbol(symbol.lower()), symbol)

    def test_normalize_symbol_rejects_invalid_symbols(self):
        for symbol in ["TOO-LONG-SYMBOL", "BAD SYMBOL", "$AAPL", "AAPL/WS"]:
            with self.assertRaises(ValueError):
                server_stream.normalize_symbol(symbol)

    def test_related_market_mapping_never_includes_selected_symbol(self):
        for symbol in ["AAPL", "SPY", "QQQ", "IWM", "NVDA", "TSLA", "MSFT"]:
            related = server_stream.get_related_market_symbols(symbol)
            self.assertNotIn(symbol, [value for value in related.values() if value])

    def test_live_candle_updates_are_isolated_by_symbol(self):
        nvda_queue = queue.Queue()
        aapl_queue = queue.Queue()
        with server_stream.stream_lock:
            server_stream.subscribers[("NVDA", "1Min")] = [nvda_queue]
            server_stream.subscribers[("AAPL", "1Min")] = [aapl_queue]

        server_stream.update_live_candles(
            "NVDA",
            150.25,
            100,
            datetime(2026, 6, 16, 14, 30, tzinfo=timezone.utc),
        )

        event = nvda_queue.get_nowait()
        self.assertEqual(event["symbol"], "NVDA")
        self.assertEqual(event["timeframe"], "1Min")
        self.assertEqual(event["latest_trade"]["price"], 150.25)
        self.assertEqual(server_stream.get_latest_trade("NVDA")["price"], 150.25)
        self.assertIsNone(server_stream.get_latest_trade("AAPL"))
        self.assertTrue(aapl_queue.empty())

    def test_reset_live_symbol_state_does_not_clear_other_symbols(self):
        server_stream.update_live_candles(
            "NVDA",
            150.25,
            100,
            datetime(2026, 6, 16, 14, 30, tzinfo=timezone.utc),
        )
        server_stream.update_live_candles(
            "AAPL",
            290.5,
            50,
            datetime(2026, 6, 16, 14, 30, tzinfo=timezone.utc),
        )

        server_stream.reset_live_symbol_state("NVDA")

        self.assertIsNone(server_stream.get_latest_trade("NVDA"))
        self.assertEqual(server_stream.get_latest_trade("AAPL")["price"], 290.5)
        self.assertTrue(all(value is None for value in server_stream.get_live_candles_snapshot("NVDA").values()))


if __name__ == "__main__":
    unittest.main()
