import unittest
from datetime import datetime, timedelta, timezone

import server_stream


def bar(timestamp, open_price, high, low, close, volume=1000):
    return {
        "t": timestamp.isoformat().replace("+00:00", "Z"),
        "o": open_price,
        "h": high,
        "l": low,
        "c": close,
        "v": volume,
    }


class CandleAccuracyTests(unittest.TestCase):
    def setUp(self):
        start = datetime(2026, 6, 12, 11, 45, tzinfo=timezone.utc)
        self.normal = []
        price = 292.85
        for index in range(20):
            timestamp = start + timedelta(minutes=index)
            close = price + (0.01 if index % 2 == 0 else -0.005)
            self.normal.append(bar(timestamp, price, price + 0.04, price - 0.04, close, 1200))
            price = close

    def test_normal_sequence_remains_valid(self):
        bundle = server_stream.build_candle_integrity_bundle(self.normal, "AAPL", "1Min", self.normal)
        self.assertEqual(bundle["rejected_candle_count"], 0)
        self.assertEqual(len(bundle["validated_candles"]), len(self.normal))

    def test_structurally_invalid_candle_is_rejected(self):
        invalid = list(self.normal)
        invalid[10] = bar(
            datetime(2026, 6, 12, 11, 55, tzinfo=timezone.utc),
            292.9,
            292.7,
            292.8,
            292.75,
            1000,
        )
        bundle = server_stream.build_candle_integrity_bundle(invalid, "AAPL", "1Min", invalid)
        self.assertEqual(bundle["rejected_candle_count"], 1)

    def test_conflicting_duplicate_rejects_both_versions(self):
        duplicate = list(self.normal[:10])
        duplicate.append({
            **duplicate[-1],
            "l": duplicate[-1]["l"] - 0.5,
            "c": duplicate[-1]["c"] - 0.25,
        })
        bundle = server_stream.build_candle_integrity_bundle(duplicate, "AAPL", "1Min", duplicate)
        self.assertEqual(bundle["rejected_candle_count"], 2)

    def test_reproduced_bad_print_is_rejected_and_not_in_rebuild(self):
        bars = list(self.normal)
        bad_time = datetime(2026, 6, 12, 12, 4, tzinfo=timezone.utc)
        bars.append(bar(bad_time, 292.9067, 293.0, 290.403, 290.403, 824))
        bars.append(bar(bad_time + timedelta(minutes=1), 292.82, 292.9, 292.78, 292.84, 1300))
        bars.sort(key=lambda item: item["t"])

        bundle = server_stream.build_candle_integrity_bundle(bars, "AAPL", "5Min", bars)

        self.assertTrue(any(item["raw_values"]["low"] == 290.403 for item in bundle["rejected_candles"]))
        self.assertTrue(all(item["low"] != 290.403 for item in bundle["validated_candles"]))
        levels = server_stream.calc_levels(
            bundle["validated_1min_candles"],
            [],
            session_day=datetime(2026, 6, 12).date(),
        )
        self.assertNotEqual(levels["pml"], 290.403)

    def test_rebuild_excludes_bucket_below_sixty_percent_coverage(self):
        audited, validated = server_stream.validate_raw_candles(
            server_stream.raw_candles_from_bars(self.normal[:5], "AAPL", "1Min")
        )
        for candle in audited[:3]:
            candle["validation_status"] = "REJECTED"
            candle["excluded_from_calculations"] = True
            candle["excluded_from_display"] = True
        retained = [candle for candle in audited if not candle["excluded_from_calculations"]]
        rebuilt = server_stream.aggregate_validated_candles(retained, "5Min", audited)
        self.assertEqual(rebuilt, [])


if __name__ == "__main__":
    unittest.main()
