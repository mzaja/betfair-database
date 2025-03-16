import unittest
from zoneinfo import ZoneInfo

from betfairdatabase.metadata import MarketMetadata

TIMESTAMP = "2023-06-01T17:09:37.000Z"

ZoneInfo("Europe/Moscow")


class TestUtils(unittest.TestCase):
    """Tests metadata submodule."""

    def test_transform_not_implemented_error(self):
        """Not overriding transfrom() method raises an error."""
        with self.assertRaises(NotImplementedError):
            MarketMetadata().transform()

    def test_calculate_local_times_no_time_provided(self):
        """Tests that all time arguments in _calculate_local_times() are optional."""
        self.assertEqual(MarketMetadata._calculate_local_times("Europe/London"), {})

    def test_calculate_local_times(self):
        """_calculate_local_times() corectly converts UTC timestamps to local times."""
        market_start_time = "2025-03-15T17:09:37.000Z"
        event_open_date = "2025-03-16T16:18:58.000Z"
        settled_time = "2025-03-17T23:39:22.000Z"

        # Test with GMT time (no change from UTC on this date due to winter time)
        times = MarketMetadata._calculate_local_times(
            "Europe/London", market_start_time, event_open_date, settled_time
        )
        self.assertEqual(
            times,
            {
                "localDayOfWeek": "Saturday",
                "localMarketStartTime": "2025-03-15 17:09:37+00:00",
                "localEventOpenDate": "2025-03-16 16:18:58+00:00",
                "localMarketSettledTime": "2025-03-17 23:39:22+00:00",
            },
        )

        # Test with GMT +3 time
        times = MarketMetadata._calculate_local_times(
            "Europe/Moscow", market_start_time, event_open_date, settled_time
        )
        self.assertEqual(
            times,
            {
                "localDayOfWeek": "Saturday",
                "localMarketStartTime": "2025-03-15 20:09:37+03:00",
                "localEventOpenDate": "2025-03-16 19:18:58+03:00",
                "localMarketSettledTime": "2025-03-18 02:39:22+03:00",
            },
        )
