import unittest
from unittest import mock

from betfairdatabase.utils import parse_datetime, ImportPatterns


TIMESTAMP = "2023-06-01T17:09:37.000Z"
EVENT_ID = "12345678"


class TestUtils(unittest.TestCase):
    """
    Unit-tests utilities submodule.
    """

    def test_parse_datetime(self):
        """Tests parsing datetime in Betfair's format."""
        dt = parse_datetime(TIMESTAMP)
        self.assertEqual(dt.year, 2023)
        self.assertEqual(dt.month, 6)
        self.assertEqual(dt.day, 1)
        self.assertEqual(dt.hour, 17)
        self.assertEqual(dt.minute, 9)
        self.assertEqual(dt.second, 37)

    def test_import_pattern_betfair_historical(self):
        """Tests the official Betfair's folder naming pattern."""
        market_catalogue_data = {
            "marketStartTime": TIMESTAMP,
            "event": {"id": EVENT_ID},
        }
        pattern = ImportPatterns.betfair_historical(market_catalogue_data)
        self.assertEqual(pattern, f"2023/Jun/1/{EVENT_ID}")

    def test_import_pattern_event_id(self):
        """Tests the pattern of grouping markets by event ids."""
        market_catalogue_data = {"event": {"id": EVENT_ID}}
        pattern = ImportPatterns.event_id(market_catalogue_data)
        self.assertEqual(pattern, EVENT_ID)

    def test_import_pattern_flat(self):
        """Tests the pattern of storing all markets in the same folder."""
        pattern = ImportPatterns.flat({})
        self.assertEqual(pattern, "")
