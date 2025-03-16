import unittest
from unittest import mock

from betfairdatabase.imports import ImportPatterns
from betfairdatabase.metadata import MarketCatalogueData, MarketDefinitionData

EVENT_ID = "12345678"


class TestUtils(unittest.TestCase):
    """Tests imports submodule."""

    def test_import_pattern_betfair_historical(self):
        """Tests the official Betfair's folder naming pattern."""
        # Without settled time
        market_time = "2023-06-01T17:09:37.000Z"
        market_catalogue_data = MarketCatalogueData(
            {
                "marketStartTime": market_time,
                "event": {"id": EVENT_ID},
            }
        )
        market_definition_data = MarketDefinitionData(
            {"marketTime": market_time, "eventId": EVENT_ID}
        )
        for metadata in [market_catalogue_data, market_definition_data]:
            market = mock.Mock(metadata=metadata)
        pattern = ImportPatterns.betfair_historical(market)
        self.assertEqual(pattern, f"2023/Jun/1/{EVENT_ID}")

        # Settled time overrides the market start time
        extra_data = {"settledTime": "2025-03-15T16:02:58.000Z"}
        market_catalogue_data.update(extra_data)
        market_definition_data.update(extra_data)
        for metadata in [market_catalogue_data, market_definition_data]:
            market = mock.Mock(metadata=metadata)
        pattern = ImportPatterns.betfair_historical(market)
        self.assertEqual(pattern, f"2025/Mar/15/{EVENT_ID}")

    def test_import_pattern_event_id(self):
        """Tests the pattern of grouping markets by event ids."""
        market_catalogue_data = MarketCatalogueData({"event": {"id": EVENT_ID}})
        market_definition_data = MarketDefinitionData({"eventId": EVENT_ID})
        for metadata in [market_catalogue_data, market_definition_data]:
            pattern = ImportPatterns.event_id(mock.Mock(metadata=metadata))
            self.assertEqual(pattern, EVENT_ID)

    def test_import_pattern_flat(self):
        """Tests the pattern of storing all markets in the same folder."""
        pattern = ImportPatterns.flat(None)  # Argument is unused
        self.assertEqual(pattern, "")
