import unittest
from unittest import mock

from betfairdatabase.market import Market

MOCK_CTG_FILENAME = "1.22334455.json"


class TestMarket(unittest.TestCase):
    """
    Tests Market class.
    """

    @mock.patch("json.load")  # No need to parse anything
    @mock.patch("builtins.open")
    def test_market_catalogues_decoded_as_utf_8(self, mock_open, mock_json_load):
        """Market catalogues files are decoded as UTF-8."""
        market = Market(MOCK_CTG_FILENAME)
        market.market_catalogue_data  # Trigger parsing
        mock_open.assert_called_with(mock.ANY, encoding="utf-8")

    @mock.patch("json.load")
    @mock.patch("builtins.open")
    def test_racing_property(self, mock_open, mock_json_load):
        """Tests the racing property."""
        for event_type_id, is_racing_market in [
            ("7", True),
            ("4339", True),
            ("1", False),
        ]:
            with self.subTest(event_type_id=event_type_id):
                mock_json_load.return_value = {"eventType": {"id": event_type_id}}
                self.assertEqual(Market(MOCK_CTG_FILENAME).racing, is_racing_market)

        # Event type id not provided
        mock_json_load.return_value = {}
        self.assertEqual(Market(MOCK_CTG_FILENAME).racing, False)

    @mock.patch("json.load")
    @mock.patch("builtins.open")
    def test_incomplete_market_catalogue(self, mock_open, mock_json_load):
        """Incomplete market catalogue should not throw any errors."""
        mock_json_load.return_value = {}
        market = Market(MOCK_CTG_FILENAME)
        market._market_data_file = (
            "1.22334455"  # Must provide something, else error is thrown
        )
        # Test passes if no exception is raised
        market.create_sql_mapping()
