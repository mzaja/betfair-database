import unittest
from pathlib import Path
from unittest import mock

from betfairdatabase.const import ENCODING_UTF_8
from betfairdatabase.market import Market

MOCK_METADATA_FILENAME = Path("1.22334455.json")
MOCK_DATA_FILENAME = Path("1.22334455.zip")


@mock.patch("json.loads")  # No need to parse anything
@mock.patch.object(Path, "read_text")
class TestMarket(unittest.TestCase):
    """
    Tests Market class.
    """

    def test_market_catalogues_decoded_as_utf_8(self, mock_read_text, mock_json_loads):
        """Market catalogues files are decoded as UTF-8."""
        market = Market(MOCK_METADATA_FILENAME, MOCK_DATA_FILENAME)
        market.metadata  # Trigger parsing
        mock_read_text.assert_called_with(encoding=ENCODING_UTF_8)

    def test_racing_property(self, mock_read_text, mock_json_loads):
        """Tests the racing property."""
        for event_type_id, is_racing_market in [
            ("7", True),
            ("4339", True),
            ("1", False),
        ]:
            with self.subTest(event_type_id=event_type_id):
                mock_json_loads.return_value = {"eventType": {"id": event_type_id}}
                print(Market(MOCK_METADATA_FILENAME, MOCK_DATA_FILENAME).metadata)
                print(type(Market(MOCK_METADATA_FILENAME, MOCK_DATA_FILENAME).metadata))
                self.assertEqual(
                    Market(MOCK_METADATA_FILENAME, MOCK_DATA_FILENAME).racing,
                    is_racing_market,
                )

        # Event type id not provided
        mock_json_loads.return_value = {}
        self.assertEqual(
            Market(MOCK_METADATA_FILENAME, MOCK_DATA_FILENAME).racing, False
        )

    def test_incomplete_market_catalogue(self, mock_read_text, mock_json_loads):
        """Incomplete market catalogue should not throw any errors."""
        mock_json_loads.return_value = {}
        market = Market(MOCK_METADATA_FILENAME, MOCK_DATA_FILENAME)
        # Test passes if no exception is raised
        market.create_sql_mapping()
