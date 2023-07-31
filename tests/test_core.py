import unittest
from unittest import mock

from betfairdatabase.core import parse_market_catalogue


class CoreUnitTest(unittest.TestCase):
    """
    Unit-tests the core functionality of the package.
    """

    @mock.patch("json.load")  # No need to parse anything
    @mock.patch("builtins.open")
    def test_market_catalogues_decoded_as_utf_8(self, mock_open, mock_json_load):
        """Market catalogues files are decoded as UTF-8."""
        filename = "1.22334455.json"
        parse_market_catalogue(filename)
        mock_open.assert_called_with(filename, encoding="utf-8")
