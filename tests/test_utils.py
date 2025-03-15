import unittest
from unittest import mock

from betfairdatabase.utils import parse_datetime

TIMESTAMP = "2023-06-01T17:09:37.000Z"


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
        self.assertEqual(dt.tzname(), "UTC")

    @mock.patch("betfairdatabase.utils.dt.datetime")
    def test_parse_datetime_3_10(self, mock_datetime):
        """
        Simulates parsing datetime on Python 3.10.
        This test is required to achieve 100 % test coverage.
        It will be removed in the future when 3.10 stops being supported.
        """
        mock_datetime.fromisoformat.side_effect = (ValueError, mock.Mock())
        parse_datetime(TIMESTAMP)
        mock_datetime.fromisoformat.assert_called_with("2023-06-01T17:09:37.000")
