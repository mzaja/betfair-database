import unittest
import shutil
import tempfile
from collections import Counter
from pathlib import Path

import betfairdatabase as bfdb
from betfairdatabase.core import INDEX_FILENAME
from betfairdatabase.exceptions import IndexExistsError, IndexMissingError


class IntegrationTest(unittest.TestCase):
    """
    Integration test for the module.
    """

    TEST_DATA_DIR = Path("./tests/data")

    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = Path(tempfile.mkdtemp())
        shutil.copytree(cls.TEST_DATA_DIR, cls.test_data_dir, dirs_exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_data_dir)

    def setUp(self):
        """Erases all database indexes between test cases."""
        for index in self.test_data_dir.rglob(INDEX_FILENAME):
            index.unlink()
        self.assertListEqual(list(self.test_data_dir.rglob(INDEX_FILENAME)), [])

    def check_value_counts(
        self, markets: list[dict], column: str, expected_counts: dict[object, int]
    ):
        """Checks the counts of values in the dataset versus expected counts."""
        actual_counts = Counter(m[column] for m in markets)
        expected_counts = Counter(expected_counts)
        self.assertEqual(actual_counts, expected_counts)  # Order does not matter

    def test_index_already_exists(self):
        """Index already exists."""
        bfdb.index(self.test_data_dir)
        with self.assertRaises(IndexExistsError):
            bfdb.index(self.test_data_dir)
        # If it does not throw an error here, it passes
        bfdb.index(self.test_data_dir, overwrite=True)

    def test_index_does_not_exist(self):
        """Trying to fetch data from the database without indexing first."""
        with self.assertRaises(IndexMissingError):
            bfdb.select(self.test_data_dir)

    def test_data_integrity(self):
        """
        Tests that the data imported into the database matches the source data.
        """
        bfdb.index(self.test_data_dir)
        markets = bfdb.select(self.test_data_dir)  # Read all data

        # There are 9 market catalogues, but one is missing its market data
        # so it shouldn't get imported
        self.assertEqual(len(markets), 8)
        self.assertNotIn("1.199967351", [m["marketId"] for m in markets])

        # Test that column names wholly match the specification
        for market in markets:
            self.assertEqual(list(market.keys()), bfdb.columns())

        # Check counts of values
        self.check_value_counts(
            markets, "eventTypeId", {"1": 2, "4": 2, "7": 2, "4339": 2}
        )
        self.check_value_counts(
            markets,
            "competitionId",
            {"11365612": 1, "12247754": 2, "12596293": 1, None: 4},
        )
        self.check_value_counts(
            markets,
            "bettingType",
            {
                "ASIAN_HANDICAP_SINGLE_LINE": 1,
                "ASIAN_HANDICAP_DOUBLE_LINE": 1,
                "LINE": 1,
                "ODDS": 5,
            },
        )
        self.check_value_counts(
            markets,
            "priceLadderDescriptionType",
            {"FINEST": 2, "LINE_RANGE": 1, "CLASSIC": 5},
        )
        self.check_value_counts(
            markets,
            "marketType",
            {
                "ASIAN_HANDICAP": 1,
                "MATCH_ODDS": 1,
                "1_INNING_6_OVR_LINE": 1,
                "TEAMB_1ST_INN_RUNS": 1,
                "EACH_WAY": 1,
                "WIN": 2,
                "PLACE": 1,
            },
        )
        self.check_value_counts(
            markets, "eventVenue", {"Uttoxeter": 1, "Sheffield": 2, "DAX": 1, None: 4}
        )
        self.check_value_counts(markets, "raceType", {"Flat": 1, "Hurdle": 1, None: 6})
        self.check_value_counts(markets, "bspMarket", {1: 3, 0: 5})
        self.check_value_counts(markets, "turnInPlayEnabled", {1: 6, 0: 2})
        self.check_value_counts(markets, "persistenceEnabled", {1: 6, 0: 2})
        self.check_value_counts(
            markets, "lineRangeInfoMarketUnit", {"Goals": 1, "Runs": 1, None: 6}
        )
        self.check_value_counts(markets, "eachWayDivisor", {5.0: 1, None: 7})
        self.check_value_counts(
            markets, "eventCountryCode", {"GB": 5, "FR": 1, None: 2}
        )
