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
        cls.all_source_files = {p.resolve() for p in cls.test_data_dir.rglob("1.*")}
        cls.catalogue_source_files = {
            p for p in cls.all_source_files if p.suffix == ".json"
        }
        cls.data_source_files = cls.all_source_files - cls.catalogue_source_files

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
        self.assertEqual(len(self.catalogue_source_files), 9)
        self.assertEqual(len(markets), 8)
        self.assertNotIn("1.199967351", [m["marketId"] for m in markets])

        # Test that column names wholly match the specification
        for market in markets:
            self.assertEqual(list(market.keys()), bfdb.columns())

        # Check that paths to files are absolute and correct.
        for market in markets:
            self.assertIn(
                Path(market["marketCatalogueFilePath"]), self.catalogue_source_files
            )
            self.assertIn(Path(market["marketDataFilePath"]), self.data_source_files)

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

    def test_select_columns_query(self):
        """Tests "columns" parameter of "select" method."""
        bfdb.index(self.test_data_dir)

        # Columns not specified - all returned in the right order
        markets = bfdb.select(self.test_data_dir)
        for market in markets:
            self.assertListEqual(list(market.keys()), bfdb.columns())

        # Columns specified and in non-standard order
        columns = ["marketCatalogueFilePath", "marketType", "marketId", "marketName"]
        markets = bfdb.select(self.test_data_dir, columns=columns)
        for market in markets:
            self.assertListEqual(list(market.keys()), columns)

    def test_select_where_query(self):
        """Tests "where" parameter of "select" method."""
        bfdb.index(self.test_data_dir)

        # AND operator
        markets = bfdb.select(
            self.test_data_dir, where="eventTypeName='Soccer' AND eventTimezone='GMT'"
        )
        self.assertEqual(len(markets), 2)
        for market in markets:
            self.assertEqual(market["eventTypeName"], "Soccer")
            self.assertEqual(market["eventTimezone"], "GMT")

        # OR and IN operators
        for query in [
            "eventCountryCode='GB' OR eventCountryCode='FR'",
            "eventCountryCode IN ('GB', 'FR')",
        ]:
            with self.subTest(query=query):
                markets = bfdb.select(
                    self.test_data_dir,
                    where=query,
                )
                self.assertEqual(len(markets), 6)
                for market in markets:
                    self.assertIn(market["eventCountryCode"], ("GB", "FR"))

        # BETWEEN operator
        markets = bfdb.select(self.test_data_dir, where="runners BETWEEN 6 AND 8")
        self.assertEqual(len(markets), 4)
        for market in markets:
            self.assertTrue(6 <= market["runners"] <= 8)

        # NOT operator
        markets = bfdb.select(self.test_data_dir, where="NOT eventTypeId='4339'")
        self.assertEqual(len(markets), 6)
        for market in markets:
            self.assertNotEqual(market["eventTypeId"], "4339")

        # NULL values
        markets = bfdb.select(self.test_data_dir, where="eventCountryCode IS NULL")
        self.assertEqual(len(markets), 2)
        for market in markets:
            self.assertIsNone(market["eventCountryCode"])

        markets = bfdb.select(self.test_data_dir, where="eventCountryCode IS NOT NULL")
        self.assertEqual(len(markets), 6)
        for market in markets:
            self.assertIsNotNone(market["eventCountryCode"])

    def test_select_limit_query(self):
        """Tests "limit" parameter of "select" method."""
        bfdb.index(self.test_data_dir)

        # No limit means return all
        markets = bfdb.select(self.test_data_dir)
        self.assertEqual(len(markets), 8)

        # Limit specified
        markets = bfdb.select(self.test_data_dir, limit=4)
        self.assertEqual(len(markets), 4)

    def test_select_return_options(self):
        """Tests return options of "select" method."""
        bfdb.index(self.test_data_dir)

        markets = bfdb.select(self.test_data_dir, return_dict=True)
        for market in markets:
            self.assertIsInstance(market, dict)

        markets = bfdb.select(self.test_data_dir, return_dict=False)
        for market in markets:
            self.assertIsInstance(market, tuple)

    def test_select_combined_queries(self):
        """Tests the combination of queries for selecting data."""
        bfdb.index(self.test_data_dir)

        for limit, market_count in zip([None, 2], [3, 2]):
            with self.subTest(limit=limit):
                columns = ["marketDataFilePath", "raceType", "runners"]
                markets = bfdb.select(
                    self.test_data_dir,
                    columns=columns,
                    where="eventTypeId IN ('7', '4339') AND eventCountryCode='GB'",
                    limit=limit,
                )
                self.assertEqual(len(markets), market_count)
                self.assertTrue(all(list(m.keys()) == columns for m in markets))

        markets = bfdb.select(
            self.test_data_dir,
            columns=["eventTypeId", "bspMarket"],
            where="eventTypeId IN ('7', '4339') AND bspMarket=true",
        )
        self.assertEqual(len(markets), 3)
        for market in markets:
            self.assertIn(market["eventTypeId"], ("7", "4339"))
            self.assertTrue(market["bspMarket"])
