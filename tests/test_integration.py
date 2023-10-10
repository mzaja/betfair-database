import csv
import shutil
import tempfile
import unittest
from collections import Counter
from pathlib import Path

import betfairdatabase as bfdb
from betfairdatabase.const import INDEX_FILENAME
from betfairdatabase.exceptions import IndexExistsError, IndexMissingError


class TestIntegrationBase(unittest.TestCase):
    """
    Base integration test scenario.
    """

    TEST_DATA_DIR_SRC = Path("./tests/data/datasets")

    @classmethod
    def setup_test_fixtures(cls):
        """
        1. Creates a temporary directory.
        2. Copies test data to it.
        3. Lists the copied files into categories.
        """
        cls.test_data_dir = Path(tempfile.mkdtemp()).resolve()
        shutil.copytree(cls.TEST_DATA_DIR_SRC, cls.test_data_dir, dirs_exist_ok=True)
        cls.all_source_files = {p.resolve() for p in cls.test_data_dir.rglob("1.*")}
        cls.catalogue_source_files = {
            p for p in cls.all_source_files if p.suffix == ".json"
        }
        cls.data_source_files = cls.all_source_files - cls.catalogue_source_files

    @classmethod
    def teardown_test_fixtures(cls):
        """Removes the temporary directory containing test data."""
        shutil.rmtree(cls.test_data_dir)

    @classmethod
    def setUpClass(cls):
        cls.setup_test_fixtures()

    @classmethod
    def tearDownClass(cls):
        cls.teardown_test_fixtures()

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


class TestIntegrationPart1(TestIntegrationBase):
    """
    Integration test for the module.

    The tests in this class require copying the test data only once.
    """

    def test_index_already_exists(self):
        """Index already exists."""
        bfdb.index(self.test_data_dir)
        with self.assertRaises(IndexExistsError):
            bfdb.index(self.test_data_dir)
        # If it does not throw an error here, it passes
        bfdb.index(self.test_data_dir, force=True)

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

        # There are 10 market catalogues, but one is missing its market data
        # so it shouldn't get imported
        self.assertEqual(len(self.catalogue_source_files), 10)
        self.assertEqual(len(markets), 9)
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
            markets, "eventTypeId", {"1": 2, "4": 2, "7": 2, "4339": 3}
        )
        self.check_value_counts(
            markets,
            "competitionId",
            {"11365612": 1, "12247754": 2, "12596293": 1, None: 5},
        )
        self.check_value_counts(
            markets,
            "bettingType",
            {
                "ASIAN_HANDICAP_SINGLE_LINE": 1,
                "ASIAN_HANDICAP_DOUBLE_LINE": 1,
                "LINE": 1,
                "ODDS": 6,
            },
        )
        self.check_value_counts(
            markets,
            "priceLadderDescriptionType",
            {"FINEST": 2, "LINE_RANGE": 1, "CLASSIC": 6},
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
                "TRAP_CHALLENGE": 1,
            },
        )
        self.check_value_counts(
            markets, "eventVenue", {"Uttoxeter": 1, "Sheffield": 2, "DAX": 1, None: 5}
        )
        self.check_value_counts(markets, "raceType", {"Flat": 1, "Hurdle": 1, None: 7})
        self.check_value_counts(markets, "bspMarket", {1: 3, 0: 6})
        self.check_value_counts(markets, "turnInPlayEnabled", {1: 7, 0: 2})
        self.check_value_counts(markets, "persistenceEnabled", {1: 7, 0: 2})
        self.check_value_counts(
            markets, "lineRangeInfoMarketUnit", {"Goals": 1, "Runs": 1, None: 7}
        )
        self.check_value_counts(markets, "eachWayDivisor", {5.0: 1, None: 8})
        self.check_value_counts(
            markets, "eventCountryCode", {"GB": 6, "FR": 1, None: 2}
        )

        # Value counts for additional metadata
        self.check_value_counts(
            markets,
            "localDayOfWeek",
            {"Friday": 4, "Tuesday": 2, "Thursday": 2, "Monday": 1},
        )
        self.check_value_counts(
            markets, "raceTypeFromName", {"OR": 2, "Mdn Claim": 1, None: 6}
        )
        meters_counter = Counter(m["raceDistanceMeters"] for m in markets)
        self.assertEqual(meters_counter[280], 2)
        self.assertEqual(meters_counter[None], 6)
        furlongs_counter = Counter(m["raceDistanceFurlongs"] for m in markets)
        self.assertEqual(furlongs_counter[6], 1)
        self.assertEqual(furlongs_counter[None], 6)
        self.assertEqual(Counter(m["raceId"] for m in markets)[None], 6)

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
                self.assertEqual(len(markets), 7)
                for market in markets:
                    self.assertIn(market["eventCountryCode"], ("GB", "FR"))

        # BETWEEN operator
        markets = bfdb.select(self.test_data_dir, where="runners BETWEEN 6 AND 8")
        self.assertEqual(len(markets), 5)
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
        self.assertEqual(len(markets), 7)
        for market in markets:
            self.assertIsNotNone(market["eventCountryCode"])

        # Date and time
        # SQLite does not have a dedicated datetime type, so dates and times are
        # represented as strings, in ISO-8601 format (also used by Betfair)
        # Find all markets from the year 2022
        markets = bfdb.select(
            self.test_data_dir,
            where="marketStartTime BETWEEN '2022-01-01' AND '2022-12-31'",
        )
        self.assertEqual(len(markets), 3)
        for market in markets:
            self.assertEqual(market["marketStartTime"][:4], "2022")

        # Find all markets starting before noon (UTC)
        markets = bfdb.select(
            self.test_data_dir,
            where="time(marketStartTime) < '12:00:00'",
        )
        self.assertEqual(len(markets), 4)
        for market in markets:
            self.assertLess(market["marketStartTime"].split("T")[1][:-1], "12:00:00")

        # Find all markets starting in April
        markets = bfdb.select(
            self.test_data_dir,
            where="strftime('%m', marketStartTime) == '04'",
        )
        self.assertEqual(len(markets), 2)
        for market in markets:
            self.assertEqual(market["marketStartTime"].split("-")[1], "04")

    def test_select_limit_query(self):
        """Tests "limit" parameter of "select" method."""
        bfdb.index(self.test_data_dir)

        # No limit means return all
        markets = bfdb.select(self.test_data_dir)
        self.assertEqual(len(markets), 9)

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

        for limit, market_count in ((None, 4), (2, 2)):
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

    def test_export_to_csv(self):
        """Tests exporting the whole database index to a CSV file."""
        bfdb.index(self.test_data_dir)

        # Create an output directory for the CSV file
        output_dir = self.test_data_dir / "output"
        output_dir.mkdir(exist_ok=True)

        for dest, output_filename in [
            (output_dir, self.test_data_dir.name + ".csv"),
            (output_dir / "abc.csv", "abc.csv"),
        ]:
            with self.subTest(dest=dest):
                # Export the index to a CSV file in the same directory
                csv_file = bfdb.export(self.test_data_dir, dest)
                # File name is inherited from the database if not provided
                self.assertEqual(csv_file.name, output_filename)
                # File is exported to the destination directory
                self.assertEqual(csv_file.parent, output_dir)

                # Validate data integrity
                markets = bfdb.select(self.test_data_dir)
                # Convert everything to strings since the CSV file is read as such
                for market in markets:
                    for key, value in market.items():
                        if value is None:
                            market[key] = ""
                        else:
                            market[key] = str(value)

                # Compare output (CSV file) and source (database) data
                with open(csv_file, "r") as f:
                    reader = csv.DictReader(f)
                    for m1, m2 in zip(reader, markets):
                        self.assertEqual(m1, m2)


class TestIntegrationPart2(TestIntegrationBase):
    """
    Tests insert() and clean() methods.

    The tests in this class require copying the test data once per test.
    """

    @classmethod
    def setUpClass(cls):
        pass  # Do nothing because setup code runs once per test

    @classmethod
    def tearDownClass(cls):
        pass  # Do nothing because teardown code runs once per test

    def setUp(self):
        self.setup_test_fixtures()
        self.dataset_1 = self.test_data_dir / "uncompressed"
        self.dataset_2 = self.test_data_dir / "zip-lzma"
        self.database_dir = self.test_data_dir / "test_db"
        self.database_dir.mkdir()

    def tearDown(self):
        self.teardown_test_fixtures()

    def duplicates_test_setup(self) -> Path:
        """
        Sets up the test cases using duplicate files and returns the path to duplicates
        source folder.
        """
        bfdb.insert(self.database_dir, self.dataset_1)
        bfdb.insert(self.database_dir, self.dataset_2)
        # Duplicates are:
        # 1.201590187.zip:  Larger than the existing file.
        # 1.216395251.json: marketTime changed to "2023-07-28T02:35:00.000Z".
        # 1.216418252.json: totalMatched changed to 134763.88 (irrelevant change)
        # The rest: Content identical to originals.
        dest_dir = self.test_data_dir / "duplicates"
        shutil.copytree("./tests/data/duplicates", dest_dir, dirs_exist_ok=True)
        return dest_dir

    def base_test_move_insert(self, copy: bool, leftover_files: list[list[str]]):
        """Base test for testing copy/move style insert method."""
        test_cases = [
            (self.dataset_1, 5, leftover_files[0]),
            (self.dataset_2, 9, leftover_files[1]),
        ]
        for dataset, market_count, leftover_src_files in test_cases:
            with self.subTest():
                bfdb.insert(self.database_dir, dataset, copy=copy)
                markets = bfdb.select(self.database_dir)
                self.assertEqual(len(markets), market_count)
                for market in markets:
                    for file_type in ("marketCatalogueFilePath", "marketDataFilePath"):
                        self.assertTrue(Path(market[file_type]).exists())
                self.assertEqual(
                    [f.name for f in dataset.iterdir()], leftover_src_files
                )

    def test_move_to_empty_database(self):
        """Tests updating the new database by moving the files into it."""
        self.base_test_move_insert(False, [["1.199967351.json"], []])

    def test_copy_to_empty_database(self):
        """Tests updating the new database by copying the files into it."""
        self.base_test_move_insert(
            True,
            [
                # Source files are still intact
                [f.name for f in self.dataset_1.iterdir()],
                [f.name for f in self.dataset_2.iterdir()],
            ],
        )

    def base_test_db_integrity_after_duplicates_update(self, old_db_data: list[dict]):
        """Only one value in the whole database should be updated."""
        new_db_data = bfdb.select(self.database_dir)
        for old_row, new_row in zip(
            sorted(old_db_data, key=lambda x: x["marketId"]),
            sorted(new_db_data, key=lambda x: x["marketId"]),
        ):
            if new_row["marketId"] == "1.216395251":
                self.assertEqual(new_row["marketTime"], "2023-07-28T02:35:00.000Z")
                self.assertNotEqual(
                    # pop also removes contentious fields from the dataset
                    new_row.pop("marketTime"),
                    old_row.pop("marketTime"),
                )
            self.assertEqual(new_row, old_row)

    def test_insert_duplicates_skip(self):
        """
        Tests "skip" duplicate resolution policy.

        None of the files are moved and the index is not updated.
        """
        duplicates_dir = self.duplicates_test_setup()
        old_db_data = bfdb.select(self.database_dir)
        bfdb.insert(self.database_dir, duplicates_dir, copy=False, on_duplicates="skip")
        self.assertEqual(len([p for p in duplicates_dir.iterdir()]), 6)  # Nothing moved
        self.assertEqual(old_db_data, bfdb.select(self.database_dir))  # Database intact

    def test_insert_duplicates_replace(self):
        """
        Tests "replace" duplicate resolution policy.

        All files are moved and the index is updated.
        """
        duplicates_dir = self.duplicates_test_setup()
        old_db_data = bfdb.select(self.database_dir)
        bfdb.insert(
            self.database_dir, duplicates_dir, copy=False, on_duplicates="replace"
        )
        self.assertEqual(len([p for p in duplicates_dir.iterdir()]), 0)  # All moved
        self.base_test_db_integrity_after_duplicates_update(old_db_data)  # DB updated

    def test_insert_duplicates_update(self):
        """
        Tests "update" duplicate resolution policy.

        Only '1.201590187.zip' and '1.216395251.json' are moved. Index is updated.
        """
        duplicates_dir = self.duplicates_test_setup()
        old_db_data = bfdb.select(self.database_dir)
        bfdb.insert(
            self.database_dir, duplicates_dir, copy=False, on_duplicates="update"
        )
        remaining_files = [p.name for p in duplicates_dir.iterdir()]
        self.assertEqual(len(remaining_files), 4)  # Two files moved
        self.assertNotIn("1.201590187.zip", remaining_files)
        self.assertNotIn("1.216395251.json", remaining_files)
        self.base_test_db_integrity_after_duplicates_update(old_db_data)  # DB updated

    def test_clean(self):
        """Tests removing rows with missing market data files from the database."""
        database_dir = self.test_data_dir  # Use source data dir as a database

        def select_market_data_file_paths() -> set:
            nonlocal database_dir
            return set(
                m[0]
                for m in bfdb.select(
                    database_dir, columns=["marketDataFilePath"], return_dict=False
                )
            )

        # Non-indexed database must raise an error
        with self.assertRaises(IndexMissingError):
            bfdb.clean(database_dir)

        # Index database and read its state
        bfdb.index(database_dir)
        markets_before = select_market_data_file_paths()
        self.assertEqual(len(markets_before), 9)

        # Randomly select 3 data files and delete them
        markets_to_remove = set(self.data_source_files.pop() for _ in range(3))
        self.assertEqual(len(markets_to_remove), 3)
        for data_file in markets_to_remove:
            data_file.unlink()
        markets_removed = set(str(p) for p in markets_to_remove)

        # Clean the database and re-select data
        bfdb.clean(database_dir)
        markets_after = select_market_data_file_paths()
        self.assertEqual(markets_after, markets_before - markets_removed)
