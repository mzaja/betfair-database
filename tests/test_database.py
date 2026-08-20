import json
import logging
import unittest
from unittest import mock

from betfairdatabase import BetfairDatabase
from betfairdatabase.const import MARKET_ID, METADATA_FILE_NAME, DuplicatePolicy
from betfairdatabase.database import logger
from betfairdatabase.imports import ImportPatterns
from betfairdatabase.processor import Counters
from tests.data import Datasets, TestFixture
from tests.utils import (
    get_debug_messages,
    get_error_messages,
    get_info_messages,
    get_warning_messages,
)


class TestLoggingBase(unittest.TestCase):
    """A base class for tests which involve logging."""

    def setUp(self):
        global logger
        self._original_logger_level = logger.getEffectiveLevel()
        self._original_logger_disabled = logger.disabled

    def tearDown(self):
        global logger
        logger.setLevel(self._original_logger_level)
        logger.disabled = self._original_logger_disabled


class TestBetfairDatabase(TestLoggingBase):
    """
    Additional tests for BetfairDatabase class and its helper classes which
    are not covered by integration tests.
    """

    def test_corrupt_metadata_and_missing_data_files(self):
        """
        Corrupt market catalogues and missing market data files do not break indexing.
        The events and offending file names are logged as errors.
        Checks that the info displayed at the end of indexing is correct.
        """
        corrupt_market_id = "1.221089567"
        missing_data_file_market_id = "1.199967351"  # In uncompressed dataset
        with (
            TestFixture(datasets=Datasets(corrupt=True, uncompressed=True)) as db_dir,
            self.assertLogs(level=logging.INFO) as logs,
        ):
            database = BetfairDatabase(db_dir)
            database.index()

            # Check what was imported and what wasn't
            all_rows = database.select(columns=["marketId"])
            self.assertTrue(len(all_rows) == 5)  # Valid markets got imported
            for row in all_rows:
                # Corrupt and missing market did not get imported
                self.assertNotEqual(row["marketId"], corrupt_market_id)
                self.assertNotEqual(row["marketId"], missing_data_file_market_id)

            # Error message were emitted for corrupt and missing data
            error_messages = get_error_messages(logs)
            self.assertEqual(len(error_messages), 2)
            # Corrupt file's name was logged
            self.assertRegex(
                error_messages[0],
                f"Error parsing.*{corrupt_market_id}\\.json",
            )
            # There was a second attempt to parse the zip file (also corrupt)
            self.assertRegex(
                error_messages[1],
                f"Error parsing.*{corrupt_market_id}\\.zip",
            )
            # Missing market data file event was logged
            warnings = get_warning_messages(logs)
            self.assertEqual(len(warnings), 1)
            self.assertRegex(
                warnings[0],
                f"{missing_data_file_market_id}.*market data files cannot be found",
            )

            # Check summary
            info_messages = get_info_messages(logs)
            expected_messages = [
                "Finished indexing 7 markets.",
                "Added: 5",
                "Corrupt: 1",
                "No data: 1",
                "No metadata: 0",
            ]
            for expected, actual in zip(expected_messages, info_messages, strict=True):
                self.assertEqual(expected, actual)

    def test_missing_metadata(self):
        """
        Tests processing self-recorded and official historical stream files
        which lack a market catalogue. Metadata file is generated from the market
        definition wherever possible.
        """
        # Set logging level to DEBUG to hit an otherwise uncovered line (print names of
        # created metadata files), but inspecting the log messages is not necessary.
        logger.setLevel(level=logging.DEBUG)
        IMPORTABLE_MARKETS_COUNT = 4
        with (
            TestFixture(
                datasets=Datasets(missing_metadata=True), flatten=True
            ) as db_dir,
            self.assertLogs(level=logging.INFO) as logs,
        ):
            database = BetfairDatabase(db_dir)
            database.index()

            # Check that individual market metadata files were not created
            individual_metadata_files = list(db_dir.glob("1.*.json"))
            self.assertEqual(len(individual_metadata_files), 0)

            # Check that one metadata.json file has been created
            bulk_metadata_files = list(db_dir.glob(METADATA_FILE_NAME))
            self.assertEqual(len(bulk_metadata_files), 1)

            # Check that the correct number of markets has been imported
            markets = database.select()
            self.assertEqual(len(markets), IMPORTABLE_MARKETS_COUNT)

            # Verify that the correct markets have (not) been imported
            imported_market_ids = {m["marketId"] for m in markets}
            self.assertNotIn("1.209492553", imported_market_ids)  # No market definition
            self.assertNotIn("1.223716890", imported_market_ids)  # Corrupt, unparsable

            # Check summary
            info_messages = get_info_messages(logs)
            expected_messages = [
                "Finished indexing 6 markets.",
                "Added: 4",
                "Corrupt: 1",
                "No data: 0",
                "No metadata: 1",
            ]
            for expected, actual in zip(expected_messages, info_messages, strict=True):
                self.assertEqual(expected, actual)

    def test_debug_logging_and_summaries(self):
        """
        Tests additional debug logging. Mandatory to achieve 100 % coverage.
        Also checks operational summaries when indexing, importing and cleaning.
        Checks that a message is written when overwriting en existing index.
        """
        DUPLICATE_FILES = ("1.201590187.zip", "1.216395251", "1.216418252")
        logger.setLevel(level=logging.DEBUG)
        with TestFixture(Datasets(compressed=True), flatten=True) as db_dir:
            with self.assertLogs(level=logging.DEBUG) as logs:
                # Indexing the database for the first time
                database = BetfairDatabase(db_dir)
                database.index()

                debug_messages = sorted(get_debug_messages(logs))
                self.assertEqual(len(debug_messages), 4)
                for message, data_file_name in zip(
                    debug_messages,
                    [
                        "1.197931750.zip",
                        "1.197931751.zip",
                        "1.201590187.zip",
                        "1.214870442.zip",
                    ],
                ):
                    self.assertTrue(message.startswith("Adding"))
                    self.assertIn(data_file_name, message)

                # Check summary
                info_messages = get_info_messages(logs)
                expected_messages = [
                    "Finished indexing 4 markets.",
                    "Added: 4",
                    "Corrupt: 0",
                    "No data: 0",
                    "No metadata: 0",
                ]
                for expected, actual in zip(
                    expected_messages, info_messages, strict=True
                ):
                    self.assertEqual(expected, actual)

            with self.assertLogs(level=logging.DEBUG) as logs:
                # Import additional markets while skipping duplicates
                Datasets(duplicates=True).copy_files(db_dir, flatten=False)
                duplicates_dir = db_dir / "duplicates"
                database.insert(
                    duplicates_dir,
                    copy=True,
                    pattern=ImportPatterns.flat,
                    on_duplicates=DuplicatePolicy.SKIP,
                )

                debug_messages = sorted(get_debug_messages(logs))
                self.assertEqual(len(debug_messages), 3)
                for message, data_file_name in zip(
                    # "Skipping" comes after "Adding", so reshuffle the order of file names
                    debug_messages,
                    DUPLICATE_FILES[1:] + DUPLICATE_FILES[:1],
                ):
                    self.assertTrue(
                        message.startswith(
                            "Skipping" if data_file_name.endswith(".zip") else "Adding"
                        )
                    )
                    self.assertIn(data_file_name, message)

                # Check summary
                info_messages = get_info_messages(logs)
                expected_messages = [
                    "Finished importing 3 markets.",
                    "Added: 2",
                    "Updated: 0",
                    "Skipped: 1",
                    "Corrupt: 0",
                    "No data: 0",
                    "No metadata: 0",
                ]
                for expected, actual in zip(
                    expected_messages, info_messages, strict=True
                ):
                    self.assertEqual(expected, actual)

            with self.assertLogs(level=logging.DEBUG) as logs:
                # Overwrite all existing entries no matter what
                database.insert(
                    duplicates_dir,
                    copy=True,
                    pattern=ImportPatterns.flat,
                    on_duplicates=DuplicatePolicy.REPLACE,
                )
                debug_messages = sorted(get_debug_messages(logs))
                self.assertEqual(len(debug_messages), 3)
                for message, data_file_name in zip(debug_messages, DUPLICATE_FILES):
                    self.assertTrue(message.startswith("Updating"))
                    self.assertIn(data_file_name, message)

                # Check summary
                info_messages = get_info_messages(logs)
                expected_messages = [
                    "Finished importing 3 markets.",
                    "Added: 0",
                    "Updated: 3",
                    "Skipped: 0",
                    "Corrupt: 0",
                    "No data: 0",
                    "No metadata: 0",
                ]
                for expected, actual in zip(
                    expected_messages, info_messages, strict=True
                ):
                    self.assertEqual(expected, actual)

            with self.assertLogs(level=logging.DEBUG) as logs:
                # Delete some data files, then clean the database
                removed_files = ["1.201590187.zip", "1.214870442.zip"]
                for file in removed_files:
                    (database.database_dir / file).unlink()
                database.clean()

                debug_messages = sorted(get_debug_messages(logs))
                self.assertEqual(len(debug_messages), 2)
                for message, data_file_name in zip(debug_messages, removed_files):
                    self.assertTrue(message.startswith("Removing"))
                    self.assertIn(data_file_name, message)

                info_messages = get_info_messages(logs)
                self.assertEqual(len(info_messages), 1)
                self.assertIn("Removed 2 ", info_messages[0])

            with self.assertLogs(level=logging.INFO) as logs:
                # Test reindexing message
                database.index(force=True)
                self.assertIn("Overwriting an existing index", logs.records[0].message)

    def test_metadata_file_generation_and_update(self):
        """
        Tests updating an existing metadata.json file when extracting market definitions.
        Also tests debug logging.
        """
        MARKETS_IMPORTED = 9
        logger.setLevel(level=logging.DEBUG)
        with (
            TestFixture(
                Datasets(official=True, bulk_metadata=True), flatten=True
            ) as db_dir,
            self.assertLogs(level=logging.DEBUG) as logs,
        ):
            individual_metadata_files = list(db_dir.glob("1.*.json"))
            data_files = [p for p in db_dir.glob("1.*") if p.suffix != ".json"]
            metadata_file_contents = json.loads(
                (db_dir / METADATA_FILE_NAME).read_bytes()
            )
            self.assertEqual(len(metadata_file_contents), 3)

            # Indexing the database for the first time
            database = BetfairDatabase(db_dir)
            database.index()

            # All markets have been imported
            self.assertEqual(len(database.select()), MARKETS_IMPORTED)

            # Check metadata file has been updated
            metadata_file_contents: list[dict] = json.loads(
                (db_dir / METADATA_FILE_NAME).read_bytes()
            )
            self.assertEqual(
                len(metadata_file_contents),
                MARKETS_IMPORTED - len(individual_metadata_files),
            )
            # Check that all the expected market IDs are present inside metadata.json
            # There are no uncompressed market data files in this test's dataset
            market_ids = set(p.stem for p in data_files) - set(
                p.stem for p in individual_metadata_files
            )
            self.assertEqual(
                market_ids, set(m[MARKET_ID] for m in metadata_file_contents)
            )

        # Check debug message (required for 100 % coverage)
        debug_messages = sorted(get_debug_messages(logs))
        self.assertEqual(
            len([msg for msg in debug_messages if msg.startswith("Adding")]),
            MARKETS_IMPORTED,
        )
        self.assertIsNotNone(
            next(
                (
                    msg
                    for msg in debug_messages
                    if f"{METADATA_FILE_NAME}' already exists and will be updated"
                ),
                None,
            )
        )
        msg = next(
            msg for msg in debug_messages if msg.startswith("Generated metadata file")
        )
        self.assertIn(METADATA_FILE_NAME, msg)

    @mock.patch("betfairdatabase.utils.tqdm")
    def test_progress_bar(self, mock_tqdm: mock.MagicMock):
        """Tests that enabling or disabling the progress bar works."""
        with TestFixture(Datasets(compressed=True), flatten=True) as db_dir:
            # tqdm must return the original iterable
            mock_tqdm.side_effect = lambda iterable, *a, **k: iterable
            for progress_bar_enabled in [True, False]:
                database = BetfairDatabase(db_dir, progress_bar_enabled)
                for method_name, args in [
                    ("index", [True]),  # force
                    ("export", [db_dir]),  # dest
                    ("clean", []),
                    ("insert", [db_dir, True]),  # src, copy
                ]:
                    with self.subTest(
                        method=method_name, progress_bar=progress_bar_enabled
                    ):
                        mock_tqdm.reset_mock()
                        getattr(database, method_name)(*args)
                        if progress_bar_enabled:
                            mock_tqdm.assert_called()
                        else:
                            mock_tqdm.assert_not_called()

    def test_counters_init(self):
        """Tests that all components of Counters instance are set to zero on init."""
        counters = Counters()
        self.assertEqual(len(counters.__slots__), 7)  # Sanity check
        for attr in counters.__slots__:
            self.assertEqual(getattr(counters, attr), 0)
        self.assertEqual(counters.markets_added, 0)  # Test property
        self.assertTrue(counters.validate())  # Validation passes

    def test_counters_do_not_add_up(self):
        """An error is logged if the counters for database insert/import statics do not add up."""
        ACTION = "importing"

        # Check default values, when everything should be set to zero
        counters = Counters()
        self.assertTrue(counters.validate())
        with self.assertNoLogs(level=logging.ERROR):
            counters.log_info(ACTION)

        # Check with valid non-default values
        # imported markets
        counters.rows_inserted = 3  # Added + updated markets
        counters.markets_updated = 1
        # Markets not imported
        counters.markets_without_data = 1
        counters.markets_without_metadata = 1
        counters.corrupt_files = 1
        counters.markets_skipped = 1  # Skipped due to import policy
        # Total
        counters.total_markets = 7  # Markets imported + markets not imported
        self.assertEqual(counters.markets_added, 2)
        self.assertTrue(counters.validate())
        with self.assertNoLogs(level=logging.ERROR):
            counters.log_info(ACTION)

        # Check with invalid non-default values
        counters.total_markets = 99
        self.assertFalse(counters.validate())
        with self.assertLogs(level=logging.ERROR):
            counters.log_info(ACTION)


@unittest.skip("To be removed")
class TestBulkMetadataFile(TestLoggingBase):
    """
    Holds tests for metadata.json files.
    """

    def setUp(self):
        super().setUp()
        self.text_fixture = TestFixture(Datasets(bulk_metadata=True))
        self.db_dir = self.text_fixture.path
        self.dataset_dir = self.db_dir / "bulk_metadata"
        self.metadata_file = self.dataset_dir / METADATA_FILE_NAME

    def tearDown(self):
        super().tearDown()
        self.text_fixture.close()

    def count_market_files(self) -> int:
        """
        Returns the number of market data and metadata files (starting with `1.`)
        in the text fixture directory.
        """
        return len(list(self.db_dir.rglob("1.*")))

    def test_parsing_and_indexing(self):
        """Tests database indexing with a metadata.json file present in the database."""
        market_files_before = self.count_market_files()
        database = BetfairDatabase(self.db_dir)
        database.index()
        market_files_after = self.count_market_files()
        # Verify that the database did not create metadata files
        self.assertEqual(market_files_after, market_files_before)
        data = database.select()
        self.assertEqual(len(data), 4)
        self.assertEqual(
            {x[MARKET_ID] for x in data},
            {"1.209492555", "1.209492556", "1.209492557", "1.209492558"},
        )

    def test_file_decode_error(self):
        """Tests handling errors in parsing metadata.json files."""
        with (self.assertLogs(level=logging.ERROR) as logs,):
            self.metadata_file.write_bytes(self.metadata_file.read_bytes()[:-5])
            database = BetfairDatabase(self.db_dir)
            database.index()
            # Verify metadata was imported from market definitions (fallback)
            self.assertEqual(len(database.select()), 4)

        # Check error messages
        error_messages = get_error_messages(logs)
        self.assertEqual(len(error_messages), 1)
        message = error_messages[0]
        self.assertIn("Error parsing", message)
        self.assertIn(METADATA_FILE_NAME, message)

    def test_file_contains_nonexistent_markets(self):
        """
        Tests that an error message is logged when metadata.json file contains
        an entry for a nonexistent market data file.
        """
        with (self.assertLogs(level=logging.ERROR) as logs,):
            data_file = self.dataset_dir / "1.209492557.zip"
            data_file.unlink()
            database = BetfairDatabase(self.db_dir)
            database.index()
            self.assertEqual(len(database.select()), 3)

        # Check error messages
        error_messages = get_error_messages(logs)
        self.assertEqual(len(error_messages), 1)
        message = error_messages[0]
        self.assertIn("cannot be found", message)
        self.assertIn(data_file.stem, message)  # Market ID included in the message

    def test_file_contains_invalid_entries(self):
        """Invalid entries in metadata.json's list are skipped, and the rest are processed."""
        with (self.assertLogs(level=logging.ERROR) as logs,):
            # Inject invalid entries into metadata file
            contents: list = json.loads(self.metadata_file.read_bytes())
            contents.insert(2, [{"what_am_I_doing": "in_a_metadata.json file?"}])
            contents.insert(1, {"some_chicken": "some_neck"})
            contents.insert(0, {})
            self.metadata_file.write_text(json.dumps(contents))

            market_files_before = self.count_market_files()
            BetfairDatabase(self.db_dir).index()
            market_files_after = self.count_market_files()
            # Verify metadata.json was used, not market definitions
            self.assertEqual(market_files_after, market_files_before)

        # Check error messages
        error_messages = get_error_messages(logs)
        self.assertEqual(len(error_messages), 3)
        message = error_messages[2]
        self.assertIn(f"contains 3 invalid entries", message)
        self.assertIn(METADATA_FILE_NAME, message)

    def test_file_in_not_a_list(self):
        """A warning is logged if metadata.json is not a list of dicts."""
        with (self.assertLogs(level=logging.ERROR) as logs,):
            self.metadata_file.write_text(json.dumps({"I_should": "be_a_list"}))
            database = BetfairDatabase(self.db_dir)
            database.index()
            self.assertEqual(len(database.select()), 4)

        # Check error messages
        error_messages = get_error_messages(logs)
        self.assertEqual(len(error_messages), 1)
        message = error_messages[0]
        self.assertIn(f"should be a list of dicts, not a dict", message)
        self.assertIn(METADATA_FILE_NAME, message)
