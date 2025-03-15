import logging
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from betfairdatabase import BetfairDatabase
from betfairdatabase.const import DuplicatePolicy
from betfairdatabase.database import Counters, logger
from betfairdatabase.imports import ImportPatterns


class TestBetfairDatabase(unittest.TestCase):
    """
    Additional tests for BetfairDatabase class and its helper classes which
    are not covered by integration tests.
    """

    TEST_DATA_DIR = Path("./tests/data")

    def setUp(self):
        global logger
        self._original_logger_level = logger.getEffectiveLevel()
        self._original_logger_disabled = logger.disabled

    def tearDown(self):
        global logger
        logger.setLevel(self._original_logger_level)
        logger.disabled = self._original_logger_disabled

    @classmethod
    def create_test_dataset(
        cls,
        temp_dir_path: Path | str,
        flatten: bool = False,
        *,  # Improve readability
        compressed: bool = False,
        uncompressed: bool = False,
        corrupt: bool = False,
        missing_data: bool = False,
        missing_metadata: bool = False,
        duplicates: bool = False,
    ) -> tempfile.TemporaryDirectory:
        """Cretes a test dataset by copying source data into the temporary directory."""
        temp_dir_path = Path(temp_dir_path).resolve()
        datasets = []
        if compressed:
            datasets.append("datasets/zip-lzma")
        if uncompressed:
            datasets.append("datasets/uncompressed")
        if corrupt:
            datasets.append("corrupt")
        if missing_data:
            datasets.append("missing_data")
        if missing_metadata:
            datasets.append("missing_metadata")
        if duplicates:
            datasets.append("duplicates")
        for dataset in datasets:
            dest = temp_dir_path / ("" if flatten else dataset)
            shutil.copytree(cls.TEST_DATA_DIR / dataset, dest, dirs_exist_ok=True)

    def test_corrupt_metadata_and_missing_data_files(self):
        """
        Corrupt market catalogues and missing market data files do not break indexing.
        The events and offending file names are logged as errors.
        Checks that the info displayed at the end of indexing is correct.
        """
        corrupt_market_id = "1.221089567"
        missing_data_file_market_id = "1.199967351"  # In uncompressed dataset
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            self.assertLogs(level=logging.INFO) as logs,
        ):
            self.create_test_dataset(tmpdir, corrupt=True, uncompressed=True)
            database = BetfairDatabase(tmpdir)
            database.index()

            # Check what was imported and what wasn't
            all_rows = database.select(columns=["marketId"])
            self.assertTrue(len(all_rows) == 5)  # Valid markets got imported
            for row in all_rows:
                # Corrupt and missing market did not get imported
                self.assertNotEqual(row["marketId"], corrupt_market_id)
                self.assertNotEqual(row["marketId"], missing_data_file_market_id)

            # Error message were emitted for corrupt and missing data
            error_messages = sorted(
                r.message for r in logs.records if r.levelno == logging.ERROR
            )
            self.assertEqual(len(error_messages), 2)
            # Corrupt file's name was logged
            message = error_messages[0]
            self.assertIn("Error parsing", message)
            self.assertIn(corrupt_market_id + ".json", message)
            # Missing market data file event was logged
            message = message = error_messages[1]
            self.assertIn("Missing market data file", message)
            self.assertIn(missing_data_file_market_id + ".json", message)

            # Check summary
            info_messages = [
                r.message for r in logs.records if r.levelno == logging.INFO
            ]
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
            tempfile.TemporaryDirectory() as tmpdir,
            self.assertLogs(level=logging.INFO) as logs,
        ):
            self.create_test_dataset(tmpdir, missing_metadata=True, flatten=True)
            database = BetfairDatabase(tmpdir)
            database.index()

            # Check that the expected number of metadata files has been created and imported
            metadata_files = list(Path(tmpdir).glob("1.*.json"))
            self.assertEqual(len(metadata_files), IMPORTABLE_MARKETS_COUNT)
            markets = database.select()
            self.assertEqual(len(markets), IMPORTABLE_MARKETS_COUNT)

            # Verify that the correct markets have (not) been imported
            imported_market_ids = {m["marketId"] for m in markets}
            self.assertNotIn("1.209492553", imported_market_ids)  # No market definition
            self.assertNotIn("1.223716890", imported_market_ids)  # Corrupt, unparsable

            # Check summary
            info_messages = [
                r.message for r in logs.records if r.levelno == logging.INFO
            ]
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
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertLogs(level=logging.DEBUG) as logs:
                # Indexing the database for the first time
                self.create_test_dataset(tmpdir, flatten=True, compressed=True)
                database = BetfairDatabase(tmpdir)
                database.index()

                debug_messages = sorted(
                    r.message for r in logs.records if r.levelno == logging.DEBUG
                )
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
                info_messages = [
                    r.message for r in logs.records if r.levelno == logging.INFO
                ]
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
                self.create_test_dataset(tmpdir, flatten=False, duplicates=True)
                duplicates_dir = Path(tmpdir).resolve() / "duplicates"
                database.insert(
                    duplicates_dir,
                    copy=True,
                    pattern=ImportPatterns.flat,
                    on_duplicates=DuplicatePolicy.SKIP,
                )

                debug_messages = sorted(
                    r.message for r in logs.records if r.levelno == logging.DEBUG
                )
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
                info_messages = [
                    r.message for r in logs.records if r.levelno == logging.INFO
                ]
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
                debug_messages = sorted(
                    r.message for r in logs.records if r.levelno == logging.DEBUG
                )
                self.assertEqual(len(debug_messages), 3)
                for message, data_file_name in zip(debug_messages, DUPLICATE_FILES):
                    self.assertTrue(message.startswith("Updating"))
                    self.assertIn(data_file_name, message)

                # Check summary
                info_messages = [
                    r.message for r in logs.records if r.levelno == logging.INFO
                ]
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

                debug_messages = sorted(
                    r.message for r in logs.records if r.levelno == logging.DEBUG
                )
                self.assertEqual(len(debug_messages), 2)
                for message, data_file_name in zip(debug_messages, removed_files):
                    self.assertTrue(message.startswith("Removing"))
                    self.assertIn(data_file_name, message)

                info_messages = [
                    r.message for r in logs.records if r.levelno == logging.INFO
                ]
                self.assertEqual(len(info_messages), 1)
                self.assertIn("Removed 2 ", info_messages[0])

            with self.assertLogs(level=logging.INFO) as logs:
                # Test reindexing message
                database.index(force=True)
                self.assertIn("Overwriting an existing index", logs.records[0].message)

    @mock.patch("betfairdatabase.database.tqdm")
    def test_progress_bar(self, mock_tqdm: mock.MagicMock):
        """Tests that enabling or disabling the progress bar works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self.create_test_dataset(tmpdir, flatten=True, compressed=True)
            # tqdm must return the original iterable
            mock_tqdm.side_effect = lambda iterable, *a, **k: iterable
            for progress_bar_enabled in [True, False]:
                database = BetfairDatabase(tmpdir, progress_bar_enabled)
                for method_name, args in [
                    ("index", [True]),  # force
                    ("export", [tmpdir]),  # dest
                    ("clean", []),
                    ("insert", [tmpdir, True]),  # src, copy
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
