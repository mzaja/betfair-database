import json
import logging
import unittest
from pathlib import Path
from zipfile import ZipFile

from betfairdatabase.const import MARKET_ID, METADATA_FILE_NAME
from betfairdatabase.metrics import Counters
from betfairdatabase.tree.directory import DatabaseDirectory
from betfairdatabase.tree.metadatafile import BulkMetadataFile
from betfairdatabase.utils import read_json
from tests.data.fixtures import Datasets, TestFixture
from tests.utils import get_error_messages


class DatabaseDirectoryTests(unittest.TestCase):
    """
    Tests DatabaseDirectory class.
    """

    # @classmethod
    # def setUpClass(cls):
    #     cls._fixture = TestFixture()
    #     cls.temp_dir = cls._fixture.path

    # @classmethod
    # def tearDownClass(cls):
    #     cls._fixture.close()

    # def assert_backup_file_exists(self, src: Path):
    #     """Asserts that a backup file of `src` has been created."""
    #     backup_file_path = src.parent / (src.name + ".bak")
    #     self.assertTrue(backup_file_path.exists())

    # def assert_backup_file_does_not_exist(self, src: Path):
    #     """Asserts that a backup file of `src` has been created."""
    #     backup_file_path = src.parent / (src.name + ".bak")
    #     self.assertFalse(backup_file_path.exists())

    @staticmethod
    def categorise_files(
        dir_path: Path,
    ) -> tuple[list[Path], list[Path], BulkMetadataFile | None]:
        """Sorts files into three categories from the provided directory."""
        data_files = []
        metadata_files = []
        for file in dir_path.glob("1.*"):
            (metadata_files if file.suffix == ".json" else data_files).append(file)
        bulk_metadata_file_path = dir_path / METADATA_FILE_NAME
        bulk_metadata_file = (
            BulkMetadataFile(bulk_metadata_file_path)
            if bulk_metadata_file_path.exists()
            else None
        )
        return data_files, metadata_files, bulk_metadata_file

    def test_merge_metadata_from_individual_files_only(self):
        """Tests merging metadata from individual market metadata files only."""
        with TestFixture(Datasets(uncompressed=True), flatten=True) as temp_dir:
            dd = DatabaseDirectory(temp_dir, *self.categorise_files(temp_dir))
            src_metadata_files = dd.individual_metadata_files
            src_metadata_lookup = {
                data[MARKET_ID]: data for data in map(read_json, src_metadata_files)
            }

            markets = dd.process(Counters(), merge_metadata=True)

            # Verify that all importable markets have been generated
            self.assertEqual(
                set(m.metadata[MARKET_ID] for m in markets),
                set(f.stem for f in src_metadata_files if f.name.startswith("1.21")),
            )

            # Verify that no individual market metadata files are left in the directory
            self.assertEqual(list(temp_dir.glob("1.*.json")), [])

            # Verify that no information was lost when merging file contents
            bulk_metadata_lookup = BulkMetadataFile(
                dd.bulk_metadata_file.path
            ).parse_and_validate()
            self.assertEqual(bulk_metadata_lookup, src_metadata_lookup)

            # Verify that a backup file was generated with all the removed files
            with ZipFile(temp_dir / "metadata_src.zip") as zf:
                archived_metadata_lookup = {
                    data[MARKET_ID]: data
                    for data in map(json.loads, map(zf.read, zf.namelist()))
                }
            self.assertEqual(archived_metadata_lookup, src_metadata_lookup)


class DatabaseDirectoryInvalidFilesTests(unittest.TestCase):
    """
    Tests DatabaseDirectory class' handling of invalid files.
    """

    def setUp(self):
        self._fixture = TestFixture()
        self.temp_dir = self._fixture.path

    def tearDown(self):
        self._fixture.close()

    def test_individual_metadata_file_is_corrupt(self):
        """Tests attempting to parse a metadata.json file which is not valid JSON."""
        meta_file = self.temp_dir / "1.11112222.json"
        meta_file.write_text("{")
        dd = DatabaseDirectory(self.temp_dir, individual_metadata_files=[meta_file])

        with self.assertLogs(level=logging.ERROR) as logs:
            self.assertEqual(dd._parse_metadata(set()), {})

        error_messages = get_error_messages(logs)
        self.assertEqual(len(error_messages), 1)
        self.assertRegex(error_messages[0], f"Error parsing.*{meta_file.name}")

    def test_individual_metadata_file_is_not_a_dict(self):
        """Tests attempting to parse a metadata.json file which is not valid JSON."""
        meta_file = self.temp_dir / "1.22223333.json"
        meta_file.write_text("[]")
        dd = DatabaseDirectory(self.temp_dir, individual_metadata_files=[meta_file])

        with self.assertLogs(level=logging.ERROR) as logs:
            self.assertEqual(dd._parse_metadata(set()), {})

        error_messages = get_error_messages(logs)
        self.assertEqual(len(error_messages), 1)
        self.assertRegex(
            error_messages[0],
            f"{meta_file.name}.*does not contain valid market metadata",
        )

    def test_individual_metadata_file_does_not_contain_a_market_id_field(self):
        """Tests attempting to parse a metadata.json file which is not valid JSON."""
        meta_file = self.temp_dir / "1.33334444.json"
        meta_file.write_text("{}")
        dd = DatabaseDirectory(self.temp_dir, individual_metadata_files=[meta_file])

        with self.assertLogs(level=logging.ERROR) as logs:
            self.assertEqual(dd._parse_metadata(set()), {})

        error_messages = get_error_messages(logs)
        self.assertEqual(len(error_messages), 1)
        self.assertRegex(
            error_messages[0],
            f"{meta_file.name}.*does not contain valid market metadata",
        )
