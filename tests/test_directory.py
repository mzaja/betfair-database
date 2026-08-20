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
        """
        Tests merging metadata from individual market metadata files only.
        This simulates the operation performed on a virgin legacy database directory.
        """
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

    def test_merge_metadata_from_mixed_sources(self):
        """
        Tests merging metadata from mixed sources:
            1. An existing `metadata.json` file.
            2. Individual market catalogue files.
            3. Market definitions from market data files.
        """
        with TestFixture(
            Datasets(uncompressed=True, official=True, bulk_metadata=True), flatten=True
        ) as temp_dir:
            dd = DatabaseDirectory(temp_dir, *self.categorise_files(temp_dir))
            src_metadata_lookup = {
                data[MARKET_ID]: data
                for data in map(read_json, dd.individual_metadata_files)
            }
            src_bulk_metadata_lookup = dd.bulk_metadata_file.parse_and_validate()
            data_file_market_ids = set(
                DatabaseDirectory._get_market_id(f) for f in dd.data_files
            )

            # Check that market definition file was included as well
            self.assertIn("1.214555872", src_metadata_lookup)

            markets = dd.process(Counters(), merge_metadata=True)

            # Verify that all importable markets have been generated (all data files)
            self.assertEqual(
                set(m.metadata[MARKET_ID] for m in markets),
                data_file_market_ids,
            )

            # Verify that no individual market metadata files are left in the directory
            self.assertEqual(list(temp_dir.glob("1.*.json")), [])

            # Verify that no information was lost when merging file contents
            bulk_metadata_lookup = BulkMetadataFile(
                dd.bulk_metadata_file.path
            ).parse_and_validate()

            # Verify that all data files have a metadata entry
            self.assertTrue(set(bulk_metadata_lookup).issuperset(data_file_market_ids))

            # Verify that source metadata is preserved in the updated metadata.json file
            for src_lookup in [src_bulk_metadata_lookup, src_metadata_lookup]:
                for market_id, src_metadata in src_lookup.items():
                    self.assertEqual(bulk_metadata_lookup[market_id], src_metadata)

            # Verify that a backup file was generated with all the removed files
            with ZipFile(temp_dir / "metadata_src.zip") as zf:
                archived_metadata_lookup = {
                    data[MARKET_ID]: data
                    for data in map(json.loads, map(zf.read, zf.namelist()))
                }
            self.assertEqual(archived_metadata_lookup, src_metadata_lookup)

    def test_merge_metadata_no_src_backup(self):
        """Verifies the "no source backup" option when merging metadata."""
        with TestFixture(Datasets(uncompressed=True), flatten=True) as temp_dir:
            dd = DatabaseDirectory(temp_dir, *self.categorise_files(temp_dir))
            src_metadata_files = dd.individual_metadata_files
            src_metadata_lookup = {
                data[MARKET_ID]: data for data in map(read_json, src_metadata_files)
            }

            markets = dd.process(
                Counters(), merge_metadata=True, backup_metadata_src=False
            )

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

            # Verify that a backup file was not generated
            self.assertFalse((temp_dir / "metadata_src.zip").exists())

    def test_do_not_merge_metadata(self):
        """
        Tests the legacy behaviour where individual market metadata files are left alone.
        However, metadata.json is still generated if
        """
        with TestFixture(
            Datasets(uncompressed=True, official=True, bulk_metadata=True), flatten=True
        ) as temp_dir:
            dd = DatabaseDirectory(temp_dir, *self.categorise_files(temp_dir))
            src_metadata_files = dd.individual_metadata_files
            src_metadata_lookup = {
                data[MARKET_ID]: data for data in map(read_json, src_metadata_files)
            }
            src_bulk_metadata_lookup = dd.bulk_metadata_file.parse_and_validate()
            data_file_market_ids = set(
                DatabaseDirectory._get_market_id(f) for f in dd.data_files
            )
            individual_metadata_file_market_ids = set(
                f.stem for f in src_metadata_files
            )

            markets = dd.process(Counters(), backup_metadata_src=True)

            # Verify that all importable markets have been generated (all data files)
            self.assertEqual(
                set(m.metadata[MARKET_ID] for m in markets),
                data_file_market_ids,
            )

            # Verify that no individual market metadata files are left in the directory
            self.assertEqual(set(temp_dir.glob("1.*.json")), set(src_metadata_files))

            # Verify that no information was lost when merging file contents
            bulk_metadata_lookup = BulkMetadataFile(
                dd.bulk_metadata_file.path
            ).parse_and_validate()

            # Verify that all data files without a matching metadata file
            # have an entry inside a metadata.json file
            self.assertTrue(
                set(bulk_metadata_lookup).issuperset(
                    data_file_market_ids.difference(individual_metadata_file_market_ids)
                )
            )

            # Verify that source metadata is preserved in the updated metadata.json file
            for market_id, src_metadata in src_bulk_metadata_lookup.items():
                self.assertEqual(bulk_metadata_lookup[market_id], src_metadata)

            # Verify that a backup file was not generated, despite backup_metadata_src=True
            self.assertFalse((temp_dir / "metadata_src.zip").exists())

    def test_bulk_metadata_file_is_not_unnecessarily_generated(self):
        """
        Tests that metadata.json file is not generated in legacy mode
        when all data files have their metadata file counterparts.
        """
        with TestFixture(Datasets(uncompressed=True), flatten=True) as temp_dir:
            dd = DatabaseDirectory(temp_dir, *self.categorise_files(temp_dir))
            dd.process(Counters())
            self.assertIsNone(dd.bulk_metadata_file)
            self.assertFalse((temp_dir / METADATA_FILE_NAME).exists())


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
