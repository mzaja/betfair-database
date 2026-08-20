import logging
import unittest
from pathlib import Path

from betfairdatabase.const import MARKET_ID
from betfairdatabase.tree.metadatafile import BulkMetadataFile
from betfairdatabase.utils import read_json, write_to_json
from tests.data.fixtures import TestFixture


class BulkMetadataFileTests(unittest.TestCase):
    """
    Tests BulkMetadataFile class.
    """

    @classmethod
    def setUpClass(cls):
        cls._fixture = TestFixture()
        cls.temp_dir = cls._fixture.path

    @classmethod
    def tearDownClass(cls):
        cls._fixture.close()

    def assert_backup_file_exists(self, src: Path):
        """Asserts that a backup file of `src` has been created."""
        backup_file_path = src.parent / (src.name + ".bak")
        self.assertTrue(backup_file_path.exists())

    def assert_backup_file_does_not_exist(self, src: Path):
        """Asserts that a backup file of `src` has been created."""
        backup_file_path = src.parent / (src.name + ".bak")
        self.assertFalse(backup_file_path.exists())

    def test_file_is_corrupt(self):
        """Tests attempting to parse a file which is not valid JSON."""
        file = BulkMetadataFile(self.temp_dir / "corrupt.json")
        file.path.write_text("{")

        with self.assertLogs(level=logging.ERROR) as logs:
            self.assertEqual(file.parse_and_validate(), {})

        self.assert_backup_file_exists(file.path)

        error_messages = [r.message for r in logs.records if r.levelno == logging.ERROR]
        self.assertEqual(len(error_messages), 1)
        self.assertRegex(error_messages[0], f"Error parsing.*{file.path.name}")

    def test_file_does_not_exist(self):
        """Tests the case when the file does not exist."""
        file_name = "i_do_not_exist.json"
        file = BulkMetadataFile(self.temp_dir / file_name)

        with self.assertLogs(level=logging.ERROR) as logs:
            self.assertEqual(file.parse_and_validate(), {})
            file.write({"1.123": {MARKET_ID: "1.123"}})  # Passes if no error is raised

        error_messages = [r.message for r in logs.records if r.levelno == logging.ERROR]
        self.assertEqual(len(error_messages), 1)
        self.assertRegex(error_messages[0], f"{file_name}.* does not exist")

    def test_file_is_not_a_list(self):
        """Tests parsing a file whose contents are not a list."""
        file = BulkMetadataFile(self.temp_dir / "not_a_list.json")
        write_to_json(file.path, {"not": "a list"})

        with self.assertLogs(level=logging.ERROR) as logs:
            self.assertEqual(file.parse_and_validate(), {})

        self.assert_backup_file_exists(file.path)

        error_messages = [r.message for r in logs.records if r.levelno == logging.ERROR]
        self.assertEqual(len(error_messages), 1)
        self.assertRegex(
            error_messages[0], f"{file.path.name}.*should be a list of dicts"
        )

    def test_file_contains_non_dict_list_elements(self):
        """Tests parsing a file which contains non-dict elements inside the list."""
        file = BulkMetadataFile(self.temp_dir / "non_dict_elements.json")
        write_to_json(
            file.path,
            [
                {MARKET_ID: "1.123"},
                ["not a dict"],
                {MARKET_ID: "1.456"},
                None,
                {MARKET_ID: "1.789"},
                "not a dict either",
            ],
        )

        with self.assertLogs(level=logging.ERROR) as logs:
            metadata_lookup = file.parse_and_validate()

        self.assertEqual(set(metadata_lookup.keys()), {"1.123", "1.456", "1.789"})
        self.assert_backup_file_exists(file.path)

        error_messages = [r.message for r in logs.records if r.levelno == logging.ERROR]
        self.assertEqual(len(error_messages), 2)
        self.assertRegex(
            error_messages[0],
            f"{file.path.name}.*contains list elements which are not dicts",
        )
        self.assertRegex(
            error_messages[1], f"{file.path.name}.*contains 3 invalid entries"
        )

    def test_file_contains_dict_list_elements_without_market_id_field(self):
        """Tests parsing a file which contains dicts without a market ID field."""
        file = BulkMetadataFile(self.temp_dir / "missing_market_ids.json")
        write_to_json(
            file.path,
            [
                {MARKET_ID: "1.123"},
                {MARKET_ID: "1.456"},
                {"not_market_id": "1.789"},
                {MARKET_ID: "1.789"},
                {"not_market_id": "1.999"},
            ],
        )

        with self.assertLogs(level=logging.ERROR) as logs:
            metadata_lookup = file.parse_and_validate()

        self.assertEqual(set(metadata_lookup.keys()), {"1.123", "1.456", "1.789"})
        self.assert_backup_file_exists(file.path)

        error_messages = [r.message for r in logs.records if r.levelno == logging.ERROR]
        self.assertEqual(len(error_messages), 2)
        self.assertRegex(
            error_messages[0],
            f"{file.path.name}.*contains dicts without a '{MARKET_ID}' field",
        )
        self.assertRegex(
            error_messages[1], f"{file.path.name}.*contains 2 invalid entries"
        )

    def test_file_contains_duplicate_market_ids(self):
        """A warning is logged if the metadata file contains entries with non-unique makrket IDs."""
        file = BulkMetadataFile(self.temp_dir / "duplicates.json")
        write_to_json(
            file.path,
            [
                {MARKET_ID: "1.123"},
                {MARKET_ID: "1.456"},
                {MARKET_ID: "1.123"},
                {MARKET_ID: "1.456"},
                {MARKET_ID: "1.789"},
            ],
        )

        with self.assertLogs(level=logging.WARNING) as logs:
            metadata_lookup = file.parse_and_validate()

        self.assertEqual(set(metadata_lookup.keys()), {"1.123", "1.456", "1.789"})
        self.assert_backup_file_exists(file.path)

        warning_messages = [
            r.message for r in logs.records if r.levelno == logging.WARNING
        ]
        self.assertEqual(len(warning_messages), 2)
        msg = warning_messages[0]
        self.assertRegex(msg, f"contains duplicate market IDs")
        for duplicate_market_id in ("1.123", "1.456"):
            self.assertIn(duplicate_market_id, msg)

    def test_writing_before_parsing_fails(self):
        """Verifies that attempting to write to a file before parsing it fails."""
        file = BulkMetadataFile(self.temp_dir / "name_does_not_matter.json")
        with self.assertRaisesRegex(ValueError, "has not been parsed"):
            file.write({"1.123": {MARKET_ID: "1.123"}})

    def test_writing_a_non_dict_fails(self):
        """Verifies that writing metadata from a non dict fails."""
        file = BulkMetadataFile(self.temp_dir / "writing_wrong_data_type.json", False)
        metadata_lookup = []
        with self.assertRaisesRegex(ValueError, "Metadata lookup must be a dict"):
            file.write(metadata_lookup)

    def test_writing_an_empty_dict_fails(self):
        """Verifies that writing metadata from an empty dict fails."""
        file = BulkMetadataFile(self.temp_dir / "writing_empty_dict.json", False)
        metadata_lookup = {}
        with self.assertRaisesRegex(ValueError, "Metadata lookup is empty"):
            file.write(metadata_lookup)

    def test_writing_values_without_market_id_fails(self):
        """Verifies that writing metadata from a lookup with values missing the market ID field fails."""
        file = BulkMetadataFile(self.temp_dir / "missing_market_id_field.json", False)
        metadata_lookup = {
            "1.123": {MARKET_ID: "1.123"},
            "1.456": {"not_market_id": "1.456"},
        }
        with self.assertRaises(KeyError):
            file.write(metadata_lookup)

    def test_writing_mismatched_market_ids_fails(self):
        """Verifies that writing metadata from a lookup with mismatched market IDs fails."""
        file = BulkMetadataFile(self.temp_dir / "mismatched_market_ids.json", False)
        metadata_lookup = {"1.123": {MARKET_ID: "1.123"}, "1.456": {MARKET_ID: "1.789"}}
        with self.assertRaisesRegex(ValueError, "Mismatched market IDs"):
            file.write(metadata_lookup)

    def test_parse_and_write(self):
        """Tests parsing a valid file and writing valid contents."""
        file = BulkMetadataFile(self.temp_dir / "valid.json")
        write_to_json(
            file.path,
            [
                {MARKET_ID: "1.123"},
                {MARKET_ID: "1.456"},
                {MARKET_ID: "1.789"},
            ],
        )
        # Read
        metadata_lookup = file.parse_and_validate()
        self.assertEqual(set(metadata_lookup.keys()), {"1.123", "1.456", "1.789"})
        self.assert_backup_file_does_not_exist(file.path)

        # Write
        new_metadata_lookup = {
            "1.321": {MARKET_ID: "1.321"},
            "1.654": {MARKET_ID: "1.654"},
            "1.987": {MARKET_ID: "1.987"},
        }
        file.write(new_metadata_lookup)
        self.assertEqual(file.parse_and_validate(), new_metadata_lookup)

    def test_parse_and_write_round_trip(self):
        """
        Tests that parsing a valid file and writing back the parsed data
        results in the same file contents.
        """
        file = BulkMetadataFile(self.temp_dir / "valid_round_trip.json")
        contents = [
            {MARKET_ID: "1.123"},
            {MARKET_ID: "1.789"},
            {MARKET_ID: "1.456"},
        ]
        write_to_json(file.path, contents)
        file.write(file.parse_and_validate())
        self.assertEqual(read_json(file.path), contents)
