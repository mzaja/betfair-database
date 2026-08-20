import logging
from collections import Counter
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from betfairdatabase.const import (
    MARKET_ID,
)
from betfairdatabase.utils import (
    create_backup,
    read_json,
    write_to_json,
)

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLASSES
# ---------------------------------------------------------------------------
class BulkMetadataFile:
    """
    `metadata.json` file interface.
    """

    def __init__(self, path: Path, exists: bool | None = None):
        self.path = path
        self._exists = exists  # Set to True or False when first parsed

    def _parse(self) -> Any | None:
        try:
            self._exists = True
            return read_json(self.path)
        except JSONDecodeError:
            logger.error("Error parsing '%s'.", self.path)
        except FileNotFoundError:
            self._exists = False
            logger.error("Error parsing '%s'. The file does not exist.", self.path)
        return None  # Invalid data

    def _create_backup(self) -> None:
        """Creates a backup of itself."""
        backup_file = create_backup(self.path)
        logger.warning(
            "'%s' contains invalid data and was renamed to '%s'.",
            self.path,
            backup_file,
        )

    def parse_and_validate(self, backup_on_error=True) -> dict[str, dict]:
        """
        Parses the file and validates its contents.

        Returns a metadata lookup `{market_id: {market_metadata}}`.

        The source file is automatically backed up if content validation fails,
        unless `backup_on_error` is set to False.
        """
        # Parse contents
        contents = self._parse()

        has_errors = False
        metadata_lookup = {}
        if contents is None:
            # Could not parse the JSON file at all
            has_errors = True
        elif not isinstance(contents, list):
            # File could be parsed, but the expected data type is wrong
            logger.error(
                "'%s' should be a list of dicts, not a %s.",
                self.path,
                contents.__class__.__name__,
            )
            has_errors = True
        elif contents:
            # File contains a non-empty list
            market_ids = [
                elem.get(MARKET_ID) for elem in contents if isinstance(elem, dict)
            ]

            # Checks all elements are dicts
            if len(market_ids) < len(contents):
                logger.error(
                    "'%s' contains list elements which are not dicts.",
                    self.path,
                )

            # Check all elements contain a market ID field
            if not all(market_ids):
                logger.error(
                    "'%s' contains dicts without a '%s' field.",
                    self.path,
                    MARKET_ID,
                )
                market_ids = [x for x in market_ids if x is not None]

            # Check there are no duplicates
            market_id_counts = Counter(market_ids)
            duplicates = [
                market_id for market_id, count in market_id_counts.items() if count > 1
            ]
            if duplicates:
                has_errors = True
                logger.warning(
                    "'%s' contains duplicate market IDs: %s", self.path, duplicates
                )

            # Create a metadata lookup dict from valid data
            for item in contents:
                try:
                    metadata_lookup[item[MARKET_ID]] = item
                except (KeyError, TypeError):
                    # Activated if item is not a dict or is missing the market ID key
                    has_errors = True

            # Report on the number of violations
            invalid_entries_count = len(contents) - len(metadata_lookup)
            if invalid_entries_count:
                has_errors = True
                logger.error(
                    "'%s' contains %d invalid entries",
                    self.path,
                    invalid_entries_count,
                )

        if backup_on_error and has_errors and self._exists:
            self._create_backup()

        return metadata_lookup

    def write(self, metadata_lookup: dict[str, dict]) -> None:
        """Writes"""
        if self._exists is None:
            raise ValueError(
                f"'{self.path}' has not been parsed yet. Parse the file before writing to it."
            )

        # Validate
        if not isinstance(metadata_lookup, dict):
            raise ValueError("Metadata lookup must be a dict.")
        elif not metadata_lookup:
            raise ValueError("Metadata lookup is empty.")
        elif not all(k == v[MARKET_ID] for k, v in metadata_lookup.items()):
            raise ValueError("Mismatched market IDs.")

        write_to_json(self.path, list(metadata_lookup.values()))
        self._exists = True
