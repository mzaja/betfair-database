import logging
from collections import Counter
from dataclasses import dataclass, field
from json import JSONDecodeError
from pathlib import Path
from typing import Iterable
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile

from betfairdatabase.const import (
    MARKET_ID,
    METADATA_FILE_NAME,
)
from betfairdatabase.exceptions import (
    MarketDefinitionMissingError,
)
from betfairdatabase.market import Market
from betfairdatabase.marketdef import MarketDefinitionProcessor
from betfairdatabase.metrics import Counters
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
@dataclass(slots=True)
class DatabaseDirectory:
    """Models a directory in the database containing files of interest."""

    path: Path
    data_files: list[Path] = field(default_factory=list)
    individual_metadata_files: list[Path] = field(default_factory=list)
    bulk_metadata_file: Path | None = None
    generate_metadata_file: bool = field(default=False, init=False)
    corrupt_markets: set = field(default_factory=set, init=False)

    @staticmethod
    def _parse_json_and_log_error(file: Path) -> dict | list | None:
        try:
            data = read_json(file)
            if isinstance(data, (dict, list)):
                return data
            else:
                logger.error("'%s' does not contain valid market metadata.", file)
        except JSONDecodeError:
            logger.error("Error parsing '%s'.", file)
        return None  # Invalid data

    def _parse_bulk_metadata_file(self) -> dict[str, dict]:
        """
        Parses a `metadata.json` file.
        If the file cannot be parsed or does not contain valid data types,
        it will be renamed to `metadata.json.bak`.
        """
        metadata_lookup = {}
        if self.bulk_metadata_file:
            file_data = self._parse_json_and_log_error(self.bulk_metadata_file)
            if not isinstance(file_data, list):
                # Data is not valid and cannot be updated
                if file_data is not None:
                    # File could be parsed, but the expected data type is wrong
                    logger.error(
                        "'%s' should be a list of dicts, not a %s.",
                        self.bulk_metadata_file,
                        file_data.__class__.__name__,
                    )
                # Backup this file because it will get overwritten otherwise
                # and some data might get lost
                backup_file = create_backup(self.bulk_metadata_file)
                logger.warning(
                    "'%s' could not be processed and was renamed to '%s'.",
                    self.bulk_metadata_file,
                    backup_file,
                )
                # Update state, just in case another call is made
                self.bulk_metadata_file = None
            elif file_data:  # It's a non-empty list -> validate and warn of violations
                market_ids = [
                    elem.get(MARKET_ID) for elem in file_data if isinstance(elem, dict)
                ]
                # Checks all elements are dicts
                if len(market_ids) < len(file_data):
                    logger.warning(
                        "'%s' contains list elements which are not dicts.",
                        self.bulk_metadata_file,
                    )
                # Check all elements contain a market ID field
                if not all(market_ids):
                    logger.warning(
                        "'%s' contains dicts without a '%s' field.",
                        self.bulk_metadata_file,
                        MARKET_ID,
                    )
                    market_ids = [x for x in market_ids if x is not None]
                # Check there are no duplicates
                market_id_counts = Counter(market_ids)
                duplicates = [
                    market_id
                    for market_id, count in market_id_counts.items()
                    if count > 1
                ]
                if duplicates:
                    logger.warning("'%s' contains duplicate market IDs: %s", duplicates)

                # Create a metadata lookup dict from valid data
                for item in file_data:
                    try:
                        metadata_lookup[item[MARKET_ID]] = item
                    except KeyError:
                        pass
                    except TypeError:
                        pass

                invalid_entries_count = len(file_data) - len(metadata_lookup)
                if invalid_entries_count:
                    logger.error(
                        "'%s' contains %d invalid entries",
                        self.bulk_metadata_file,
                        invalid_entries_count,
                    )

                # Check if any market data files are missing
                missing_data_files = set(metadata_lookup).difference(
                    self._get_market_id(f) for f in self.data_files
                )
                if missing_data_files:
                    logger.error(
                        "'%s' contains an entry for market IDs %s, "
                        "but matching market data files cannot be found in the directory.",
                        self.bulk_metadata_file,
                        sorted(missing_data_files),
                    )

        return metadata_lookup

    def _parse_individual_metadata_files(
        self, files_to_process: Iterable[Path]
    ) -> dict[str, dict]:
        """
        Parses individual `1.*.json` files containing market catalogues or market definitions.
        """
        files_to_process = list(files_to_process)
        metadata_lookup = {
            data[MARKET_ID]: data
            for file in files_to_process
            if (data := self._parse_json_and_log_error(file))
        }
        self.corrupt_markets |= set(f.stem for f in files_to_process).difference(
            metadata_lookup
        )
        return metadata_lookup

    def _parse_market_definitions(
        self, files_to_process: Iterable[Path], counters: Counters
    ) -> dict[str, dict]:
        """Parses market definitions from market data files."""
        metadata_lookup = {}
        for file in files_to_process:
            try:
                market_def = MarketDefinitionProcessor.parse_market_definition(file)
                metadata_lookup[market_def[MARKET_ID]] = market_def
            except MarketDefinitionMissingError:
                logger.error("Market definition missing in '%s'.", file)
                counters.markets_without_metadata += 1
            except (JSONDecodeError, BadZipFile):
                logger.error("Error parsing '%s'.", file)
                self.corrupt_markets.add(self._get_market_id(file))
        return metadata_lookup

    def _parse_metadata(
        self, counters: Counters, individual_file_market_ids: set
    ) -> dict[str, dict]:
        """
        Parses the directory metadata. Starts with `metadata.json`, moves to individual
        metadata files (`<market_id>.json`) and finishes by extracting market definitions
        from data files if metadata was not extracted sooner.
        """
        metadata_lookup = self._parse_bulk_metadata_file()

        files_to_process = (
            f for f in self.individual_metadata_files if f.stem not in metadata_lookup
        )
        individual_metadata = self._parse_individual_metadata_files(files_to_process)
        metadata_lookup |= individual_metadata
        individual_file_market_ids |= set(individual_metadata)

        files_to_process = (
            f for f in self.data_files if self._get_market_id(f) not in metadata_lookup
        )
        market_defs = self._parse_market_definitions(files_to_process, counters)
        if market_defs:
            self.generate_metadata_file = True
            metadata_lookup |= market_defs

        return metadata_lookup

    def _generate_bulk_metadata_file(self, metadata: list[dict]) -> Path:
        """Generates a metadata.json file from the given data."""
        self.bulk_metadata_file = file_path = self.path / METADATA_FILE_NAME
        write_to_json(file_path, metadata)
        return self.bulk_metadata_file

    def _clean_individual_metadata_files(self, create_backup: bool = True) -> None:
        """
        Removes individual metadata files, either by deleting or archiving them.
        The end result is zero `<market_id>.json` files on disk.
        """
        # Backup individual market metadata files if required
        if create_backup and self.individual_metadata_files:
            archive_path = self.path / "metadata_src.zip"
            with ZipFile(archive_path, "w", ZIP_DEFLATED) as zf:
                for file in self.individual_metadata_files:
                    zf.write(file)
            logger.info(
                "Archived %d market metadata files in '%s'.",
                len(self.individual_metadata_files),
                archive_path,
            )
        # Remove individual metadata files which are not needed anymore
        for file in self.individual_metadata_files:
            file.unlink()
        self.individual_metadata_files = []

    @staticmethod
    def _get_market_id(file: Path) -> str:
        """Extracts the market ID from the file path."""
        if len(file.suffix) > 8:
            return file.name
        else:
            return file.stem

    def process(
        self,
        counters: Counters,
        merge_metadata: bool = False,
        backup_individual_metadata_files: bool = True,
    ) -> list[Market]:
        """Process a directory."""
        # Parse metadata
        individual_file_market_ids = set()  # Required for legacy mode only
        metadata_lookup = self._parse_metadata(counters, individual_file_market_ids)

        # Update counters
        data_file_market_ids = set(self._get_market_id(f) for f in self.data_files)
        metadata_file_market_ids = set(f.stem for f in self.individual_metadata_files)
        counters.total_markets += len(data_file_market_ids | metadata_file_market_ids)
        markets_without_data = metadata_file_market_ids.difference(data_file_market_ids)
        counters.markets_without_data += len(markets_without_data)
        if markets_without_data:
            logger.error(
                "Missing market data file for metadata files: %s",
                [
                    str(self.path / f"{market_id}.json")
                    for market_id in sorted(markets_without_data)
                ],
            )
        counters.corrupt_files += len(self.corrupt_markets)

        # Generate metadata.json file
        if self.generate_metadata_file:
            if not merge_metadata and individual_file_market_ids:
                # De-duplicate data written to metadata.json
                metadata_to_write = [
                    val
                    for key, val in metadata_lookup.items()
                    if key not in individual_file_market_ids
                ]
            else:
                metadata_to_write = list(metadata_lookup.values())
            bulk_metadata_file = self._generate_bulk_metadata_file(metadata_to_write)
            logger.debug("Generated metadata file '%s'.", bulk_metadata_file)

        # Clean up individual metadata files (archive and/or delete)
        if merge_metadata:
            self._clean_individual_metadata_files(backup_individual_metadata_files)

        # Generate importable markets
        if merge_metadata:
            # All metadata comes from a single metadata.json file
            return [
                Market(
                    self.bulk_metadata_file,
                    data_file,
                    metadata_lookup[self._get_market_id(data_file)],
                )
                for data_file in self.data_files
            ]
        else:
            # Legacy/mixed mode -> Metadata partially/fully comes from individual metadata files
            metadata_file_lookup = {
                market_id: file
                for file in self.individual_metadata_files
                if (market_id := file.stem) in individual_file_market_ids
            }
            return [
                Market(
                    metadata_file_lookup.get(
                        market_id,
                        self.bulk_metadata_file,
                    ),
                    data_file,
                    market_metadata,
                )
                for data_file in self.data_files
                if (
                    market_metadata := metadata_lookup.get(
                        (market_id := self._get_market_id(data_file))
                    )
                )
            ]
