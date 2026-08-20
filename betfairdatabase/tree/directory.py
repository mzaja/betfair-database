import logging
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
from betfairdatabase.tree.metadatafile import BulkMetadataFile
from betfairdatabase.utils import (
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
    bulk_metadata_file: BulkMetadataFile | None = None
    _generate_metadata_file: bool = field(default=False, init=False)
    # For metrics only
    _corrupt_markets: set[str] = field(default_factory=set, init=False)
    _markets_without_metadata_count: int = field(default=0, init=False)

    @staticmethod
    def _get_market_id(file: Path) -> str:
        """Extracts the market ID from the file path."""
        return file.name if (len(file.suffix) > 8) else file.stem

    def _parse_bulk_metadata_file(self) -> dict[str, dict]:
        """
        Parses a `metadata.json` file.
        If the file cannot be parsed or does not contain valid data types,
        it will be renamed to `metadata.json.bak`.
        """
        return (
            self.bulk_metadata_file.parse_and_validate()
            if self.bulk_metadata_file
            else {}
        )

    def _parse_individual_metadata_files(
        self, files_to_process: Iterable[Path]
    ) -> dict[str, dict]:
        """
        Parses individual `1.*.json` files containing market catalogues or market definitions.
        """
        metadata_lookup = {}
        for file in files_to_process:
            try:
                data = read_json(file)
            except JSONDecodeError:
                logger.error("Error parsing '%s'.", file)
                self._corrupt_markets.add(file.stem)
                continue

            if not isinstance(data, dict) or (market_id := data.get(MARKET_ID)) is None:
                logger.error("'%s' does not contain valid market metadata.", file)
                self._corrupt_markets.add(file.stem)
            else:
                metadata_lookup[market_id] = data
        return metadata_lookup

    def _parse_market_definitions(
        self, files_to_process: Iterable[Path]
    ) -> dict[str, dict]:
        """Parses market definitions from market data files."""
        metadata_lookup = {}
        self._markets_without_metadata_count = 0  # Reset counter
        for file in files_to_process:
            try:
                market_def = MarketDefinitionProcessor.parse_market_definition(file)
                metadata_lookup[market_def[MARKET_ID]] = market_def
            except MarketDefinitionMissingError:
                logger.error("Market definition missing in '%s'.", file)
                self._markets_without_metadata_count += 1
            except (JSONDecodeError, BadZipFile):
                logger.error("Error parsing '%s'.", file)
                self._corrupt_markets.add(self._get_market_id(file))
        return metadata_lookup

    def _check_for_missing_data_files(self, metadata_lookup: dict[str, dict]) -> None:
        """Checks whether any data files are missing compared to the parsed metadata."""
        missing_data_files = set(metadata_lookup).difference(
            self._get_market_id(f) for f in self.data_files
        )
        if missing_data_files:
            logger.warning(
                "'%s' contains metadata for market IDs %s, "
                "but the matching market data files cannot be found in the directory.",
                self.path,
                sorted(missing_data_files),
            )

    def _parse_metadata(self, individual_file_market_ids: set) -> dict[str, dict]:
        """
        Parses the directory metadata. Starts with `metadata.json`, moves to individual
        metadata files (`<market_id>.json`) and finishes by extracting market definitions
        from data files if metadata was not extracted sooner.

        `individual_file_market_ids` is an output parameter, used for differentiating between
        market metadata obtained from metadata.json and from individual metadata files. It is
        only used when running in legacy mode, when `metadata.json` is not generated from the
        existing market metadata files.
        """
        # Parse metadata.json
        metadata_lookup = self._parse_bulk_metadata_file()

        # Parse individual metadata files
        files_to_process = (
            f for f in self.individual_metadata_files if f.stem not in metadata_lookup
        )
        individual_metadata = self._parse_individual_metadata_files(files_to_process)
        metadata_lookup |= individual_metadata
        individual_file_market_ids |= set(individual_metadata)

        # Check whether any expected data files are missing from the directory
        # (From here onwards, only data files are processed, which obviously exist)
        self._check_for_missing_data_files(metadata_lookup)

        # Parse market definitions from data files (streams)
        files_to_process = (
            f for f in self.data_files if self._get_market_id(f) not in metadata_lookup
        )
        market_defs = self._parse_market_definitions(files_to_process)
        if market_defs:
            self._generate_metadata_file = True
            metadata_lookup |= market_defs

        return metadata_lookup

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

    def process(
        self,
        counters: Counters,
        merge_metadata: bool = False,
        backup_individual_metadata_files: bool = True,
    ) -> list[Market]:
        """Process a directory."""
        # Parse metadata
        individual_file_market_ids = set()  # Required for legacy mode only
        metadata_lookup = self._parse_metadata(individual_file_market_ids)

        # Update counters
        data_file_market_ids = set(self._get_market_id(f) for f in self.data_files)
        metadata_file_market_ids = set(f.stem for f in self.individual_metadata_files)
        counters.total_markets += len(data_file_market_ids | metadata_file_market_ids)
        markets_without_data = metadata_file_market_ids.difference(data_file_market_ids)
        counters.markets_without_data += len(markets_without_data)
        counters.markets_without_metadata += self._markets_without_metadata_count
        counters.corrupt_files += len(self._corrupt_markets)

        # Generate metadata.json file
        if self._generate_metadata_file or merge_metadata:
            if not merge_metadata and individual_file_market_ids:
                # De-duplicate data written to metadata.json
                metadata_to_write = {
                    key: val
                    for key, val in metadata_lookup.items()
                    if key not in individual_file_market_ids
                }
            else:
                metadata_to_write = metadata_lookup

            if self.bulk_metadata_file is None:
                self.bulk_metadata_file = BulkMetadataFile(
                    self.path / METADATA_FILE_NAME, False
                )

            self.bulk_metadata_file.write(metadata_to_write)
            logger.debug("Generated metadata file '%s'.", self.bulk_metadata_file.path)

        # Clean up individual metadata files (archive and/or delete)
        if merge_metadata:
            self._clean_individual_metadata_files(backup_individual_metadata_files)

        # Generate importable markets
        if merge_metadata:
            # All metadata comes from a single metadata.json file
            return [
                Market(
                    self.bulk_metadata_file.path,
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
                        (
                            self.bulk_metadata_file.path
                            if self.bulk_metadata_file
                            else None
                        ),
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
