import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from json import JSONDecodeError
from pathlib import Path
from typing import Iterable
from zipfile import ZIP_DEFLATED, ZipFile

from betfairdatabase.const import (
    DATA_FILE_SUFFIXES,
    MARKET_ID,
    METADATA_FILE_NAME,
)
from betfairdatabase.exceptions import (
    MarketDefinitionMissingError,
)
from betfairdatabase.market import Market
from betfairdatabase.marketdef import MarketDefinitionProcessor
from betfairdatabase.utils import (
    ProgressBarMixin,
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

    def __bool__(self) -> bool:
        """Returns True if the directory contains any files of interest, else False."""
        return bool(
            self.data_files or self.individual_metadata_files or self.bulk_metadata_file
        )

    def __len__(self) -> int:
        """Returns the number of files of interest in the directory."""
        return (
            len(self.data_files)
            + len(self.individual_metadata_files)
            + bool(self.bulk_metadata_file)  # bool is implicitly int
        )

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

    def _parse_bulk_metadata_file(self) -> list[dict]:
        """
        Parses a `metadata.json` file.
        If the file cannot be parsed or does not contain valid data types,
        it will be renamed to `metadata.json.bak`.
        """
        data = []
        if self.bulk_metadata_file:
            data = self._parse_json_and_log_error(self.bulk_metadata_file)
            if not isinstance(data, list):
                # Data is not valid and cannot be updated
                if data is not None:
                    # File could be parsed, but the expected data type is wrong
                    logger.error(
                        "'%s' should be a list of dicts, not a %s.",
                        self.bulk_metadata_file,
                        data.__class__.__name__,
                    )
                # Backup this file because it will get overwritten otherwise
                # and some data might get lost
                backup_file = create_backup(self.bulk_metadata_file)
                logger.warning(
                    "'%s' could not be processed and was renamed to '%s'.",
                    self.bulk_metadata_file,
                    backup_file,
                )
                self.bulk_metadata_file = (
                    None  # Update state, just in case another call is made
                )
            elif data:
                # It's a non-empty list -> validate and warn of violations
                if not all(isinstance(elem, dict) for elem in data):
                    logger.warning(
                        "'%s' contains list elements which are not dicts.",
                        self.bulk_metadata_file,
                    )
                market_ids = [elem.get(MARKET_ID) for elem in data]
                if not all(market_ids):
                    logger.warning(
                        "'%s' contains dicts without a '%s' field.",
                        self.bulk_metadata_file,
                        MARKET_ID,
                    )
                    market_ids = [x for x in market_ids if x is not None]
                market_id_counts = Counter(market_ids)
                duplicates = [
                    market_id
                    for market_id, count in market_id_counts.values()
                    if count > 1
                ]
                if duplicates:
                    logger.warning("'%s' contains duplicate market IDs: %s", duplicates)
        return data

    @classmethod
    def _parse_individual_metadata_files(
        cls, files_to_process: Iterable[Path]
    ) -> list[dict]:
        """
        Parses individual `1.*.json` files containing market catalogues or market definitions.
        """
        return [
            data
            for file in files_to_process
            if (data := cls._parse_json_and_log_error(file))
        ]

    @staticmethod
    def _parse_market_definitions(files_to_process: Iterable[Path]) -> list[dict]:
        """Parses market definitions from market data files."""
        data = []
        for file in files_to_process:
            try:
                data.append(MarketDefinitionProcessor.parse_market_definition(file))
            except MarketDefinitionMissingError:
                logger.warning("Market definition missing in '%s'.", file)
        return data

    def parse_metadata(self) -> list[dict]:
        """
        Parses the directory metadata. Starts with `metadata.json`, moves to individual
        metadata files (`<market_id>.json`) and finishes by extracting market definitions
        from data files if metadata was not extracted sooner.
        """
        bulk_metadata = self._parse_bulk_metadata_file()
        parsed_market_ids = set(m.get(MARKET_ID) for m in bulk_metadata)

        files_to_process = (
            f for f in self.individual_metadata_files if f.stem not in parsed_market_ids
        )
        individual_metadata = self._parse_individual_metadata_files(files_to_process)
        parsed_market_ids |= set(m.get(MARKET_ID) for m in individual_metadata)

        files_to_process = (
            f for f in self.data_files if parsed_market_ids.isdisjoint({f.stem, f.name})
        )
        market_defs = self._parse_market_definitions(files_to_process)
        return bulk_metadata + individual_metadata + market_defs

    def generate_bulk_metadata_file(
        self, backup_individual_metadata_files: bool = True
    ) -> tuple[list[dict], Path]:
        """
        Generates one `metadata.json` file to rule them all.
        """
        # Backkup individual market metadata files if required
        if backup_individual_metadata_files and self.individual_metadata_files:
            archive_path = self.path / "metadata_src.zip"
            with ZipFile(archive_path, "w", ZIP_DEFLATED) as zf:
                for file in self.individual_metadata_files:
                    zf.write(file)
            logger.info(
                "Archived %d market metadata files in '%s'.",
                len(self.individual_metadata_files),
                archive_path,
            )

        # Parse and merge metadata from all sources
        metadata = self.parse_metadata()

        # Write merged metadata to a metadata.json file
        self.bulk_metadata_file = file_path = self.path / METADATA_FILE_NAME
        write_to_json(file_path, metadata)

        # Remove individual metadata files which are not needed anymore
        for file in self.individual_metadata_files:
            file.unlink()
        self.individual_metadata_files = []

        return metadata, file_path

    def process(
        self, merge_metadata: bool = True, backup_individual_metadata_files: bool = True
    ) -> list[Market]:
        """Process a directory."""
        raise NotImplementedError
        if merge_metadata:
            metadata, metadata_file = self.generate_bulk_metadata_file(
                backup_individual_metadata_files
            )
            # Check that all data files have been covered, but not more than that
            return [
                Market(
                    metadata_file, mdf, metadata.get(mdf.stem, metadata.get(mdf.name))
                )
                for mdf in self.data_files
            ]
        else:
            metadata = self.parse_metadata()
            return [Market()]


class DirectoryParser(ProgressBarMixin):
    """Parses a database directory, with an optional progress bar."""

    def parse_directory_tree(self, root_dir: Path) -> list[DatabaseDirectory]:
        """
        Locates market data and metadata files in the source directory,
        linking them using a common dictionary key.
        """
        data_file_suffixes = list(DATA_FILE_SUFFIXES)
        data_file_suffixes.remove("")
        data_files = defaultdict(list)
        individual_metadata_files = defaultdict(list)
        bulk_metadata_files = {}
        # pathlib.Path.rglob does not support brace expansion to look for multiple
        # patterns at once. Therefore, all files need to be located and filtered
        # manually to avoid traversing the whole directory tree more than once.
        for file in self._progress_bar(root_dir.rglob("*")), "Locating markets":
            file_name = file.name
            if file_name.startswith("1."):
                file_suffix = file.suffix
                # Metadata files always have a .json extension
                if file_suffix == ".json":
                    individual_metadata_files[file.parent].append(file)
                # Data files
                # Uncompressed data files do not have an extension, but
                # the numbers following 1. are treated as one.
                # They usually have 9 "decimal places"
                elif (file_suffix in data_file_suffixes) or (len(file_suffix) > 8):
                    data_files[file.parent].append(file)
            elif file_name == METADATA_FILE_NAME:
                bulk_metadata_files[file.parent] = file
        metadata_without_data = (
            set(individual_metadata_files) + set(bulk_metadata_files) - set(data_files)
        )
        if metadata_without_data:
            logger.warning(
                "Found %d directories with metadata files but no data files: %s",
                len(metadata_without_data),
                sorted(metadata_without_data),
            )
        return [
            DatabaseDirectory(
                dnm,
                dfs,
                individual_metadata_files.get(dnm, []),
                bulk_metadata_files.get(dnm),
            )
            for dnm, dfs in data_files.items()
        ]
