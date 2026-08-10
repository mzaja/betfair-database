import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Callable, Literal, NamedTuple, TypeVar

from betfairdatabase.const import (
    DATA_FILE_SUFFIXES,
    MARKET_ID,
    MARKET_METADATA_FILE_PATH,
    METADATA_FILE_NAME,
    SQL_TABLE_NAME,
    DuplicatePolicy,
    SQLAction,
)
from betfairdatabase.exceptions import (
    MarketDefinitionMissingError,
)
from betfairdatabase.market import Market
from betfairdatabase.marketdef import MarketDefinitionProcessor
from betfairdatabase.racing import RacingDataProcessor
from betfairdatabase.utils import (
    ProgressBarMixin,
    is_debug_logging_enabled,
    read_json,
    write_to_json,
)

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HELPER CLASSES
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class Counters:
    """
    Stores counters for various operations of the market file processor.
    Used to display import statistics.
    """

    total_markets: int = 0
    markets_without_data: int = 0
    markets_without_metadata: int = 0
    corrupt_files: int = 0
    rows_inserted: int = 0
    markets_updated: int = 0
    markets_skipped: int = 0

    @property
    def markets_added(self) -> int:
        """Returns the number of added markets."""
        return self.rows_inserted - self.markets_updated

    def log_info(self, action: Literal["indexing", "importing"]) -> None:
        """Logs the counters as INFO messages."""
        logger.info("Finished %s %d markets.", action, self.total_markets)
        logger.info("Added: %d", self.markets_added)
        if action == "importing":
            logger.info("Updated: %d", self.markets_updated)
            logger.info("Skipped: %d", self.markets_skipped)
        logger.info("Corrupt: %d", self.corrupt_files)
        logger.info("No data: %d", self.markets_without_data)
        logger.info("No metadata: %d", self.markets_without_metadata)
        if not self.validate():
            logger.error("Counters do not add up.")

    def validate(self) -> bool:
        """
        Performs a sanity check on the contents to ensure that the sum of components
        matches the total. Returns True if the checks passes, else False.
        """
        return self.total_markets == (
            self.rows_inserted
            + self.markets_skipped
            + self.corrupt_files
            + self.markets_without_data
            + self.markets_without_metadata
        )


class MarketFiles(NamedTuple):
    """
    Holds the market files categorised by class:
        - data: Market stream files (`1.<market_id>` or `1.<market_id>.<compression>`)
        - individual_metadata: Individual market metadata files (`1.<market_id>.json`)
        - bulk_metadata: Bulk metadata files (`metadata.json`)
    """

    data: dict[Path, Path]
    individual_metadata: dict[Path, Path]
    bulk_metadata: list[Path]


# ---------------------------------------------------------------------------
# MAIN CLASS
# ---------------------------------------------------------------------------
class MarketFileProcessor(ProgressBarMixin):
    """
    Processes market data and metadata files (market catalogues and market definitions).
    Two public methods are provided:
        - index_database(): Indexes a directory containing market data and metadata files.
        - update_database(): Inserts files from the source directory into the existing database.

    This class is a refactor of the now-removed BetfairDatabase._process_market_data() method,
    which had grown too big and complex to maintain. The class splits the code sections
    of that method into smaller, more manageable methods.
    """

    INVALID = TypeVar("INVALID")  # Sentinel for invalid metadata.json contents

    def __init__(
        self,
        database_dir: str | Path,
        progress_bar: bool = True,
    ):
        super().__init__(progress_bar)
        self.database_dir = Path(database_dir)
        self.debug_logging_enabled = is_debug_logging_enabled(logger)

    def index_database(self, connection: sqlite3.Connection) -> int:
        """
        Indexes the database by processing market data and metadata files.
        Returns the number of indexed markets.
        """
        return self._main(self.database_dir, connection)

    def update_database(
        self,
        source_dir: str | Path,
        connection: sqlite3.Connection,
        copy: bool = False,
        import_pattern: Callable[[dict], str] | None = None,
        on_duplicates: DuplicatePolicy | None = None,
    ) -> int:
        """
        Inserts the market data and metadata files from source_dir into the existing database.
        Returns the number of inserted markets.
        """
        return self._main(
            Path(source_dir), connection, copy, import_pattern, on_duplicates
        )

    def _main(self, source_dir: Path, connection: sqlite3.Connection, *args) -> int:
        """
        Main routine of this class.
        Called by the public methods `index_database` and `update_database`.
        """
        counters = Counters()

        # Locate files for processing
        files = self._locate_data_and_metadata_files(Path(source_dir))
        counters.total_markets = len(set(files.data) | set(files.individual_metadata))

        # Parse metadata.json files
        bulk_metadata_file_contents = self._parse_bulk_metadata_files(
            files.bulk_metadata
        )

        # Process metadata.json files
        # Slims down data files and makes the next call potentially faster
        markets_1 = self._process_bulk_metadata_files(
            files.data, bulk_metadata_file_contents
        )

        # Slims down individual_metadata files and makes the next call potentially faster
        self._remove_metadata_files_without_data(
            files.data, files.individual_metadata, counters
        )

        # Process market data files without a dedicated metadata file by opening
        # them and generating metadata.json from market definitions.
        # Slims down self.data_files and makes the next call potentially faster
        markets_2 = self._process_markets_without_metadata_files(
            files.data, files.individual_metadata, bulk_metadata_file_contents, counters
        )

        # Process individual market metadata files (catalogues and market definitions)
        markets_3 = self._process_individual_market_metadata_files(
            files.data, files.individual_metadata, counters
        )

        # Import markets into the database, optionally moving or copying files if needed
        importable_markets = markets_1 + markets_2 + markets_3
        self._import_markets_into_database(
            importable_markets, connection, counters, *args
        )
        action = "importing" if args else "indexing"
        counters.log_info(action)
        return counters.rows_inserted

    def _locate_data_and_metadata_files(self, source_dir: Path | None) -> MarketFiles:
        """
        Locates market data and metadata files in the source directory,
        linking them using a common dictionary key.
        """
        data_file_suffixes = list(DATA_FILE_SUFFIXES)
        data_file_suffixes.remove("")
        data_files = {}
        individual_metadata_files = {}
        bulk_metadata_files = []
        # pathlib.Path.rglob does not support brace expansion to look for multiple
        # patterns at one. Therefore, all files need to be located and filtered
        # manually to avoid traversing the whole directory tree more than once.
        for file in self._progress_bar(source_dir.rglob("*"), "Locating markets"):
            file_name = file.name
            if file_name.startswith("1."):
                file_suffix = file.suffix
                # Metadata files always have a .json extension
                if file_suffix == ".json":
                    individual_metadata_files[file.with_suffix("")] = file
                # Compressed data files
                elif file_suffix in data_file_suffixes:
                    data_files[file.with_suffix("")] = file
                # Uncompressed data files do not have an extension, but
                # the numbers following 1. are treated as one.
                # They usually have 9 "decimal places"
                elif len(file_suffix) > 8:
                    data_files[file] = file
            elif file_name == METADATA_FILE_NAME:
                bulk_metadata_files.append(file)
        return MarketFiles(data_files, individual_metadata_files, bulk_metadata_files)

    def _parse_bulk_metadata_files(
        self, bulk_metadata_files: list[Path]
    ) -> dict[Path, list[dict] | INVALID]:
        """Parses bulk metadata files and returns a lookup `{file_path: data}`."""
        bulk_metadata_file_contents = {}
        for file in self._progress_bar(
            bulk_metadata_files, f"Processing {METADATA_FILE_NAME} files"
        ):
            # Parse contents
            try:
                cache_value = contents = read_json(file)
                if not isinstance(contents, list):
                    cache_value = self.INVALID
                    logger.error(
                        "'%s' should be a list of dicts, not a %s.",
                        file,
                        contents.__class__.__name__,
                    )
            except JSONDecodeError:
                cache_value = self.INVALID
                # counters.corrupt_files += 1  # Would not pass validation
                logger.error("Error parsing '%s'.", file)

            bulk_metadata_file_contents[file] = cache_value
        return bulk_metadata_file_contents

    def _process_bulk_metadata_files(
        self,
        data_files: dict[Path, Path],
        bulk_metadata_file_contents: dict[Path, list[dict] | INVALID],
    ) -> list[Market]:
        """
        Processes bulk metadata (metadata.json) files. Returns a list of importable
        market objects with the metadata attached.

        Market data files for which the metadata has already been determined are removed
        from the cache.

        Because this routine is called before individual metadata files are processed,
        `<market_id>.json` files are skipped in case of metadata source duplication.
        """
        importable_markets = []
        for metadata_file, file_contents in self._progress_bar(
            bulk_metadata_file_contents.items(),
            f"Processing {METADATA_FILE_NAME} files",
        ):
            if file_contents is self.INVALID:
                continue

            # Generate a lookup {market_id: market_metadata}
            file_metadata_cache: dict[str, dict] = {}
            for market_metadata in file_contents:
                try:
                    file_metadata_cache[market_metadata[MARKET_ID]] = market_metadata
                except (KeyError, TypeError):
                    pass  # A warning is logged below

            # Check for invalid entries in the metadata file
            invalid_entries_count = len(file_contents) - len(file_metadata_cache)
            if invalid_entries_count:
                logger.error(
                    "'%s' contains %d invalid entries",
                    metadata_file,
                    invalid_entries_count,
                )

            for market_id, market_metadata in file_metadata_cache.items():
                data_file = data_files.pop(metadata_file.parent / market_id, None)
                if data_file is None:
                    logger.error(
                        "'%s' contains an entry for market ID '%s', "
                        "but a matching market data file cannot be found in the directory.",
                        metadata_file,
                        market_id,
                    )
                    continue
                importable_markets.append(
                    Market(metadata_file, data_file, market_metadata)
                )

        return importable_markets

    def _remove_metadata_files_without_data(
        self,
        data_files: dict[Path, Path],
        individual_metadata_files: dict[Path, Path],
        counters: Counters,
    ) -> None:
        """Removes metadata files without a corresponding data file from the cache."""
        metadata_files_without_data = {
            k: p for k, p in individual_metadata_files.items() if k not in data_files
        }
        counters.markets_without_data = len(metadata_files_without_data)

        if metadata_files_without_data:
            logger.error(
                "Missing market data file for metadata files: %s",
                [str(p) for p in metadata_files_without_data.values()],
            )
            for key in metadata_files_without_data:
                # Delete all metadata files without a corresponding data file
                del individual_metadata_files[key]

    @staticmethod
    def _backup_a_file(file: Path) -> Path:
        """
        Creates a backup file by appending .bak to the file name.
        If the backup file already exists, it gets replaced.
        """
        backup_file_name = file.with_suffix(file.suffix + ".bak")
        return file.replace(backup_file_name)

    def _parse_market_definitions_from_streams(
        self,
        data_files_without_metadata: dict[Path, Path],
        data_files: dict[Path, Path],
        counters: Counters,
    ) -> defaultdict[Path, dict[str, dict]]:
        """
        Parses market definitions from market data files and returns them
        in a mutlilevel dict: `{dir_path: {market_id: market_def}}`.
        """
        parsed_market_defs = defaultdict(dict)
        for stem_path, data_file in self._progress_bar(
            data_files_without_metadata.items(), "Creating metadata files"
        ):
            try:
                parsed_market_defs[stem_path.parent][stem_path.name] = (
                    MarketDefinitionProcessor.parse_market_definition(data_file)
                )
            except MarketDefinitionMissingError:
                del data_files[stem_path]
                counters.markets_without_metadata += 1
                logger.error("Market definition missing in '%s'.", data_file)
            except JSONDecodeError:
                del data_files[stem_path]
                counters.corrupt_files += 1
                logger.error("Error parsing '%s'.", data_file)
        return parsed_market_defs

    def _create_missing_metadata_files(
        self,
        parsed_market_defs: defaultdict[Path, dict[str, dict]],
        bulk_metadata_file_contents: dict[Path, dict | None],
    ) -> None:
        """Creates missing metadata files for data files missing them."""
        # Generate metadata.json files
        for file_dir, market_defs in parsed_market_defs.items():
            file = file_dir / METADATA_FILE_NAME
            # existing_data can be:
            # 1. INVALID: Not a list or not even JSON.
            # 2. None: The file does not exist.
            # 3. Valid data otherwise.
            metadata = bulk_metadata_file_contents.pop(file, None)
            if metadata is self.INVALID:  # The file exists but cannot be parsed
                backup_file = self._backup_a_file(file)
                logger.info(
                    "'%s' could not be updated and was renamed to '%s' to avoid overwriting it.",
                    file,
                    backup_file,
                )
                metadata = []
            elif metadata is None:  # The file does not exist
                metadata = []
            else:  # The file contains valid data and can be updated
                if self.debug_logging_enabled:
                    logger.debug("'%s' already exists and will be updated.", file)

            metadata.extend(market_defs.values())  # Merge metadata
            write_to_json(file, metadata)  # (Over)write metadata.json file
            if self.debug_logging_enabled:
                logger.debug("Generated metadata file '%s'.", file)

    def _process_markets_without_metadata_files(
        self,
        data_files: dict[Path, Path],
        individual_metadata_files: dict[Path, Path],
        bulk_metadata_file_contents: dict[Path, dict | None],
        counters: Counters,
    ) -> list[Market]:
        """
        1. Extracts market metadata from the market definition embedded inside market
        data files, if present.
        2. Updates existing metadata.json files.
        3. Returns a list of Market objects ready for import.
        """
        # Identify files for which this procedure needs to be performed
        data_files_without_metadata = {
            stem_path: full_path
            for stem_path, full_path in data_files.items()
            if stem_path not in individual_metadata_files
        }
        # Parse market definitions from stream files
        parsed_market_defs = self._parse_market_definitions_from_streams(
            data_files_without_metadata, data_files, counters
        )
        # Generate metadata.json files (or update existing)
        self._create_missing_metadata_files(
            parsed_market_defs, bulk_metadata_file_contents
        )
        # Return importable Market objects
        return [
            Market(
                file_dir / METADATA_FILE_NAME,
                data_files_without_metadata[file_dir / market_id],
                metadata,
            )
            for file_dir, subdict in parsed_market_defs.items()
            for market_id, metadata in subdict.items()
        ]

    def _process_individual_market_metadata_files(
        self,
        data_files: dict[Path, Path],
        individual_metadata_files: dict[Path, Path],
        counters: Counters,
    ) -> list[Market]:
        """
        Parses market metadata files, extract and injects additional metadata,
        returns a list of importable Market objects (with valid metadata).
        """
        importable_markets = []
        markets_gen = (
            Market(full_path, data_files[stem_path])
            for stem_path, full_path in individual_metadata_files.items()
        )
        for market in self._progress_bar(
            markets_gen, "Processing markets", total=len(individual_metadata_files)
        ):
            try:
                # Trigger metadata parsing to determine whether the market is importable
                market.metadata  # Trigger metadata parsing
                importable_markets.append(market)  # Metadata parsed
            except JSONDecodeError:
                counters.corrupt_files += 1
                logger.error("Error parsing '%s'.", market.market_metadata_file)
        return importable_markets

    def _import_markets_into_database(
        self,
        importable_markets: list[Market],
        connection: sqlite3.Connection,
        counters: Counters,
        copy: bool = False,
        import_pattern: Callable[[Market], str] | None = None,
        on_duplicates: DuplicatePolicy | None = None,
    ) -> None:
        """
        Imports the markets with valid metadata into the database.
        Returns the number of inserted SQL table rows.
        """
        racing_data_processor = RacingDataProcessor(
            importable_markets, self.progress_bar_enabled
        )
        update_existing_database = bool(import_pattern and on_duplicates)

        for market in self._progress_bar(importable_markets, "Importing markets"):
            # Database is being updated
            if update_existing_database:
                dest_dir = self.database_dir / import_pattern(market)
                # Move and copy are conditional on the duplicate handling policy
                # and set market.sql_action accordingly
                market = (
                    market.copy(dest_dir, on_duplicates)
                    if copy
                    else market.move(dest_dir, on_duplicates)
                )
                if market.sql_action is SQLAction.SKIP:
                    counters.markets_skipped += 1
                    if self.debug_logging_enabled:
                        logger.debug("Skipping '%s'.", market.market_data_file)
                    continue
                elif market.sql_action is SQLAction.UPDATE:
                    # SQL does not support updating a whole row at a time and requires one to list
                    # individual fields and values to update. A simpler way to achieve the same
                    # outcome is to delete and re-insert the row.
                    connection.execute(
                        f"DELETE FROM {SQL_TABLE_NAME}"
                        f" WHERE {MARKET_METADATA_FILE_PATH} = '{market.market_metadata_file.resolve()}'"
                    )
                    counters.markets_updated += 1
                    if self.debug_logging_enabled:
                        logger.debug("Updating '%s'.", market.market_data_file)

            # This section is always executed, for both updating and indexing
            sql_data_map = market.create_sql_mapping(
                racing_data_processor.get(
                    market
                )  # Inject additional racing metadata, if any
            )
            connection.execute(
                f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
                tuple(sql_data_map.values()),
            )
            counters.rows_inserted += 1
            if self.debug_logging_enabled and (market.sql_action is SQLAction.INSERT):
                logger.debug("Adding '%s'.", market.market_data_file)
