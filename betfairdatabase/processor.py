import json
import logging
import sqlite3
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Callable, Literal

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
from betfairdatabase.utils import ProgressBarMixin, is_debug_logging_enabled

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

    def __init__(
        self,
        database_dir: str | Path,
        progress_bar: bool = True,
    ):
        super().__init__(progress_bar)
        self.database_dir = Path(database_dir)
        self.counters = Counters()
        self.racing_data_processor = RacingDataProcessor()
        self.market_definition_processor = MarketDefinitionProcessor(
            cache_parsed_definitions=True
        )
        self.debug_logging_enabled = is_debug_logging_enabled(logger)
        # Initialise file caches
        self.metadata_files: dict[Path, Path] = {}  # Market catalogues/definitions
        self.data_files: dict[Path, Path] = {}  # Stream files
        self.bulk_metadata_files: list[Path] = []  # metadata.json files

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
        action = "importing" if args else "indexing"

        # Locate files for processing
        self._locate_data_and_metadata_files(Path(source_dir))
        self.counters.total_markets = len(
            set(self.data_files) | set(self.metadata_files)
        )

        # Process metadata.json files
        importable_markets_1 = self._process_bulk_metadata_files()

        # Process individual market metadata files (catalogues and market definitions)
        self._remove_metadata_files_without_data()
        self._create_missing_metadata_files()
        importable_markets_2 = self._process_market_metadata_files()

        # Import markets into the database, optionally moving or copying files if needed
        importable_markets = importable_markets_1 + importable_markets_2
        self._import_markets_into_database(importable_markets, connection, *args)
        self.counters.log_info(action)
        return self.counters.rows_inserted

    def _locate_data_and_metadata_files(self, source_dir: Path | None) -> None:
        """
        Locates market data and metadata files in the source directory,
        linking them using a common dictionary key.

        Sets attributes: `self.metadata_files`, `self.data_files`.
        """
        data_file_suffixes = list(DATA_FILE_SUFFIXES)
        data_file_suffixes.remove("")
        # pathlib.Path.rglob does not support brace expansion to look for multiple
        # patterns at one. Therefore, all files need to be located and filtered
        # manually to avoid traversing the whole directory tree more than once.
        for file in self._progress_bar(source_dir.rglob("*"), "Locating markets"):
            file_name = file.name
            if file_name.startswith("1."):
                file_suffix = file.suffix
                # Metadata files always have a .json extension
                if file_suffix == ".json":
                    self.metadata_files[file.with_suffix("")] = file
                # Compressed data files
                elif file_suffix in data_file_suffixes:
                    self.data_files[file.with_suffix("")] = file
                # Uncompressed data files do not have an extension, but
                # the numbers following 1. are treated as one.
                # They usually have 9 "decimal places"
                elif len(file_suffix) > 8:
                    self.data_files[file] = file
            elif file_name == METADATA_FILE_NAME:
                self.bulk_metadata_files.append(file)

    def _process_bulk_metadata_files(self) -> list[Market]:
        """
        Processes bulk metadata (metadata.json) files. Returns a list of importable
        market objects with the metadata attached.

        Market data files for which the metadata has already been determined are removed
        from the cache.

        Because this routine is called before individual metadata files are processed,
        `<market_id>.json` files are skipped in case of metadata source duplication.
        """
        importable_markets = []
        for metadata_file in self._progress_bar(
            self.bulk_metadata_files, f"Processing {METADATA_FILE_NAME} files"
        ):
            # Parse contents
            try:
                file_entries: list[dict] = json.loads(metadata_file.read_bytes())
            except JSONDecodeError:
                # self.counters.corrupt_files += 1  # Would not pass validation
                logger.error("Error parsing '%s'.", metadata_file)
                continue

            if not isinstance(file_entries, list):
                logger.error(
                    "'%s' should be a list of dicts, not a %s.",
                    metadata_file,
                    file_entries.__class__.__name__,
                )
                continue

            # Process contents
            file_cache: dict[str, dict] = {}
            for market_metadata in file_entries:
                try:
                    file_cache[market_metadata[MARKET_ID]] = market_metadata
                except (KeyError, TypeError):
                    pass  # A warning is logged below

            # Check for invalid entries in the metadata file
            invalid_entries_count = len(file_entries) - len(file_cache)
            if invalid_entries_count:
                logger.error(
                    "'%s' contains %d invalid entries",
                    metadata_file,
                    invalid_entries_count,
                )

            for market_id, market_metadata in file_cache.items():
                data_file = self.data_files.pop(metadata_file.parent / market_id, None)
                if data_file is None:
                    logger.error(
                        "'%s' contains an entry for market ID '%s', "
                        "but a matching market data file cannot be found in the directory.",
                        metadata_file,
                        market_id,
                    )
                    continue
                market = Market(metadata_file, data_file)
                market.attach_metadata(market_metadata)
                self.racing_data_processor.add(market)
                importable_markets.append(market)

        return importable_markets

    def _remove_metadata_files_without_data(self) -> None:
        """Removes metadata files without a corresponding data file from the cache."""
        metadata_files_without_data = {
            k: p for k, p in self.metadata_files.items() if k not in self.data_files
        }
        self.counters.markets_without_data = len(metadata_files_without_data)

        if metadata_files_without_data:
            logger.error(
                "Missing market data file for metadata files: %s",
                [str(p) for p in metadata_files_without_data.values()],
            )
            for key in metadata_files_without_data:
                # Delete all metadata files without a corresponding data file
                del self.metadata_files[key]

    def _create_missing_metadata_files(self) -> None:
        """Creates missing metadata files for data files missing them."""
        data_files_without_metadata = {
            stem_path: full_path
            for stem_path, full_path in self.data_files.items()
            if stem_path not in self.metadata_files
        }
        for key, data_file in self._progress_bar(
            data_files_without_metadata.items(), "Creating metadata files"
        ):
            try:
                metadata_file = (
                    self.market_definition_processor.create_market_definition_file(
                        data_file
                    )
                )
                # Add the generated metadata file to registry
                self.metadata_files[key] = metadata_file
                if self.debug_logging_enabled:
                    logger.debug("Created metadata file for '%s'.", data_file)
            except MarketDefinitionMissingError:
                self.counters.markets_without_metadata += 1
                logger.error("Market definition missing in '%s'.", data_file)
            except JSONDecodeError:
                self.counters.corrupt_files += 1
                logger.error("Error parsing '%s'.", data_file)

    def _process_market_metadata_files(self) -> list[Market]:
        """
        Parses market metadata files, extract and injects additional metadata,
        returns a list of importable Market objects (with valid metadata).
        """
        importable_markets = []
        markets_gen = (
            Market(full_path, self.data_files[stem_path])
            for stem_path, full_path in self.metadata_files.items()
        )
        for market in self._progress_bar(
            markets_gen, "Processing markets", total=len(self.metadata_files)
        ):
            # If market definition has already been parsed, attach it rather
            # than opening and parsing the market definition file again
            market.attach_metadata(
                self.market_definition_processor.parsed_definitions.pop(
                    market.market_metadata_file, None
                )
            )
            try:
                # Racing data processor triggers the parsing of the market metadata
                # because it needs to check whether this is a racing market.
                # Non-racing markets are ignored by the racing data processor.
                self.racing_data_processor.add(market)
                # No error parsing the market metadata means it is importable
                importable_markets.append(market)
            except JSONDecodeError:
                self.counters.corrupt_files += 1
                logger.error("Error parsing '%s'.", market.market_metadata_file)
        return importable_markets

    def _import_markets_into_database(
        self,
        importable_markets: list[Market],
        connection: sqlite3.Connection,
        copy: bool = False,
        import_pattern: Callable[[Market], str] | None = None,
        on_duplicates: DuplicatePolicy | None = None,
    ) -> None:
        """
        Imports the markets with valid metadata into the database.
        Returns the number of inserted SQL table rows.
        """
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
                    self.counters.markets_skipped += 1
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
                    self.counters.markets_updated += 1
                    if self.debug_logging_enabled:
                        logger.debug("Updating '%s'.", market.market_data_file)

            # This section is always executed, for both updating and indexing
            sql_data_map = market.create_sql_mapping(
                self.racing_data_processor.get(market)  # Rejects non-racing market
            )
            connection.execute(
                f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
                tuple(sql_data_map.values()),
            )
            self.counters.rows_inserted += 1
            if self.debug_logging_enabled and (market.sql_action is SQLAction.INSERT):
                logger.debug("Adding '%s'.", market.market_data_file)
