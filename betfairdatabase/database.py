import contextlib
import csv
import logging
import os
import sqlite3
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Callable, Iterable, Literal

from tqdm import tqdm

from betfairdatabase.const import (
    DATA_FILE_SUFFIXES,
    INDEX_FILENAME,
    MARKET_DATA_FILE_PATH,
    MARKET_METADATA_FILE_PATH,
    ROWID,
    SQL_TABLE_COLUMNS,
    SQL_TABLE_NAME,
    DuplicatePolicy,
    SQLAction,
)
from betfairdatabase.exceptions import (
    DatabaseDirectoryError,
    IndexExistsError,
    IndexMissingError,
    MarketDefinitionMissingError,
)
from betfairdatabase.market import Market
from betfairdatabase.racing import RacingDataProcessor
from betfairdatabase.utils import ImportPatterns, create_market_definition_file

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


def _is_debug_logging_enabled() -> bool:
    """
    Returns True if debug logging is enabled, else False.

    Performance-wise, it is over 10x more efficient to check this once and
    skip logging in-code than letting the logger check on every single call.
    """
    return (not logger.disabled) and (logger.getEffectiveLevel() <= logging.DEBUG)


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


class ProgressBarMixin:
    """Progress bar mixin class."""

    def __init__(self, progress_bar_enabled: bool):
        self.progress_bar_enabled = progress_bar_enabled

    # This is a private method because it is not supposed to be a part
    # of the public API. It is only used internally by the class.
    def _progress_bar(
        self,
        iterable: Iterable,
        name: str,
        unit: str = "markets",
        total: int | None = None,
    ) -> Iterable:
        """Applies the progress bar to the iterable."""
        if not self.progress_bar_enabled:
            return iterable
        else:
            if not unit.startswith(" "):
                unit = " " + unit
            return tqdm(iterable, desc=name, unit=unit, total=total)


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
        self.debug_logging_enabled = _is_debug_logging_enabled()
        # Initialise file caches
        self.metadata_files = {}  # Market catalogues or market definitions
        self.data_files = {}  # Stream files

    def index_database(self, connection: sqlite3.Connection) -> int:
        """
        Indexes the database by processing market data and metadata files.
        Returns the number of indexed markets.
        """
        # Locate files for processing
        self._locate_data_and_metadata_files(self.database_dir)
        self.counters.total_markets = len(
            set(self.data_files) | set(self.metadata_files)
        )
        # Process files without siblings
        self._remove_metadata_files_without_data()
        self._create_missing_metadata_files()

        # Process and import markets
        importable_markets = self._process_market_metadata_files()
        self._import_markets_into_database(importable_markets, connection)
        self.counters.log_info("indexing")
        return self.counters.rows_inserted

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
        # Locate files for processing
        self._locate_data_and_metadata_files(Path(source_dir))
        self.counters.total_markets = len(
            set(self.data_files) | set(self.metadata_files)
        )
        # Process files without siblings
        self._remove_metadata_files_without_data()
        self._create_missing_metadata_files()

        # Process and import markets
        importable_markets = self._process_market_metadata_files()
        self._import_markets_into_database(
            importable_markets, connection, copy, import_pattern, on_duplicates
        )
        self.counters.log_info("importing")
        return self.counters.rows_inserted

    def _locate_data_and_metadata_files(self, source_dir: Path | None) -> None:
        """
        Locates market data and metadata files in the source directory,
        linking them using a common dictionary key.

        Sets attributes: self.metadata_files, self.data_files.
        """
        data_file_suffixes = list(DATA_FILE_SUFFIXES)
        data_file_suffixes.remove("")
        for p in self._progress_bar(source_dir.rglob("1.*"), "Locating markets"):
            # Metadata files always have a .json extension
            if p.suffix == ".json":
                self.metadata_files[p.with_suffix("")] = p
            # Compressed data files
            elif p.suffix in data_file_suffixes:
                self.data_files[p.with_suffix("")] = p
            # Uncompressed data files do not have an extension, but
            # the numbers following 1. are treated as one.
            elif len(p.suffix) > 8:  # Market data files usually have 9 "decimal places"
                self.data_files[p] = p

    def _remove_metadata_files_without_data(self) -> None:
        """Removes metadata files without a corresponding data file from the cache."""
        metadata_files_without_data = {
            k: p for k, p in self.metadata_files.items() if k not in self.data_files
        }
        self.counters.markets_without_data = len(metadata_files_without_data)

        if metadata_files_without_data:
            logger.error(
                "Missing market data file for metadata files: %s",
                [f"'{p}'" for p in metadata_files_without_data.values()],
            )
            for key in metadata_files_without_data:
                # Delete all metadata files without a corresponding data file
                del self.metadata_files[key]

    def _create_missing_metadata_files(self) -> None:
        """Creates missing metadata files for data files missing them."""
        data_files_without_metadata = {
            k: p for k, p in self.data_files.items() if k not in self.metadata_files
        }
        for key, data_file in self._progress_bar(
            data_files_without_metadata.items(), "Creating metadata files"
        ):
            try:
                metadata_file = create_market_definition_file(data_file)
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
            Market(v, self.data_files[k]) for k, v in self.metadata_files.items()
        )
        for market in self._progress_bar(
            markets_gen, "Processing markets", total=len(self.metadata_files)
        ):
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
        import_pattern: Callable[[dict], str] | None = None,
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
                dest_dir = self.database_dir / import_pattern(market.metadata)
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
                        f" WHERE {MARKET_METADATA_FILE_PATH} = '{market.market_metadata_file}'"
                    )
                    self.counters.markets_updated += 1
                    if self.debug_logging_enabled:
                        logger.debug("Updating '%s'.", market.market_data_file)

            # This section is always executed, for both updating and indexing
            sql_data_map = market.create_sql_mapping(
                # Rejects non-racing markets
                self.racing_data_processor.get(market)
            )
            connection.execute(
                f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
                tuple(sql_data_map.values()),
            )
            self.counters.rows_inserted += 1
            if self.debug_logging_enabled and (market.sql_action is SQLAction.INSERT):
                logger.debug("Adding '%s'.", market.market_data_file)


# ---------------------------------------------------------------------------
# MAIN CLASS
# ---------------------------------------------------------------------------
class BetfairDatabase(ProgressBarMixin):
    """
    Transforms a directory of captured Betfair market data into
    a queryable SQL database.
    """

    def __init__(self, database_dir: str | Path, progress_bar: bool = True):
        super().__init__(progress_bar)
        self.database_dir = Path(database_dir)
        if not self.database_dir.exists():
            # This is the most elegant place to raise this error
            # since most methods depend on it, although it makes
            # it slightly awkward.
            raise DatabaseDirectoryError(f"'{database_dir}' does not exist.")
        if not self.database_dir.is_dir():
            raise DatabaseDirectoryError(f"'{database_dir}' is not a directory.")
        self._index_file = self.database_dir / INDEX_FILENAME
        self._racing_data_processor = RacingDataProcessor()

    def index(self, force: bool = False) -> int:
        """
        Turns the target directory into a database by indexing its contents.

        Returns the number of indexed market data files.
        Throws IndexExistsError if an index already exists. Use force=True to
        overwrite the existing index.
        """
        # Check if index already exists and whether it should be overwritten
        if self._index_file.exists():
            if force:
                self._index_file.unlink()
                logger.info("Overwriting an existing index at '%s'.", self._index_file)
            else:
                raise IndexExistsError(
                    self.database_dir,
                    " Use force=True option to reindex the database.",
                )
        # Construct index
        processor = MarketFileProcessor(self.database_dir, self.progress_bar_enabled)
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            conn.execute(
                f"CREATE TABLE {SQL_TABLE_NAME}({','.join(SQL_TABLE_COLUMNS)}"
                f", UNIQUE({','.join(SQL_TABLE_COLUMNS[-2:])}))"
            )
            return processor.index_database(conn)

    def insert(
        self,
        source_dir: str | Path,
        copy: bool = False,
        pattern: Callable[[dict], str] = ImportPatterns.betfair_historical,
        on_duplicates: (
            DuplicatePolicy | Literal["skip", "replace", "update"]
        ) = DuplicatePolicy.UPDATE,
    ) -> int:
        """
        Inserts market metadata and data files from source_dir into the database.

        Returns the number of inserted table rows (market metadata/data file pairs).

        A custom import pattern can be provided to instruct the database how to
        interally organise the files into directories.

        Procedure for handling duplicates is specified through `on_duplicates` argument:
            - `skip`: Duplicate files are not processed. Index is not updated.
            - `replace`: Existing duplicate files are replaced with incoming ones. Index is updated.
            - `update`:
                Existing market metadata file is replaced if the incoming file contains a change
                which is reflected in the index, and the index is updated. Market data files are
                replaced if the incoming data file is larger than the existing one.
        """
        if not self._index_file.exists():
            self.index()  # Make a database if it does not exist
        processor = MarketFileProcessor(self.database_dir, self.progress_bar_enabled)
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            return processor.update_database(
                source_dir,
                conn,
                copy=copy,
                import_pattern=pattern,
                on_duplicates=DuplicatePolicy(on_duplicates),
            )

    def select(
        self,
        columns: list[str] = None,
        where: str = None,
        limit: int = None,
        return_dict: bool = True,
    ) -> list[dict | tuple]:
        """
        Selects data from the index.

        Parameters:
            - database_dir: Main directory of the database initialised with 'index'.
            - columns: Names of columns to return. If not specified, returns all columns.
            - where: SQL "WHERE" query for selecting data from the database.
            - limit: Maximum number of entries to return. Returns all entries if not specified.
            - return_dict: If True, returns each entry as {column name: value} mapping. If False,
                            returns just the values (faster, but harder to work with).

        Returns:
            A list of dicts if return_dict=True, else a list of tuples.
        """
        # Cannot select data if it hasn't been indexed
        if not self._index_file.exists():
            raise IndexMissingError(self.database_dir)

        # Expand SQL queries
        query_columns = "*" if columns is None else ",".join(columns)
        query_where = "" if where is None else f"WHERE {where}"
        query_limit = "" if limit is None else f"LIMIT {limit}"

        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            values = conn.execute(
                f"SELECT {query_columns} FROM {SQL_TABLE_NAME} {query_where} {query_limit}"
            ).fetchall()

        if return_dict:
            return [dict(zip(columns or SQL_TABLE_COLUMNS, v)) for v in values]
        else:
            return values

    # Must be a callable as static property is not a thing
    @staticmethod
    def columns() -> list:
        """Returns a list of queryable database columns."""
        return list(SQL_TABLE_COLUMNS)

    def export(self, dest: str | Path = ".") -> Path:
        """
        Exports the database to a CSV file and returns the path to it.

        dest can be either a directory or a file name. If it is a directory,
        the output file name becomes database name + ".csv"

        WARNING!
        This can be very slow and resource-intensive for large databases.
        No optimisations, such as chunkifying read data, are performed.
        """
        dest = Path(dest)
        if dest.is_dir():
            dest /= self.database_dir.name + ".csv"
        data = self.select()
        if data:
            with open(dest, "w", newline="") as f:
                writer = csv.DictWriter(f, data[0].keys())
                writer.writeheader()
                writer.writerows(self._progress_bar(data, "Exporting", unit="rows"))
        logger.info("Exported %d rows to '%s'.", len(data), dest)
        return dest

    def clean(self) -> int:
        """
        Deletes all database entries with a missing market data file. Returns the
        number of removed entries.

        This method reduces the need to reindex the database whenever the files are
        removed from it. However, reindexing can be faster if a large number of files
        has been removed.
        """
        # Cannot process data if it has not been indexed
        if not self._index_file.exists():
            raise IndexMissingError(self.database_dir)

        rows_deleted = 0
        debug_logging_enabled = _is_debug_logging_enabled()

        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            # Iterate over table rows, test if market data file exists, mark files which don't
            cursor = conn.cursor()
            for row in self._progress_bar(
                conn.execute(
                    f"SELECT {ROWID}, {MARKET_DATA_FILE_PATH} FROM {SQL_TABLE_NAME}"
                ),
                "Cleaning",
                total=self._get_number_of_entries(conn),
            ):
                row_id, data_file_path = row
                if not os.path.exists(
                    data_file_path
                ):  # Faster than creating a Path object just to test this
                    cursor.execute(
                        f"UPDATE {SQL_TABLE_NAME}"
                        f" SET {MARKET_DATA_FILE_PATH} = NULL WHERE {ROWID} = {row_id}"
                    )
                    rows_deleted += 1
                    if debug_logging_enabled:
                        logger.debug("Removing '%s' from the database.", data_file_path)
            # Delete all marked rows
            conn.execute(
                f"DELETE FROM {SQL_TABLE_NAME} WHERE {MARKET_DATA_FILE_PATH} IS NULL"
            )
        logger.info("Removed %d entries from the database.", rows_deleted)
        return rows_deleted

    def size(self) -> int:
        """Returns the number of indexed entries in the database."""
        if not self._index_file.exists():
            raise IndexMissingError(self.database_dir)
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            return self._get_number_of_entries(conn)

    ################# PRIVATE METHODS #######################

    def _get_number_of_entries(self, connection: sqlite3.Connection) -> int:
        """
        Returns the number of rows in the database index.

        This method is preferred over size() when an already open
        connection exists because it does not carry the overhead
        of opening and closing a database connection.
        """
        return connection.execute(f"SELECT COUNT(*) FROM {SQL_TABLE_NAME}").fetchone()[
            0
        ]
