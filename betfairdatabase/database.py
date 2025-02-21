import contextlib
import csv
import logging
import os
import sqlite3
from json import JSONDecodeError
from pathlib import Path
from typing import Callable, Literal

from betfairdatabase.const import (
    INDEX_FILENAME,
    MARKET_CATALOGUE_FILE_PATH,
    MARKET_DATA_FILE_PATH,
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
    MarketDataFileError,
)
from betfairdatabase.market import Market
from betfairdatabase.racing import RacingDataProcessor
from betfairdatabase.utils import ImportPatterns

logger = logging.getLogger(__name__)


def _is_debug_logging_enabled() -> bool:
    """
    Returns True if debug logging is enabled, else False.

    Performance-wise, it is over 10x more efficient to check this once and
    skip logging in-code than letting the logger check on every single call.
    """
    return (not logger.disabled) and (logger.getEffectiveLevel() <= logging.DEBUG)


class BetfairDatabase:
    """
    Transforms a directory of captured Betfair market data into
    a queryable SQL database.
    """

    def __init__(self, database_dir: str | Path):
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
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            conn.execute(
                f"CREATE TABLE {SQL_TABLE_NAME}({','.join(SQL_TABLE_COLUMNS)}"
                f", UNIQUE({','.join(SQL_TABLE_COLUMNS[-2:])}))"
            )
            return self._process_market_data(self.database_dir, conn)

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
        Inserts market catalogue/data files from source_dir into the database.

        Returns the number of inserted table rows (market catalogue/data file pairs).

        A custom import pattern can be provided to instruct the database how to
        interally organise the files into directories.

        Procedure for handling duplicates is specified through `on_duplicates` argument:
            - `skip`: Duplicate files are not processed. Index is not updated.
            - `replace`: Existing duplicate files are replaced with incoming ones. Index is updated.
            - `update`:
                Existing market catalogue file is replaced if the incoming file contains a change
                which is reflected in the index, and the index is updated. Market data files are
                replaced if the incoming data file is larger than the existing one.
        """
        duplicate_policy = DuplicatePolicy(on_duplicates)
        if not self._index_file.exists():
            self.index()  # Make a database if it does not exist
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            return self._process_market_data(
                source_dir,
                conn,
                copy=copy,
                import_pattern=pattern,
                on_duplicates=duplicate_policy,
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
                writer.writerows(data)
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
            for row in conn.execute(
                f"SELECT {ROWID}, {MARKET_DATA_FILE_PATH} FROM {SQL_TABLE_NAME}"
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

    ################# PRIVATE METHODS #######################

    @staticmethod
    def _locate_market_catalogues(target_dir: str | Path) -> list[Path]:
        """
        Returns a list of path to market catalogues found in the
        target directory.
        """
        return Path(target_dir).rglob("1.*.json")

    def _process_market_data(
        self,
        source_dir: str | Path,
        connection: sqlite3.Connection,
        copy: bool = False,
        import_pattern: Callable[[dict], str] | None = None,
        on_duplicates: DuplicatePolicy | None = None,
    ) -> int:
        """
        Processes market catalogues, converts the data to a tabular format and
        inserts it as rows into a SQL table.

        Returns the number of SQL table rows inserted. Optionally performs additional
        data processing for racing markets.

        copy, import_pattern and on_duplicates need to be provided when inserting data
        into the database, but should be omitted when indexing the database.
        """
        rows_inserted = 0
        corrupt_markets = []
        markets = list(Market(mc) for mc in self._locate_market_catalogues(source_dir))
        total_markets_count = len(markets)
        markets_updated_count = 0
        markets_skipped_count = 0
        missing_data_files_count = 0
        updating_existing_database = bool(import_pattern and on_duplicates)
        debug_logging_enabled = _is_debug_logging_enabled()

        # Two-pass required, so cache generated Market objects in RAM
        for market in markets:
            try:
                self._racing_data_processor.add(market)  # Rejects non-racing markets
            except JSONDecodeError:
                logger.error("Error parsing '%s'.", market.market_catalogue_file)
                corrupt_markets.append(market)
        for market in corrupt_markets:
            markets.remove(market)
        for market in markets:
            try:
                # Database is being updated
                if updating_existing_database:
                    dest_dir = self.database_dir / import_pattern(
                        market.market_catalogue_data
                    )
                    # Move and copy are conditional on the duplicate handling policy
                    # and set market.sql_action accordingly
                    market = (
                        market.copy(dest_dir, on_duplicates)
                        if copy
                        else market.move(dest_dir, on_duplicates)
                    )
                    if market.sql_action is SQLAction.SKIP:
                        markets_skipped_count += 1
                        if debug_logging_enabled:
                            logger.debug("Skipping '%s'.", market.market_data_file)
                        continue

                # This code block is only ever executed when updating the database
                if market.sql_action is SQLAction.UPDATE:
                    # SQL does not support updating a whole row at a time and requires one to list
                    # individual fields and values to update. A simpler way to achieve the same
                    # outcome is to delete and re-insert the row.
                    connection.execute(
                        f"DELETE FROM {SQL_TABLE_NAME}"
                        f" WHERE {MARKET_CATALOGUE_FILE_PATH} = '{market.market_catalogue_file}'"
                    )
                    markets_updated_count += 1
                    if debug_logging_enabled:
                        logger.debug("Updating '%s'.", market.market_data_file)

                # This section is always executed, for both updating and indexing
                sql_data_map = market.create_sql_mapping(
                    # Rejects non-racing markets
                    self._racing_data_processor.get(market)
                )
                connection.execute(
                    f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
                    tuple(sql_data_map.values()),
                )
                rows_inserted += 1
                if debug_logging_enabled and (market.sql_action is SQLAction.INSERT):
                    logger.debug("Adding '%s'.", market.market_data_file)

            except MarketDataFileError:
                missing_data_files_count += 1
                logger.error(
                    "Missing market data file for catalogue '%s'",
                    market.market_catalogue_file,
                )
        logger.info(
            "Finished %s %d markets.",
            "importing" if updating_existing_database else "indexing",
            total_markets_count,
        )
        logger.info("Added: %d", rows_inserted - markets_updated_count)
        if updating_existing_database:
            logger.info("Updated: %d", markets_updated_count)
            logger.info("Skipped: %d", markets_skipped_count)
        logger.info("Corrupt: %d", len(corrupt_markets))
        logger.info("Incomplete: %d", missing_data_files_count)
        return rows_inserted
