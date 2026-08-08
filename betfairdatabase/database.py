import contextlib
import csv
import logging
import os
import sqlite3
from pathlib import Path
from typing import Callable, Literal

from betfairdatabase.const import (
    INDEX_FILENAME,
    MARKET_DATA_FILE_PATH,
    ROWID,
    SQL_TABLE_COLUMNS,
    SQL_TABLE_NAME,
    DuplicatePolicy,
)
from betfairdatabase.exceptions import (
    DatabaseDirectoryError,
    IndexExistsError,
    IndexMissingError,
)
from betfairdatabase.imports import ImportPatterns
from betfairdatabase.processor import MarketFileProcessor
from betfairdatabase.racing import RacingDataProcessor
from betfairdatabase.utils import ProgressBarMixin, is_debug_logging_enabled

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


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
        debug_logging_enabled = is_debug_logging_enabled(logger)

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
