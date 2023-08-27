import contextlib
import csv
import sqlite3
from pathlib import Path
from typing import Callable

from betfairdatabase.const import INDEX_FILENAME, SQL_TABLE_COLUMNS, SQL_TABLE_NAME
from betfairdatabase.exceptions import (
    MarketDataFileError,
    IndexExistsError,
    IndexMissingError,
)
from betfairdatabase.market import Market
from betfairdatabase.utils import ImportPatterns
from betfairdatabase.racing import RacingDataProcessor


class BetfairDatabase:
    """
    Transforms a directory of captured Betfair market data into
    a queryable SQL database.
    """

    def __init__(self, database_dir: str | Path):
        self.database_dir = Path(database_dir)
        self._index_file = self.database_dir / INDEX_FILENAME
        self._racing_data_processor = RacingDataProcessor()

    def index(self, overwrite: bool = False) -> int:
        """
        Turns the target directory into a database by indexing its contents.
        Returns the number of indexed market data files.
        """
        # Check if index already exists and whether it should be overwritten
        if self._index_file.exists():
            if overwrite:
                self._index_file.unlink()
            else:
                raise IndexExistsError(
                    self.database_dir,
                    " Use overwrite=True option to reindex the database.",
                )
        # Construct index
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            conn.execute(
                f"CREATE TABLE {SQL_TABLE_NAME}({','.join(SQL_TABLE_COLUMNS)})"
            )
            return self._handle_market_catalogues(self.database_dir, conn)

    def insert(
        self,
        source_dir: str | Path,
        copy: bool = False,
        pattern: Callable[[dict], str] = ImportPatterns.betfair_historical,
    ) -> int:
        """
        Inserts market catalogue/data files from source_dir into the database.

        Returns the number of inserted table rows (market catalogue/data file pairs).

        Files must not exist at the destination, otherwise FileExistsError is raised.
        If copy is True, copies the files instead of moving them.
        A custom import pattern can be provided to instruct the database how to
        interally organise the files into directories.
        """
        if not self._index_file.exists():
            self.index()  # Make a database if it does not exist
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            return self._handle_market_catalogues(
                source_dir, conn, copy=copy, pattern=pattern
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
    def columns():
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
        return dest

    ################# PRIVATE METHODS #######################

    @staticmethod
    def _locate_market_catalogues(target_dir: str | Path) -> list[Path]:
        """
        Returns a list of path to market catalogues found in the
        target directory.
        """
        return Path(target_dir).rglob("1.*.json")

    def _handle_market_catalogues(
        self, source_dir: str | Path, connection: sqlite3.Connection, **kwargs
    ) -> int:
        """
        Processes market catalogues, converts the data to a tabular format and
        inserts it as rows into a SQL table.

        Returns the number of SQL table rows inserted. Optionally performs additional data processing
        for racing markets.
        """
        # racing = True
        copy = kwargs.get("copy", False)
        pattern = kwargs.get("pattern", None)
        rows_inserted = 0
        markets = list(Market(mc) for mc in self._locate_market_catalogues(source_dir))
        # Two-pass required, so cache generated Market objects in RAM
        for market in markets:
            self._racing_data_processor.add(market)  # Rejects non-racing markets
        for market in markets:
            try:
                if pattern is not None:
                    dest_dir = self.database_dir / pattern(market.market_catalogue_data)
                    market = market.copy(dest_dir) if copy else market.move(dest_dir)
                sql_data_map = market.create_sql_mapping(
                    # Rejects non-racing markets
                    self._racing_data_processor.get(market)
                )
                connection.execute(
                    f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
                    tuple(sql_data_map.values()),
                )
                rows_inserted += 1
            except MarketDataFileError:
                # Log warning that a data file is missing
                pass
            # FileExistsErrors are not handled
        return rows_inserted
