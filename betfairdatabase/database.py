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


class BetfairDatabase:
    """
    Transforms a directory of captured Betfair market data into
    a queryable SQL database.
    """

    def __init__(self, database_dir: str | Path):
        self.database_dir = Path(database_dir)
        self._index_file = self.database_dir / INDEX_FILENAME

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
        data_files_indexed = 0
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            conn.execute(
                f"CREATE TABLE {SQL_TABLE_NAME}({','.join(SQL_TABLE_COLUMNS)})"
            )
            for market_catalogue_file in self._locate_market_catalogues(
                self.database_dir
            ):
                try:
                    self._insert_row(conn, Market(market_catalogue_file))
                    data_files_indexed += 1
                except MarketDataFileError:
                    # Log warning that a data file is missing
                    pass
        return data_files_indexed

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
        rows_inserted = 0
        with contextlib.closing(sqlite3.connect(self._index_file)) as conn, conn:
            for market_catalogue_file in self._locate_market_catalogues(source_dir):
                try:
                    market = Market(market_catalogue_file)
                    dest_dir = self.database_dir / pattern(market.market_catalogue_data)
                    new_market = (
                        market.copy(dest_dir) if copy else market.move(dest_dir)
                    )
                    self._insert_row(conn, new_market)
                    rows_inserted += 1
                except MarketDataFileError:
                    # Log warning that a data file is missing
                    pass
                # FileExistsErrors are not handled
        return rows_inserted

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

    def export(self, dest_dir: str | Path = ".") -> Path:
        """
        Exports the database to a CSV file and returns the path to it.

        WARNING!
        This can be very slow and resource-intensive for large databases.
        No optimisations, such as chunkifying read data, are performed.
        """
        output_file = Path(dest_dir) / (self.database_dir.name + ".csv")
        data = self.select()
        if data:
            with open(output_file, "w", newline="") as f:
                writer = csv.DictWriter(f, data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        return output_file

    ################# PRIVATE METHODS #######################

    @staticmethod
    def _locate_market_catalogues(target_dir: str | Path) -> list[Path]:
        """
        Returns a list of path to market catalogues found in the
        target directory.
        """
        return Path(target_dir).rglob("1.*.json")

    @staticmethod
    def _insert_row(connection: sqlite3.Connection, market: Market):
        """
        Parses the market catalogue file, transforms the data into an SQL table row and
        imports the said row into an SQL table using the provided connection object.
        """
        sql_data_map = market.create_sql_mapping()
        connection.execute(
            f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
            tuple(sql_data_map.values()),
        )
