import contextlib
import csv
import datetime as dt
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Callable
from functools import cache, cached_property

from betfairdatabase.const import INDEX_FILENAME, SQL_TABLE_COLUMNS, SQL_TABLE_NAME
from betfairdatabase.exceptions import MarketDataFileError
from betfairdatabase.market import Market


def construct_index_path(database_dir: str | Path) -> Path:
    """Returns the expected path of the database index."""
    return Path(database_dir) / INDEX_FILENAME


def locate_index(database_dir: str | Path) -> Path | None:
    """
    Returns the path to database index if it exists in the
    target directory. If the index is not found, returns None.
    """
    index_path = construct_index_path(database_dir)
    return index_path if index_path.exists() else None


def locate_market_catalogues(database_dir: str | Path) -> list[Path]:
    """
    Returns a list of path to market catalogues found in the
    database directory.
    """
    return Path(database_dir).rglob("1.*.json")


def insert_row(connection: sqlite3.Connection, market: Market):
    """
    Parses the market catalogue file, transforms the data into an SQL table row and
    imports the said row into an SQL table using the provided connection object.
    """
    if market.market_data_file is None:
        # The whole point of this package is to quickly access market data files,
        # so fail if they are missing.
        raise MarketDataFileError(
            f"Market data file is missing for market catalogue '{market.market_catalogue_file}'."
        )
    sql_data_map = market.create_sql_mapping()
    connection.execute(
        f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
        tuple(sql_data_map.values()),
    )


def construct_index(database_dir: str | Path) -> int:
    """
    Constructs the database index (in the form of another database) from source files
    located in database_dir.
    """
    data_files_indexed = 0
    with contextlib.closing(
        sqlite3.connect(construct_index_path(database_dir))
    ) as conn, conn:
        conn.execute(f"CREATE TABLE {SQL_TABLE_NAME}({','.join(SQL_TABLE_COLUMNS)})")
        for market_catalogue_file in locate_market_catalogues(database_dir):
            market = Market(market_catalogue_file)
            try:
                insert_row(conn, market)
                data_files_indexed += 1
            except MarketDataFileError:
                # Log warning that a data file is missing
                pass
    return data_files_indexed


def select_data(
    database_dir: str | Path,
    columns: list[str] = None,
    where: str = None,
    limit: int = None,
    return_dict: bool = True,
) -> list[dict | tuple]:
    """
    Selects data from the index.

    Parameters:
        - database_dir: Parent directory of the Betfair database
        - columns: Names of columns to return. If not specified, returns all columns.
        - where: SQL "WHERE" query for selecting data from the database.
        - limit: Maximum number of entries to return. Returns all entries if not specified.
        - return_dict: If True, returns each entry as {column name: value} mapping. If False,
                        returns just the values (faster, but harder to work with).

    Returns:
        A list of dicts if return_dict=True, else a list of tuples.
    """
    query_columns = "*" if columns is None else ",".join(columns)
    query_where = "" if where is None else f"WHERE {where}"
    query_limit = "" if limit is None else f"LIMIT {limit}"

    with contextlib.closing(
        sqlite3.connect(construct_index_path(database_dir))
    ) as conn, conn:
        values = conn.execute(
            f"SELECT {query_columns} FROM {SQL_TABLE_NAME} {query_where} {query_limit}"
        ).fetchall()

    if return_dict:
        return [dict(zip(columns or SQL_TABLE_COLUMNS, v)) for v in values]
    else:
        return values


def export_data_to_csv(data: list[dict], output_file: str | Path):
    """Exports the data given as a list of mappings to a CSV file."""
    if data:
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, data[0].keys())
            writer.writeheader()
            writer.writerows(data)


def parse_datetime(datetime_str: str) -> dt.datetime:
    """Parses Betfair's ISO 8601 datetime format."""
    try:
        # Python >= 3.11 parses timezone
        return dt.datetime.fromisoformat(datetime_str)
    except ValueError:
        # Python 3.10 does not, so remove "Zulu" time marker from the end
        return dt.datetime.fromisoformat(datetime_str.replace("Z", ""))


class ImportPatterns:
    """
    Contains patterns for mapping market catalogue data to the output
    directory path in the database.
    """

    @staticmethod
    def betfair_historical(market_catalogue_data: dict) -> str:
        """
        Generates the output directory path using the pattern
        "{year_no}/{month_name}/{day_no}/{event_id}"
        e.g. "2022/Jun/06/3828473", where reference time is "marketStartTime".
        This pattern is how official Betfair historical data files are organised.

        NOTE:
        The actual naming pattern Betfair uses is slightly different as the reference
        time is the event end time, rather than the market start time.
        If an event takes place across 4th and 5th of February 2022, all markets will
        be stored inside "2022/Feb/5/{event_id}", rather than a split of ".../Feb/4/..."
        and ".../Feb/5/...". However, since the event end time is not a part of the
        market catalogue, it is impossible to fully recreate this pattern.
        """
        market_time = parse_datetime(market_catalogue_data["marketStartTime"])
        event_id = market_catalogue_data["event"]["id"]
        return market_time.strftime(f"%Y/%b/{market_time.day}/{event_id}")

    @staticmethod
    def event_id(market_catalogue_data: dict) -> str:
        """Market data is stored into directories named after event ids."""
        return market_catalogue_data["event"]["id"]

    @staticmethod
    def flat(market_catalogue_data: dict) -> str:
        """Market data is stored directly into the base directory."""
        return ""


def insert(
    database_dir: str | Path,
    source_dir: str | Path,
    copy: bool = False,
    pattern: Callable[[dict], str] = ImportPatterns.betfair_historical,
):
    """
    If copy is True, copies the files instead of moving them.
    """
    if not locate_index(database_dir):
        construct_index(database_dir)
    with contextlib.closing(
        sqlite3.connect(construct_index_path(database_dir))
    ) as conn, conn:
        for market_catalogue_file in locate_market_catalogues(source_dir):
            market = Market(market_catalogue_file)
            dest_dir = Path(database_dir) / pattern(market.market_catalogue_data)
            new_market = market.copy(dest_dir) if copy else market.move(dest_dir)
            insert_row(conn, new_market)
