import contextlib
import csv
import datetime as dt
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Callable
from functools import cache, cached_property

INDEX_FILENAME = ".betfairdatabaseindex"
DATA_FILE_SUFFIXES = ("", ".zip", ".gz", ".bz2")
SQL_TABLE_NAME = "BetfairDatabaseIndex"
SQL_TABLE_COLUMNS = (
    "marketId",
    "marketName",
    "marketStartTime",
    "persistenceEnabled",
    "bspMarket",
    "marketTime",
    "suspendTime",
    "bettingType",
    "turnInPlayEnabled",
    "marketType",
    "priceLadderDescriptionType",
    "lineRangeInfoMarketUnit",
    "eachWayDivisor",
    "raceType",
    "runners",
    "eventTypeId",
    "eventTypeName",
    "competitionId",
    "competitionName",
    "eventId",
    "eventName",
    "eventCountryCode",
    "eventTimezone",
    "eventVenue",
    "eventOpenDate",
    # Keep these two fields at the end of the list
    "marketCatalogueFilePath",
    "marketDataFilePath",
)


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


class Market:
    """
    Holds the information about market catalogue and market data files.
    """

    def __init__(self, market_catalogue_file: str | Path):
        self.market_catalogue_file = Path(market_catalogue_file).resolve()

    @cached_property
    def market_data_file(self) -> Path | None:
        """Returns the path to the market data file for this market."""
        return self._locate_market_data_file()

    @cached_property
    def market_catalogue_data(self) -> dict:
        """Parsed market catalogue data."""
        with open(self.market_catalogue_file, encoding="utf-8") as f:
            return json.load(f)

    @cache
    def create_sql_mapping(self) -> dict[str, object]:
        """
        Returns a dictionary where keys are SQL table column names and
        values are values in a row.
        """
        sql_data_map = self._transform_market_catalogue()
        sql_data_map["marketCatalogueFilePath"] = str(self.market_catalogue_file)
        sql_data_map["marketDataFilePath"] = str(self.market_data_file)
        return sql_data_map

    def _locate_market_data_file(self) -> Path | None:
        """
        Locates the market data file, which is expected to be next to the
        market catalogue file and share the same basename.
        """
        for suffix in DATA_FILE_SUFFIXES:
            data_file = self.market_catalogue_file.with_suffix(suffix)
            if data_file.exists():
                return data_file.resolve()
        return None  # Market data file was not found

    @staticmethod
    def _flatten_subdict(parent_dict: dict[str, object], child_key: str) -> None:
        """
        Flattens a dictionary by combining parent and child's key names.
        Modifies the dictionary in place.
        """
        if subdict := parent_dict.pop(child_key, None):
            for subkey, value in subdict.items():
                # Preserve camel case in the combined key
                combined_key = child_key + subkey[0].upper() + subkey[1:]
                parent_dict[combined_key] = value

    def _transform_market_catalogue(self) -> dict:
        """
        Transforms parsed market catalogue data into a flat dict
        representation suitable for SQL table import.
        """
        # Break out unnecessary parts and those that need further processing
        data = self.market_catalogue_data.copy()

        if description := data.pop("description", None):
            self._flatten_subdict(description, "priceLadderDescription")
            self._flatten_subdict(description, "lineRangeInfo")
            data.update(description)

        if runners := data.pop("runners", None):
            data["runners"] = len(runners)  # Only note down the number of selections

        self._flatten_subdict(data, "eventType")
        self._flatten_subdict(data, "competition")
        self._flatten_subdict(data, "event")

        # All keys not in SQL_TABLE_COLUMNS are dropped
        return {k: data.get(k, None) for k in SQL_TABLE_COLUMNS}


def parse_market_catalogue(market_catalogue_file: str | Path) -> dict:
    """Parses the market catalogue and returns the result as a dictionary."""
    with open(market_catalogue_file, encoding="utf-8") as f:
        return json.load(f)


def flatten_subdict(parent_dict: dict[str, object], child_key: str) -> None:
    """
    Flattens a dictionary by combining parent and child's key names.
    Modifies the dictionary in place.
    """
    if subdict := parent_dict.pop(child_key, None):
        for subkey, value in subdict.items():
            # Preserve camel case in the combined key
            combined_key = child_key + subkey[0].upper() + subkey[1:]
            parent_dict[combined_key] = value


def transform_market_catalogue(market_catalogue_data: dict) -> dict:
    """
    Transforms parsed market catalogue data into a flat dict
    representation suitable for SQL table import.
    """
    # Break out unnecessary parts and those that need further processing
    data = market_catalogue_data.copy()

    if description := data.pop("description", None):
        flatten_subdict(description, "priceLadderDescription")
        flatten_subdict(description, "lineRangeInfo")
        data.update(description)

    if runners := data.pop("runners", None):
        data["runners"] = len(runners)  # Only note down the number of selections

    flatten_subdict(data, "eventType")
    flatten_subdict(data, "competition")
    flatten_subdict(data, "event")

    # All keys not in SQL_TABLE_COLUMNS are dropped
    return {k: data.get(k, None) for k in SQL_TABLE_COLUMNS}


def create_sql_mapping(
    market_catalogue_file: str | Path, market_data_file: str | Path
) -> dict[str, object]:
    """
    Returns a dictionary where keys are SQL table column names and
    values are values in a row.
    """
    # Make paths absolute
    market_catalogue_file = Path(market_catalogue_file).resolve()
    market_data_file = Path(market_data_file).resolve()
    sql_data_map = transform_market_catalogue(
        (parse_market_catalogue(market_catalogue_file))
    )
    sql_data_map["marketCatalogueFilePath"] = str(market_catalogue_file)
    sql_data_map["marketDataFilePath"] = str(market_data_file)
    return sql_data_map


def parse_data_and_insert_row(
    connection: sqlite3.Connection,
    market_catalogue_file: str | Path,
    market_data_file: str | Path,
):
    """
    Parses the market catalogue file, transforms the data into an SQL table row and
    imports the said row into an SQL table using the provided connection object.
    """
    sql_data_map = create_sql_mapping(market_catalogue_file, market_data_file)
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
            if data_file := locate_market_data_file(market_catalogue_file):
                parse_data_and_insert_row(conn, market_catalogue_file, data_file)
                data_files_indexed += 1
            else:
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


def locate_market_data_file(market_catalogue_file: str | Path) -> Path | None:
    """
    Locates the market data file, which is expected to be next to the
    market catalogue file and share the same basename.
    """
    for suffix in DATA_FILE_SUFFIXES:
        data_file = Path(market_catalogue_file).with_suffix(suffix)
        if data_file.exists():
            return data_file
    return None  # Market data file was not found


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
            if data_file := locate_market_data_file(market_catalogue_file):
                output_dir = Path(database_dir) / pattern(
                    parse_market_catalogue(market_catalogue_file)
                )
                market_catalogue_dest_file = output_dir / market_catalogue_file.name
                market_data_dest_file = output_dir / data_file.name
                if (
                    market_catalogue_dest_file.exists()
                    or market_data_dest_file.exists()
                ):
                    # Do not allow replacing existing files at the moment because that
                    # requires updating the SQL table
                    continue
                else:
                    # Copy or move the files to the destination
                    file_operation = shutil.copy if copy else shutil.move
                    file_operation(market_catalogue_file, market_catalogue_dest_file)
                    file_operation(data_file, market_data_dest_file)
                    parse_data_and_insert_row(
                        conn, market_catalogue_dest_file, market_data_dest_file
                    )
