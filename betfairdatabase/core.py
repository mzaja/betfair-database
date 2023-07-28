import contextlib
import csv
import json
import sqlite3
from pathlib import Path

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


def parse_market_catalogue(market_catalogue_file: str | Path) -> dict:
    """Parses the market catalogue and"""
    with open(market_catalogue_file) as f:
        return json.load(f)


def flatten_subdict(parent_dict: dict[str, object], child_key: str) -> None:
    """
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
            for suffix in DATA_FILE_SUFFIXES:
                data_file = market_catalogue_file.with_suffix(suffix)
                if data_file.exists():
                    sql_data_map = create_sql_mapping(market_catalogue_file, data_file)
                    conn.execute(
                        f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
                        tuple(sql_data_map.values()),
                    )
                    data_files_indexed += 1
                    break
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
