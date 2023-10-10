from pathlib import Path
from typing import Callable, Literal

from betfairdatabase.const import DuplicatePolicy
from betfairdatabase.database import BetfairDatabase
from betfairdatabase.utils import ImportPatterns


def index(database_dir: str | Path, force: bool = False) -> int:
    """
    Turns the target directory into a database by indexing its contents.

    Returns the number of indexed market data files.
    Throws IndexExistsError if an index already exists. Use force=True to
    overwrite the existing index.
    """
    return BetfairDatabase(database_dir).index(force)


def select(
    database_dir: str | Path,
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
    return BetfairDatabase(database_dir).select(columns, where, limit, return_dict)


def columns() -> list:
    """Returns a list of queryable database columns."""
    return BetfairDatabase.columns()


def export(database_dir: str | Path, dest: str | Path = ".") -> Path:
    """
    Exports the database to a CSV file and returns the path to it.

    dest can be either a directory or a file name. If it is a directory,
    the output file name becomes database name + ".csv"

    WARNING!
    This can be very slow and resource-intensive for large databases.
    No optimisations, such as chunkifying read data, are performed.
    """
    return BetfairDatabase(database_dir).export(dest)


def insert(
    database_dir: str | Path,
    source_dir: str | Path,
    copy: bool = False,
    pattern: Callable[[dict], str] = ImportPatterns.betfair_historical,
    on_duplicates: DuplicatePolicy
    | Literal["skip", "replace", "update"] = DuplicatePolicy.UPDATE,
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
    return BetfairDatabase(database_dir).insert(
        source_dir, copy, pattern, on_duplicates
    )


def clean(database_dir: str | Path):
    """
    Deletes all database entries with a missing market data file.

    This method reduces the need to reindex the database whenever the files are
    removed from it. However, reindexing can be faster if a large number of files
    has been removed.
    """
    return BetfairDatabase(database_dir).clean()
