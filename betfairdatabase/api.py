from pathlib import Path

from .core import locate_index, construct_index, select_data, SQL_TABLE_COLUMNS
from .exceptions import IndexExistsError, IndexMissingError


def index(database_dir: str | Path, overwrite: bool = False) -> int:
    """
    Turns the target directory into a database by indexing its contents.
    Returns the number of indexed market data files.
    """
    if index_file := locate_index(database_dir):
        if overwrite:
            index_file.unlink()
        else:
            raise IndexExistsError(
                database_dir, " Use overwrite=True option to reindex the database."
            )
    return construct_index(database_dir)


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
    if not locate_index(database_dir):
        raise IndexMissingError(database_dir)
    return select_data(**locals())


def columns() -> list:
    """Returns a list of queryable database columns."""
    return list(SQL_TABLE_COLUMNS)
