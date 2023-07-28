from pathlib import Path

from .core import (
    SQL_TABLE_COLUMNS,
    construct_index,
    export_data_to_csv,
    locate_index,
    select_data,
)
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


def export(database_dir: str | Path, dest_dir: str | Path = ".") -> Path:
    """
    Exports the database to a CSV file and returns the path to it.

    WARNING!
    This can be very slow and resource-intensive for large databases.
    No optimisations, such as chunkifying read data, are performed.
    """
    database_dir = Path(database_dir)
    ouput_file = Path(dest_dir) / (database_dir.name + ".csv")
    data = select(database_dir)
    export_data_to_csv(data, ouput_file)
    return Path(ouput_file)
