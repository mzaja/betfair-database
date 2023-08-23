from pathlib import Path
from betfairdatabase.database import BetfairDatabase


def index(database_dir: str | Path, overwrite: bool = False) -> int:
    """
    Turns the target directory into a database by indexing its contents.
    Returns the number of indexed market data files.
    """
    return BetfairDatabase(database_dir).index(overwrite)


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


def export(database_dir: str | Path, dest_dir: str | Path = ".") -> Path:
    """
    Exports the database to a CSV file and returns the path to it.

    WARNING!
    This can be very slow and resource-intensive for large databases.
    No optimisations, such as chunkifying read data, are performed.
    """
    return BetfairDatabase(database_dir).export(dest_dir)
