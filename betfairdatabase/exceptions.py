class BetfairDatabaseException(Exception):
    """Base exception for betfairdatabase module."""


class IndexMissingError(BetfairDatabaseException):
    """
    Raised when the database index is missing in the target directory.
    """

    def __init__(self, target_dir: str):
        msg = f"Betfair database index not found in {target_dir}."
        super().__init__(msg)


class IndexExistsError(BetfairDatabaseException):
    """
    Raised when the database index already exists in the target directory.
    """

    def __init__(self, target_dir: str, extra: str = ""):
        msg = f"Betfair database index already exists in {target_dir}." + extra
        super().__init__(msg)
