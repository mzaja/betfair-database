class BetfairDatabaseError(Exception):
    """Base exception for betfairdatabase module."""


class DatabaseDirectoryError(BetfairDatabaseError):
    """
    Raised when a database directory is not a directory or it does not exist.
    """


class IndexMissingError(BetfairDatabaseError):
    """
    Raised when the database index is missing in the target directory.
    """

    def __init__(self, target_dir: str):
        msg = (
            f"Betfair database index not found in '{target_dir}'."
            " Index the database, then try again."
        )
        super().__init__(msg)


class IndexExistsError(BetfairDatabaseError):
    """
    Raised when the database index already exists in the target directory.
    """

    def __init__(self, target_dir: str, extra: str = ""):
        self.base_msg = f"Betfair database index already exists in '{target_dir}'."
        super().__init__(self.base_msg + extra)


class MarketDataFileError(BetfairDatabaseError):
    """
    Raised when a market data file cannot be found or an
    error is encountered when processing it.
    """

    def __init__(self, msg: str):
        super().__init__(msg)
