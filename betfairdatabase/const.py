from enum import Enum

INDEX_FILENAME = ".betfairdatabaseindex"
DATA_FILE_SUFFIXES = ("", ".zip", ".gz", ".bz2")
SQL_TABLE_NAME = "BetfairDatabaseIndex"
ROWID = "rowid"
MARKET_DATA_FILE_PATH = "marketDataFilePath"
MARKET_CATALOGUE_FILE_PATH = "marketCatalogueFilePath"
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
    "localDayOfWeek",
    "localMarketStartTime",
    "localEventOpenDate",
    "raceId",
    "raceTypeFromName",
    "raceDistanceMeters",
    "raceDistanceFurlongs",
    # Keep these two fields at the end of the list
    MARKET_CATALOGUE_FILE_PATH,
    MARKET_DATA_FILE_PATH,
)


class DuplicatePolicy(Enum):
    """
    Policy for handling duplicates when inserting data into an existing database.

    - SKIP: Duplicate files are left where they are and not processed.
    - REPLACE: Duplicate files in the database are always replaced with new files.
    - UPDATE:
        Duplicate files in the database are replaced with new files if:
            1. Market catalogues are different.
            2. Existing market data file is smaller than the new market data file.
    """

    SKIP = "skip"
    REPLACE = "replace"
    UPDATE = "update"


class SQLAction(Enum):
    """
    Specifies the SQL action to perform on a Market object.
    """

    INSERT = "INSERT"
    SKIP = "SKIP"
    UPDATE = "UPDATE"
