INDEX_FILENAME = ".betfairdatabaseindex"
DATA_FILE_SUFFIXES = ("", ".zip", ".gz", ".bz2")
SQL_TABLE_NAME = "BetfairDatabaseIndex"
ROWID = "rowid"
MARKET_DATA_FILE_PATH = "marketDataFilePath"
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
    "marketCatalogueFilePath",
    MARKET_DATA_FILE_PATH,
)
