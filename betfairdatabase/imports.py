from betfairdatabase.market import Market
from betfairdatabase.metadata import MarketCatalogueData, MarketDefinitionData
from betfairdatabase.utils import parse_datetime


class ImportPatterns:
    """
    Contains patterns for mapping market metadata to the output
    directory path in the database.
    """

    @staticmethod
    def betfair_historical(market: Market) -> str:
        """
        Generates the output directory path using the pattern
        "{year_no}/{month_name}/{day_no}/{event_id}"
        e.g. "2022/Jun/06/3828473", where reference time is market settled time ("settledTime"),
        if one exists. If market settled time does not exist, market start time is used instead.
        This pattern is how official Betfair historical data files are organised.

        NOTE:
        The actual naming pattern Betfair uses is slightly different as the reference
        time is the event end time, rather than the market start or end time.
        If an event takes place across 4th and 5th of February 2022, all markets will
        be stored inside "2022/Feb/5/{event_id}", rather than a split of ".../Feb/4/..."
        and ".../Feb/5/...". However, since the event end time is not a part of the
        market catalogue, it is impossible to completely recreate this pattern.
        """
        metadata = market.metadata
        market_settled_time = metadata.get("settledTime")
        if isinstance(metadata, MarketCatalogueData):
            market_time = parse_datetime(
                market_settled_time or metadata["marketStartTime"]
            )
            event_id = metadata["event"]["id"]
        elif isinstance(metadata, MarketDefinitionData):
            market_time = parse_datetime(market_settled_time or metadata["marketTime"])
            event_id = metadata["eventId"]
        return market_time.strftime(f"%Y/%b/{market_time.day}/{event_id}")

    @staticmethod
    def event_id(market: Market) -> str:
        """Market data is stored into directories named after event ids."""
        metadata = market.metadata
        if isinstance(metadata, MarketCatalogueData):
            return metadata["event"]["id"]
        elif isinstance(metadata, MarketDefinitionData):
            return metadata["eventId"]

    @staticmethod
    def flat(market: Market) -> str:
        """Market data is stored directly into the base directory."""
        return ""
