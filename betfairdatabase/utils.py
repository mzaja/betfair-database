import datetime as dt
from zoneinfo import ZoneInfo


def parse_datetime(datetime_str: str) -> dt.datetime:
    """
    Parses Betfair's ISO 8601 datetime format.

    Returns a timezone-aware datetime object.
    """
    try:
        # Python >= 3.11 parses timezone
        return dt.datetime.fromisoformat(datetime_str)
    except ValueError:
        # Python 3.10 does not, so remove "Zulu" time marker from the end annd
        # manually add the timezone
        return dt.datetime.fromisoformat(datetime_str.replace("Z", "")).replace(
            tzinfo=ZoneInfo("UTC")
        )


class ImportPatterns:
    """
    Contains patterns for mapping market catalogue data to the output
    directory path in the database.
    """

    @staticmethod
    def betfair_historical(market_catalogue_data: dict) -> str:
        """
        Generates the output directory path using the pattern
        "{year_no}/{month_name}/{day_no}/{event_id}"
        e.g. "2022/Jun/06/3828473", where reference time is "marketStartTime".
        This pattern is how official Betfair historical data files are organised.

        NOTE:
        The actual naming pattern Betfair uses is slightly different as the reference
        time is the event end time, rather than the market start time.
        If an event takes place across 4th and 5th of February 2022, all markets will
        be stored inside "2022/Feb/5/{event_id}", rather than a split of ".../Feb/4/..."
        and ".../Feb/5/...". However, since the event end time is not a part of the
        market catalogue, it is impossible to fully recreate this pattern.
        """
        market_time = parse_datetime(market_catalogue_data["marketStartTime"])
        event_id = market_catalogue_data["event"]["id"]
        return market_time.strftime(f"%Y/%b/{market_time.day}/{event_id}")

    @staticmethod
    def event_id(market_catalogue_data: dict) -> str:
        """Market data is stored into directories named after event ids."""
        return market_catalogue_data["event"]["id"]

    @staticmethod
    def flat(market_catalogue_data: dict) -> str:
        """Market data is stored directly into the base directory."""
        return ""
