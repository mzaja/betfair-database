import datetime as dt
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------
def parse_datetime(datetime_str: str) -> dt.datetime:
    """
    Parses Betfair's ISO 8601 datetime format.
    Returns a timezone-aware datetime object.
    """
    try:
        # Python >= 3.11 parses timezone
        return dt.datetime.fromisoformat(datetime_str)
    except ValueError:
        # Python 3.10 does not, so remove "Zulu" time marker from the end and
        # manually add the timezone
        return dt.datetime.fromisoformat(datetime_str.replace("Z", "")).replace(
            tzinfo=ZoneInfo("UTC")
        )
