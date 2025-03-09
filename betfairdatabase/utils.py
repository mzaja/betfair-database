import datetime as dt
import json
from os import SEEK_CUR, SEEK_END, SEEK_SET
from pathlib import Path
from zoneinfo import ZoneInfo

from smart_open import open

from betfairdatabase.const import ENCODING_UTF_8
from betfairdatabase.exceptions import MarketDefinitionMissingError

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
MARKET_DEFINITION = "marketDefinition"
MARKET_DEFINITION_BYTES = MARKET_DEFINITION.encode()
JSON_SEPARATORS = (",", ":")  # Eliminate unnecessary whitespace


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


def get_market_definition(market_data_file: Path) -> dict:
    """
    Reads a market data file and parses the market definition.
    Accepts both compressed and plaintext files.

    Market id, ordinarily a part of market change ("mc") message but not the market
    definition, is injected into the output data.

    The market definition is expected to be present either in the last (preferrable)
    or the first line of the file. If market definition cannot be found on these two
    lines, MarketDefinitionMissingError is raised.
    """
    # Using smart-open, so streams are decompressed on-the-fly
    with open(market_data_file, "rb") as f:
        # Try reading the last line first, because the final market definition
        # contains the most up-to-date data
        try:
            # Go 2 bytes from the end because the last one might be \n,
            # in which case this algorithm would exit prematurely
            f.seek(-2, SEEK_END)
            # One step forward, two steps back until a newline is read
            while f.read(1) != b"\n":
                f.seek(-2, SEEK_CUR)
        except OSError:
            # Reached the beginning of the file -> this is the only line
            f.seek(SEEK_SET)
        line = f.readline()  # Read the line from the current cursor position

        # If the last line does not contain a market definition, get the first line
        if MARKET_DEFINITION_BYTES not in line:
            f.seek(SEEK_SET)  # Go to the beginning
            line = f.readline()
            if MARKET_DEFINITION_BYTES not in line:
                raise MarketDefinitionMissingError(market_data_file)

    # Parse data, inject market ID and return the correct dict sub-class
    market_change_message = json.loads(line)["mc"][0]
    market_definition = market_change_message[MARKET_DEFINITION]
    market_definition["marketId"] = market_change_message["id"]  # Inject market ID
    return market_definition


def create_market_definition_file(market_data_file: Path) -> Path:
    """
    Creates a market definition file from the market data file and
    stores it in the same directory as <market_id>.json.
    Returns the path to the generated market definition file.

    Processing is skipped altogether if a file with the same name already exists.
    """
    output_file = market_data_file.with_suffix(".json")
    if not output_file.exists():
        output_file.write_text(
            json.dumps(
                get_market_definition(market_data_file), separators=JSON_SEPARATORS
            ),
            encoding=ENCODING_UTF_8,
        )
    return output_file


# ---------------------------------------------------------------------------
# CLASSES
# ---------------------------------------------------------------------------
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
