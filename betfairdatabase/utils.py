import datetime as dt
import json
from bz2 import BZ2File
from io import BufferedReader
from os import SEEK_CUR, SEEK_END, SEEK_SET
from pathlib import Path
from zoneinfo import ZoneInfo

from betfairdatabase.const import ENCODING_UTF_8
from betfairdatabase.exceptions import MarketDefinitionMissingError

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
MARKET_DEFINITION = "marketDefinition"
MARKET_DEFINITION_BYTES = MARKET_DEFINITION.encode()
JSON_SEPARATORS = (",", ":")  # Eliminate unnecessary whitespace
REVERSE_READ_STEP = 64 * 1024  # Characters to start reading a file from reverse
# Store supported decompressors in this dict
DECOMPRESSORS = {".bz2": BZ2File}


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


def read_last_line_in_a_text_file(file_reader: BufferedReader) -> bytes:
    """
    Reads the last line in a text file by jumping to the end of the file and
    moving backwards in steps of 64 KiB. This size should be sufficient to immediately
    locate the last line in the vast majority of Betfair stream files. However, in
    exceptional cases where this is not sufficient, the window keeps moving backwards
    until the beginning of the line is found.

    The function does not work on compressed files because they cannot be incrementally
    decompressed from the rear. f.seek() in compressed files only works by sequentially
    decompressing the file up until that point, defeating the prupose of jumping to the
    back. For compressed files, it is faster and cleaner to simply decompress the whole
    file and read the lines in reverse.
    """
    buffer = b""
    file_reader.seek(0, SEEK_END)  # Go to the end of the file
    while True:
        bytes_from_beginning = file_reader.tell()
        if bytes_from_beginning > REVERSE_READ_STEP:
            read_step = REVERSE_READ_STEP
            whole_file = False
        else:
            read_step = bytes_from_beginning
            whole_file = True
        # Move back by the step size, then read that many bytes
        file_reader.seek(-read_step, SEEK_CUR)
        buffer = file_reader.read(read_step) + buffer
        try:
            # If a newline is detected in the buffer, select and return the last line.
            # Ignore the last element in the buffer search in case it is a newline.
            return buffer[buffer[:-1].rindex(b"\n") + 1 :]
        except ValueError:
            # End up here whenever a newline is not found in the buffer.
            if whole_file:
                return buffer
        # Roll back the head to undo the last read
        file_reader.seek(-read_step, SEEK_CUR)


def _find_last_market_definition_line(file_reader: BufferedReader | BZ2File) -> bytes:
    """
    Finds the last market definition in a file by reading it wholly and iterating
    backwards from the end of the file until the market definition is encountered.

    Raises MarketDefinitionMissingError if the market definition is not found.
    """
    for line in reversed(file_reader.readlines()):
        if MARKET_DEFINITION_BYTES in line:
            return line
    raise MarketDefinitionMissingError(file_reader.name)


def get_market_definition(market_data_file: Path) -> dict:
    """
    Reads a market data file and parses the market definition.
    Accepts both compressed and plaintext files.

    Market id, ordinarily a part of market change ("mc") message but not the market
    definition, is injected into the output data.

    Raises MarketDefinitionMissingError if the market definition is not found.
    """
    decompressor = DECOMPRESSORS.get(market_data_file.suffix, None)
    if decompressor is not None:
        with decompressor(market_data_file) as f:
            line = _find_last_market_definition_line(f)
    else:
        # With plaintext files, try the shortcut of reading the last line first.
        # If that does not locate the market definition, read and search the whole file.
        with open(market_data_file, "rb") as f:
            line = read_last_line_in_a_text_file(f)
            if MARKET_DEFINITION_BYTES not in line:
                f.seek(0, SEEK_SET)  # Move back to the beginning of the file
                line = _find_last_market_definition_line(f)

    # Parse data, inject market ID and return the correct dict sub-class
    market_change_message = json.loads(line)["mc"][0]
    market_definition = market_change_message[MARKET_DEFINITION]
    market_definition["marketId"] = market_change_message["id"]  # Inject market ID
    return market_definition


def create_market_definition_file(
    market_data_file: Path, overwrite: bool = False
) -> Path:
    """
    Creates a market definition file from the market data file and
    stores it in the same directory as <market_id>.json.
    Returns the path to the generated market definition file.

    Processing is skipped altogether if a file with the same name already exists,
    unless overwrite=True is provided.
    """
    output_file = market_data_file.with_suffix(".json")
    if overwrite or not output_file.exists():
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
