import datetime as dt
from io import BufferedReader
from os import SEEK_CUR, SEEK_END, SEEK_SET
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
REVERSE_READ_STEP = 64 * 1024  # Characters to start reading a file from reverse


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


def read_last_line_in_a_file(file_reader: BufferedReader) -> bytes:
    """
    Reads the last line in a (text) file by jumping to the end of the file and
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
