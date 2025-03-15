import json
from bz2 import BZ2File
from contextlib import contextmanager
from gzip import GzipFile
from io import BufferedReader
from os import SEEK_CUR, SEEK_END, SEEK_SET
from pathlib import Path
from zipfile import ZipFile

from betfairdatabase.const import ENCODING_UTF_8
from betfairdatabase.exceptions import MarketDefinitionMissingError

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
MARKET_DEFINITION = "marketDefinition"
MARKET_DEFINITION_BYTES = MARKET_DEFINITION.encode()
JSON_SEPARATORS = (",", ":")  # Eliminate unnecessary whitespace
REVERSE_READ_STEP = 64 * 1024  # Characters to start reading a file from reverse


# ---------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# CLASSES
# ---------------------------------------------------------------------------
@contextmanager
def ZipFileWrapper(market_data_file: Path):
    """Context manager to open a ZIP-compressed market data file."""
    with ZipFile(market_data_file, "r") as zf:
        with zf.open(market_data_file.stem) as f:
            yield f


class MarketDefinitionProcessor:
    """
    Extracts market definitions from market data files and stores them in a JSON file.
    """

    # Store supported decompressors in this dict
    DECOMPRESSORS = {".bz2": BZ2File, ".gz": GzipFile, ".zip": ZipFileWrapper}

    def __init__(self, cache_parsed_definitions: bool):
        self.cache_parsed_definitions = cache_parsed_definitions
        self.parsed_definitions = {}

    @staticmethod
    def _find_last_market_definition_line(lines: list[bytes]) -> bytes | None:
        """
        Finds the last market definition in a file by reading it wholly and iterating
        backwards from the end of the file until the market definition is encountered.

        Raises MarketDefinitionMissingError if the market definition is not found.
        """
        for line in reversed(lines):
            if MARKET_DEFINITION_BYTES in line:
                return line
        return None

    def parse_market_definition(self, market_data_file: Path) -> dict:
        """
        Reads a market data file and parses the market definition.
        Accepts both compressed and plaintext files.

        Market id, ordinarily a part of market change ("mc") message but not the market
        definition, is injected into the output data.

        Raises MarketDefinitionMissingError if the market definition is not found.
        """
        line = None
        decompressor = self.DECOMPRESSORS.get(market_data_file.suffix, None)
        if decompressor is not None:
            with decompressor(market_data_file) as f:
                line = self._find_last_market_definition_line(f.readlines())
        else:
            # With plaintext files, try the shortcut of reading the last line first.
            # If that does not locate the market definition, read and search the whole file.
            with open(market_data_file, "rb") as f:
                line = read_last_line_in_a_text_file(f)
                if MARKET_DEFINITION_BYTES not in line:
                    f.seek(0, SEEK_SET)  # Move back to the beginning of the file
                    line = self._find_last_market_definition_line(f.readlines())

        if line is None:
            raise MarketDefinitionMissingError(market_data_file)
        # Parse data, inject market ID and return the correct dict sub-class
        market_change_message = json.loads(line)["mc"][0]
        market_definition = market_change_message[MARKET_DEFINITION]
        market_definition["marketId"] = market_change_message["id"]  # Inject market ID
        return market_definition

    def create_market_definition_file(
        self, market_data_file: Path, overwrite: bool = False
    ) -> Path:
        """
        Creates a market definition file from the market data file and
        stores it in the same directory as <market_id>.json.
        Returns the path to the generated market definition file.

        Processing is skipped altogether if a file with the same name already exists,
        unless overwrite=True is provided.
        """
        # This method will generally not be called with an existing market catalogue
        output_file = market_data_file.with_suffix(".json")
        if overwrite or not output_file.exists():
            metadata = self.parse_market_definition(market_data_file)
            if self.cache_parsed_definitions:
                self.parsed_definitions[output_file] = metadata
            output_file.write_text(
                json.dumps(metadata, separators=JSON_SEPARATORS),
                encoding=ENCODING_UTF_8,
            )
        return output_file
