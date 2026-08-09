import json
from bz2 import BZ2File
from contextlib import contextmanager
from gzip import GzipFile
from os import SEEK_SET
from pathlib import Path
from zipfile import ZipFile

from betfairdatabase.const import MARKET_ID
from betfairdatabase.exceptions import MarketDefinitionMissingError
from betfairdatabase.utils import read_last_line_in_a_file, write_to_json

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
MARKET_DEFINITION = "marketDefinition"
MARKET_DEFINITION_BYTES = MARKET_DEFINITION.encode()


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
    Extracts market definitions from market data files.
    """

    # Store supported decompressors in this dict
    DECOMPRESSORS = {".bz2": BZ2File, ".gz": GzipFile, ".zip": ZipFileWrapper}

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

    @classmethod
    def parse_market_definition(cls, market_data_file: Path) -> dict:
        """
        Reads a market data file and parses the market definition.
        Accepts both compressed and plaintext files.

        Market id, ordinarily a part of market change ("mc") message but not the market
        definition, is injected into the output data.

        Raises MarketDefinitionMissingError if the market definition is not found.
        """
        line = None
        decompressor = cls.DECOMPRESSORS.get(market_data_file.suffix, None)
        if decompressor is not None:
            with decompressor(market_data_file) as f:
                line = cls._find_last_market_definition_line(f.readlines())
        else:
            # With plaintext files, try the shortcut of reading the last line first.
            # If that does not locate the market definition, read and search the whole file.
            with open(market_data_file, "rb") as f:
                line = read_last_line_in_a_file(f)
                if MARKET_DEFINITION_BYTES not in line:
                    f.seek(0, SEEK_SET)  # Move back to the beginning of the file
                    line = cls._find_last_market_definition_line(f.readlines())

        if line is None:
            raise MarketDefinitionMissingError(market_data_file)
        # Parse data, inject market ID and return the correct dict sub-class
        market_change_message = json.loads(line)["mc"][0]
        market_definition = market_change_message[MARKET_DEFINITION]
        market_definition[MARKET_ID] = market_change_message["id"]  # Inject market ID
        return market_definition
