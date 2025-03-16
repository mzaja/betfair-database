import json
from bz2 import BZ2File
from contextlib import contextmanager
from gzip import GzipFile
from os import SEEK_SET
from pathlib import Path
from zipfile import ZipFile

from betfairdatabase.const import ENCODING_UTF_8
from betfairdatabase.exceptions import MarketDefinitionMissingError
from betfairdatabase.utils import read_last_line_in_a_file

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
MARKET_DEFINITION = "marketDefinition"
MARKET_DEFINITION_BYTES = MARKET_DEFINITION.encode()
JSON_SEPARATORS = (",", ":")  # Eliminate unnecessary whitespace


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
                line = read_last_line_in_a_file(f)
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
        output_file = (
            market_data_file.with_suffix(".json")
            if len(market_data_file.suffixes) == 2
            else (market_data_file.with_suffix(market_data_file.suffix + ".json"))
        )
        if overwrite or not output_file.exists():
            metadata = self.parse_market_definition(market_data_file)
            if self.cache_parsed_definitions:
                self.parsed_definitions[output_file] = metadata
            output_file.write_text(
                json.dumps(metadata, separators=JSON_SEPARATORS),
                encoding=ENCODING_UTF_8,
            )
        return output_file
