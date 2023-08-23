import json
import shutil
from pathlib import Path
from typing import Any
from functools import cache
import copy as cp

from betfairdatabase.const import DATA_FILE_SUFFIXES, SQL_TABLE_COLUMNS
from betfairdatabase.exceptions import MarketDataFileError


class Market:
    """
    Holds the information about market catalogue and market data files.

    Improves performance by caching results of slow I/O or CPU-intensive operations.
    """

    def __init__(self, market_catalogue_file: str | Path):
        self.market_catalogue_file = Path(market_catalogue_file).resolve()

    @property
    def market_data_file(self) -> Path:
        """
        Returns the path to the market data file for this market. Raises
        MarketDataFileError if the file does not exist.

        Market data file is expected to be next to the market catalogue file
        and share the same basename.
        """
        try:
            return self._market_data_file
        except AttributeError:
            for suffix in DATA_FILE_SUFFIXES:
                data_file = self.market_catalogue_file.with_suffix(suffix)
                if data_file.exists():
                    self._market_data_file = data_file.resolve()
                    return self._market_data_file
            raise MarketDataFileError(
                f"Market data file is missing for market catalogue '{self.market_catalogue_file}'."
            )

    @property
    def market_catalogue_data(self) -> dict:
        """Parsed market catalogue data."""
        try:
            return self._market_catalogue_data
        except AttributeError:
            with open(self.market_catalogue_file, encoding="utf-8") as f:
                self._market_catalogue_data = json.load(f)
            return self._market_catalogue_data

    def create_sql_mapping(self) -> dict[str, Any]:
        """
        Returns a dictionary where keys are SQL table column names and
        values are values in a row.
        """
        sql_data_map = self._transform_market_catalogue()
        sql_data_map["marketCatalogueFilePath"] = self._str_or_none(
            self.market_catalogue_file
        )
        sql_data_map["marketDataFilePath"] = self._str_or_none(self.market_data_file)
        return sql_data_map

    def copy(self, dest_dir: str | Path) -> "Market":
        """
        Copies the market catalogue and market data file to the destination
        directory, returning a new Market wrapper around them.

        Caches are preserved with this operation.
        If the destination file already exists, raises FileExistsError.
        """
        overwrite = False
        return self._change_location(dest_dir, True, overwrite)

    def move(self, dest_dir: str | Path) -> "Market":
        """
        Moves the market catalogue and market data file to the destination
        directory, modifying this object in place and returning a reference to it.

        Caches are preserved with this operation.
        If the destination file already exists, raises FileExistsError.
        """
        overwrite = False
        return self._change_location(dest_dir, False, overwrite)

    ################# PRIVATE METHODS #######################

    @staticmethod
    def _flatten_subdict(parent_dict: dict[str, Any], child_key: str) -> None:
        """
        Flattens a dictionary by combining parent and child's key names.
        Modifies the dictionary in place.
        """
        if subdict := parent_dict.pop(child_key, None):
            for subkey, value in subdict.items():
                # Preserve camel case in the combined key
                combined_key = child_key + subkey[0].upper() + subkey[1:]
                parent_dict[combined_key] = value

    @cache
    def _transform_market_catalogue(self) -> dict:
        """
        Transforms parsed market catalogue data into a flat dict
        representation suitable for SQL table import.
        """
        # Break out unnecessary parts and those that need further processing
        data = self.market_catalogue_data.copy()

        if description := data.pop("description", None):
            self._flatten_subdict(description, "priceLadderDescription")
            self._flatten_subdict(description, "lineRangeInfo")
            data.update(description)

        if runners := data.pop("runners", None):
            data["runners"] = len(runners)  # Only note down the number of selections

        self._flatten_subdict(data, "eventType")
        self._flatten_subdict(data, "competition")
        self._flatten_subdict(data, "event")

        # All keys not in SQL_TABLE_COLUMNS are dropped
        return {k: data.get(k, None) for k in SQL_TABLE_COLUMNS}

    @staticmethod
    def _str_or_none(obj) -> str | None:
        """Returns None if the obj is None, else its string representation."""
        return None if obj is None else str(obj)

    def _change_location(
        self, dest_dir: str | Path, copy: bool, overwrite: bool
    ) -> "Market":
        """
        Returns a new Market object where the paths to market catalogue and
        market data files have been updated as if the .
        """
        dest_dir = Path(dest_dir).resolve()
        market_catalogue_dest_file = dest_dir / self.market_catalogue_file.name
        market_data_dest_file = dest_dir / self.market_data_file.name
        if (overwrite is False) and market_catalogue_dest_file.exists():
            raise FileExistsError(
                f"Market catalogue file already exists at '{market_catalogue_dest_file}'."
            )
        elif (overwrite is False) and market_data_dest_file.exists():
            raise FileExistsError(
                f"Market data file already exists at '{market_data_dest_file}'."
            )
        else:
            # Copy or move the files to the destination
            if copy:
                file_operation = shutil.copy
                market = cp.copy(self)  # Create a copy of itself to modify
            else:
                file_operation = shutil.move
                market = self  # Modify itself in-place
            market_catalogue_dest_file.mkdir(exist_ok=True, parents=True)
            file_operation(self.market_catalogue_file, market_catalogue_dest_file)
            file_operation(self.market_data_file, market_data_dest_file)
            market.market_catalogue_file = market_catalogue_dest_file
            market._market_data_file = market_data_dest_file
            return market
