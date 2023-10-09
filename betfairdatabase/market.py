from __future__ import annotations

import copy as cp
import json
import shutil
from functools import cache, cached_property
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from betfairdatabase.const import (
    DATA_FILE_SUFFIXES,
    SQL_TABLE_COLUMNS,
    DuplicatePolicy,
    SQLAction,
)
from betfairdatabase.exceptions import MarketDataFileError
from betfairdatabase.utils import parse_datetime

RACING_EVENT_TYPE_IDS = (
    "7",  # Horse racing
    "4339",  # Greyhound racing
)


class Market:
    """
    Holds the information about market catalogue and market data files.

    Improves performance by caching results of slow I/O or CPU-intensive operations.
    """

    def __init__(self, market_catalogue_file: str | Path):
        self.market_catalogue_file = Path(market_catalogue_file).resolve()
        self.sql_action = SQLAction.INSERT

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

    @cached_property
    def market_catalogue_data(self) -> dict:
        """Parsed market catalogue data."""
        return self._parse_json_file(self.market_catalogue_file)

    @cached_property
    def racing(self) -> bool:
        """
        Returns True if the market is a racing one, False if it isn't or it
        cannot be determined.
        """
        try:
            return (
                self.market_catalogue_data["eventType"]["id"] in RACING_EVENT_TYPE_IDS
            )
        except KeyError:
            return False

    def create_sql_mapping(
        self, additional_metadata: dict | None = None, no_paths: bool = False
    ) -> dict[str, Any]:
        """
        Returns a dictionary where keys are SQL table column names and
        values are values in a row.

        If no_paths is True, marketCatalogueFilePath and marketDataFilePath
        field values are set to None.
        """
        # Call below is cached, so it must be a separate method
        data = self._transform_market_catalogue()

        # Insert additional metadata if any is provided
        if additional_metadata:
            data.update(additional_metadata)

        # Insert file location info
        if not no_paths:
            data["marketCatalogueFilePath"] = self._str_or_none(
                self.market_catalogue_file
            )
            data["marketDataFilePath"] = self._str_or_none(self.market_data_file)

        # All keys not in SQL_TABLE_COLUMNS are dropped
        return {k: data.get(k, None) for k in SQL_TABLE_COLUMNS}

    def copy(self, dest_dir: str | Path, on_duplicates: DuplicatePolicy) -> Market:
        """
        Copies the market catalogue and market data file to the destination
        directory, returning a new Market wrapper around them.

        Caches are preserved with this operation.
        If the destination file already exists, raises FileExistsError.
        """
        return self._change_location(dest_dir, True, on_duplicates)

    def move(self, dest_dir: str | Path, on_duplicates: DuplicatePolicy) -> Market:
        """
        Moves the market catalogue and market data file to the destination
        directory, modifying this object in place and returning a reference to it.

        Caches are preserved with this operation.
        If the destination file already exists, raises FileExistsError.
        """
        return self._change_location(dest_dir, False, on_duplicates)

    ################# PRIVATE METHODS #######################

    @staticmethod
    def _parse_json_file(file: Path) -> dict:
        """Parses a JSON file and returns it as a dict."""
        with open(file, encoding="utf-8") as f:
            return json.load(f)

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

    @staticmethod
    def _str_or_none(obj) -> str | None:
        """Returns None if the obj is None, else its string representation."""
        return None if obj is None else str(obj)

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

        # Calculate local times if possible
        try:
            time_zone = ZoneInfo(self.market_catalogue_data["event"]["timezone"])
            market_start_time_local = parse_datetime(
                self.market_catalogue_data["marketStartTime"]
            ).astimezone(time_zone)
            event_open_date_local = parse_datetime(
                self.market_catalogue_data["event"]["openDate"]
            ).astimezone(time_zone)
            data["localDayOfWeek"] = market_start_time_local.strftime("%A")
            data["localMarketStartTime"] = str(market_start_time_local)
            data["localEventOpenDate"] = str(event_open_date_local)
        except KeyError:
            pass  # "event", and therefore "timezone", are not provided

        return data

    def _change_location(
        self, dest_dir: str | Path, copy: bool, on_duplicates: DuplicatePolicy
    ) -> Market:
        """
        Returns a new Market object where the paths to market catalogue and
        market data files have been updated as if the .
        """
        # Determine output dir and destination file paths
        dest_dir = Path(dest_dir).resolve()

        # Process market catalogue?
        market_catalogue_dest_file = dest_dir / self.market_catalogue_file.name
        if market_catalogue_dest_file.exists():
            if on_duplicates is DuplicatePolicy.REPLACE:
                # With this policy we replace the file no matter what
                self.sql_action = SQLAction.UPDATE
            elif (on_duplicates is DuplicatePolicy.SKIP) or (
                # Only the difference in data that goes into the SQL table matters
                self.create_sql_mapping(no_paths=True)
                == Market(market_catalogue_dest_file).create_sql_mapping(no_paths=True)
            ):
                # Policy is SKIP or market catalogue data has not been modified
                self.sql_action = SQLAction.SKIP
            else:  # Policy is UPDATE and market catalogue data has been modified
                self.sql_action = SQLAction.UPDATE
        process_market_catalogue = self.sql_action is not SQLAction.SKIP

        # Process market data file?
        market_data_dest_file = dest_dir / self.market_data_file.name
        process_market_data_file = True
        if market_data_dest_file.exists():
            if (on_duplicates is DuplicatePolicy.SKIP) or (
                (on_duplicates is DuplicatePolicy.UPDATE)
                and (
                    market_data_dest_file.stat().st_size  # Existing file size
                    >= self.market_data_file.stat().st_size  # Incoming file size
                )
            ):
                process_market_data_file = False

        # Copy or move the files to the destination if required
        if copy:
            file_operation = shutil.copy
            market = cp.copy(self)  # Create a copy of itself to modify
        else:
            file_operation = shutil.move
            market = self  # Modify itself in-place

        dest_dir.mkdir(exist_ok=True, parents=True)
        if process_market_catalogue:
            file_operation(self.market_catalogue_file, market_catalogue_dest_file)
        if process_market_data_file:
            file_operation(self.market_data_file, market_data_dest_file)

        # Always change paths to the database dir, regardless if the files were moved or not
        market.market_catalogue_file = market_catalogue_dest_file
        market._market_data_file = market_data_dest_file
        return market
