from __future__ import annotations

import copy as cp
import json
import shutil
from functools import cache, cached_property
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from betfairdatabase.const import (
    ENCODING_UTF_8,
    MARKET_DATA_FILE_PATH,
    MARKET_METADATA_FILE_PATH,
    SQL_TABLE_COLUMNS,
    DuplicatePolicy,
    SQLAction,
)
from betfairdatabase.utils import parse_datetime

RACING_EVENT_TYPE_IDS = (
    "7",  # Horse racing
    "4339",  # Greyhound racing
)


class MarketCatalogueData(dict):
    """Market metadata sourced from a market catalogue."""


class MarketDefinitionData(dict):
    """Market metadata sourced from a market definition."""


class Market:
    """
    Holds information about the market metadata and the market data files.

    Improves performance by caching results of slow I/O or CPU-intensive operations.

    Attributes:
        - market_metadata_file : Path to the file describing the contents of the market data file.
                                 Can be either a market catalogue file or a market definition file.
        - market_data_file : Path to the market data file for this market.
        - sql_action : Tells the database what to do with the market when procesing it.
                       It gets modified when the market is moved or copied, depending on
                       the duplicate handling policy. Default value is INSERT.
    Properties:
        - metadata : Returns the parsed market metadata data as a dict.
        - racing : True if this is a horse or a greyhound racing market, else False.

    Methods:
        - create_sql_mapping : Transforms the parsed market metadata to a flat
                               dictionary representation suitable for SQL import.
        - copy : Copies market metadata and data files to the destination. Updates paths.
        - move : Moves market metadata and data files to the destination. Updates paths.
    """

    def __init__(self, market_metadata_file: Path, market_data_file: Path):
        self.market_metadata_file = market_metadata_file.resolve()
        self.market_data_file = market_data_file.resolve()
        self.sql_action = SQLAction.INSERT

    @cached_property
    def metadata(self) -> MarketCatalogueData | MarketDefinitionData:
        """Returns parsed market metadata, with the data source indicated by the return type."""
        metadata = json.loads(
            self.market_metadata_file.read_text(encoding=ENCODING_UTF_8),
        )
        if "name" in metadata:  # Called "marketName" in a market catalogue
            return MarketDefinitionData(metadata)
        return MarketCatalogueData(metadata)

    @cached_property
    def racing(self) -> bool:
        """
        Returns True if the market is a racing one, False if it isn't or it
        cannot be determined.
        """
        data = self.metadata
        try:
            event_type_id = (
                data["eventType"]["id"]
                if isinstance(data, MarketCatalogueData)
                else data["eventTypeId"]
            )
            return event_type_id in RACING_EVENT_TYPE_IDS
        except KeyError:
            return False

    def create_sql_mapping(
        self, additional_metadata: dict | None = None, no_paths: bool = False
    ) -> dict[str, Any]:
        """
        Returns a dictionary where keys are SQL table column names and
        values are values in a row. Extra key-value pairs can be added
        through additional_metadata argument.

        If no_paths is True, marketMetadataFilePath and marketDataFilePath
        fields are set to None.
        """
        # Call below is cached, so it must be a separate method
        data = self._transform_market_metadata()

        # Insert additional metadata if any is provided
        if additional_metadata:
            data.update(additional_metadata)

        # Insert file location info
        if not no_paths:
            data[MARKET_METADATA_FILE_PATH] = self._str_or_none(
                self.market_metadata_file
            )
            data[MARKET_DATA_FILE_PATH] = self._str_or_none(self.market_data_file)

        # All keys not in SQL_TABLE_COLUMNS are dropped
        return {k: data.get(k, None) for k in SQL_TABLE_COLUMNS}

    def copy(self, dest_dir: str | Path, on_duplicates: DuplicatePolicy) -> Market:
        """
        Copies the market metadata and market data file to the destination
        directory, returning a new Market wrapper around them.

        Replaces the destination file if it already exists.
        Caches are preserved with this operation.
        """
        return self._change_location(dest_dir, True, on_duplicates)

    def move(self, dest_dir: str | Path, on_duplicates: DuplicatePolicy) -> Market:
        """
        Moves the market metadata and market data file to the destination
        directory, modifying this object in place and returning a reference to it.

        Replaces the destination file if it already exists.
        Caches are preserved with this operation.
        """
        return self._change_location(dest_dir, False, on_duplicates)

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

    @staticmethod
    def _str_or_none(obj) -> str | None:
        """Returns None if the obj is None, else its string representation."""
        return None if obj is None else str(obj)

    @cache
    def _transform_market_metadata(self) -> dict:
        """
        Transforms parsed market metadata into a flat dict
        representation suitable for SQL table import.
        """
        # TODO: Handle market definition metadata
        data = self.metadata.copy()  # Copy so the original is not modified

        # Break out unnecessary parts and those that need further processing
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
            time_zone = ZoneInfo(self.metadata["event"]["timezone"])
            market_start_time_local = parse_datetime(
                self.metadata["marketStartTime"]
            ).astimezone(time_zone)
            event_open_date_local = parse_datetime(
                self.metadata["event"]["openDate"]
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
        Moves the market metadata and the market data file to the specified directory.
        Returns a new Market object with the updated paths to market files. The paths
        are updated regardless of whether the files were actually moved or copied.
        """
        # Determine output dir and destination file paths
        dest_dir = Path(dest_dir).resolve()

        # Process the market metadata file?
        market_metadata_dest_file = dest_dir / self.market_metadata_file.name
        if market_metadata_dest_file.exists():
            if on_duplicates is DuplicatePolicy.REPLACE:
                # With this policy we replace the file no matter what
                self.sql_action = SQLAction.UPDATE
            elif (on_duplicates is DuplicatePolicy.SKIP) or (
                self.create_sql_mapping(no_paths=True)
                # Market data file is irrelevant here, so use a dummy path
                == Market(market_metadata_dest_file, Path(".")).create_sql_mapping(
                    no_paths=True
                )
            ):
                # Policy is SKIP,
                # or policy is UPDATE the market metadata file has not been modified
                self.sql_action = SQLAction.SKIP
            else:
                # Policy is UPDATE and the market metadata file has been modified
                self.sql_action = SQLAction.UPDATE
        process_market_metadata_file = self.sql_action is not SQLAction.SKIP

        # Process the market data file?
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

        # Copy or move the files to the destination if required.
        # Both shutil.copy and shutil.move replace the destination file if it exists.
        if copy:
            file_operation = shutil.copy
            market = cp.copy(self)  # Create a copy of itself to modify
        else:
            file_operation = shutil.move
            market = self  # Modify itself in-place

        dest_dir.mkdir(exist_ok=True, parents=True)
        if process_market_metadata_file:
            file_operation(self.market_metadata_file, market_metadata_dest_file)
        if process_market_data_file:
            file_operation(self.market_data_file, market_data_dest_file)

        # Always change paths to the destination dir, regardless if the files were moved or not
        market.market_metadata_file = market_metadata_dest_file
        market.market_data_file = market_data_dest_file
        return market
