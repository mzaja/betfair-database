from typing import Any
from zoneinfo import ZoneInfo

from betfairdatabase.utils import parse_datetime


class MarketMetadata(dict):
    """Base class for storing and transforming market metadata."""

    def transform(self) -> dict:
        """
        Transforms itself into a flat dict representation, suitable for SQL table import.

        The result is cached, so subsequent calls involve no data processing. However,
        this also means that invalid results are returned if the object has been modified.
        """
        try:
            return self._transform_result
        except AttributeError:
            self._transform_result = self._transform(self.copy())
            return self._transform_result

    @classmethod
    def _transform(self, data: dict) -> dict:
        """Override this method in a subclass to provide custom implementation."""
        raise NotImplementedError

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
    def _calculate_local_times(
        time_zone: str,
        market_start_time: str | None = None,
        event_open_date: str | None = None,
        settled_time: str | None = None,
    ) -> dict[str, str]:
        """
        Calculates the local times and returns a dict containing the following keys:
            - localDayOfWeek : Local day of the week on which the market started.
            - localMarketStartTime : Local ISO 8601 time when the market started.
            - localEventOpenDate : Local ISO 8601 time when the event opened (optional).
            - localMarketSettledTime : Local ISO 8601 time when the market settled (optional).

        The field is not created if the input required to compute its value is not provided.
        """
        time_zone = ZoneInfo(time_zone)
        data = {}
        if market_start_time:
            market_start_time_local = parse_datetime(market_start_time).astimezone(
                time_zone
            )
            data["localDayOfWeek"] = market_start_time_local.strftime("%A")
            data["localMarketStartTime"] = str(market_start_time_local)
        if event_open_date:
            data["localEventOpenDate"] = str(
                parse_datetime(event_open_date).astimezone(time_zone)
            )
        if settled_time:
            data["localMarketSettledTime"] = str(
                parse_datetime(event_open_date).astimezone(time_zone)
            )
        return data


class MarketCatalogueData(MarketMetadata):
    """Market metadata sourced from a market catalogue."""

    @classmethod
    def _transform(cls, data: dict) -> dict:
        """Transforms market catalogue data into a flat dict representation."""
        # Break out unnecessary parts and those that need further processing
        if description := data.pop("description", None):
            cls._flatten_subdict(description, "priceLadderDescription")
            cls._flatten_subdict(description, "lineRangeInfo")
            data.update(description)

        # Calculate local times if possible
        try:
            data.update(
                cls._calculate_local_times(
                    data["event"]["timezone"],
                    data.get("marketStartTime"),
                    data["event"]["openDate"],
                    # Betfair docu says "settleTime", but this is probably a typo.
                    # In practice however, market catalogues become unavailable
                    # as soon as the market closes, so it is very unlikely to
                    # have this field in the data.
                    description.get("settledTime"),
                )
            )
        except KeyError:
            pass  # "event", and therefore "timezone", are not provided

        if runners := data.pop("runners", None):
            data["runners"] = len(runners)  # Only note down the number of selections

        cls._flatten_subdict(data, "eventType")
        cls._flatten_subdict(data, "competition")
        cls._flatten_subdict(data, "event")

        return data


class MarketDefinitionData(MarketMetadata):
    """
    Market metadata sourced from a market definition.

    Market definitions differ in streams from self-recorded and official sources.
    This class handles both with a single interface.
    """

    # Format is: (original_name, new_name)
    FIELDS_TO_RENAME = (
        ("name", "marketName"),
        ("openDate", "eventOpenDate"),
        ("timezone", "eventTimezone"),
        # Entries below are not always provided
        ("countryCode", "eventCountryCode"),
        ("venue", "eventVenue"),
        ("settledTime", "marketSettledTime"),
    )

    @classmethod
    def _transform(cls, data: dict) -> dict:
        """Transforms market definition data into a flat dict representation."""
        # Calculate local times if possible
        data.update(
            cls._calculate_local_times(
                data["timezone"],
                data["marketTime"],
                data["openDate"],
                data.get("settledTime"),
            )
        )

        # Remove full runner definitions, replace with a number of runners
        data["runners"] = len(data.pop("runners"))

        # priceLadderDefinition is available in self-recorded data only
        if price_ladder_definition := data.pop("priceLadderDefinition", None):
            data["priceLadderDescriptionType"] = price_ladder_definition["type"]

        # Rename fields to match market catalogue
        for old_name, new_name in cls.FIELDS_TO_RENAME:
            data[new_name] = data.pop(old_name, None)

        return data
