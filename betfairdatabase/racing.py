import re

from betfairdatabase.market import Market

# ---------------------------------------------------------------------------
# CONST
# ---------------------------------------------------------------------------
METERS_PER_FURLONG = 201.168
FURLONGS_PER_MILE = 8
WIN = "WIN"

# ---------------------------------------------------------------------------
# REGEX
# ---------------------------------------------------------------------------
# extracts the race distance from the EVENT_NAME field
RACE_DIST_REGEX = re.compile(r"(?:(\d*)[Mm])?(?:(\d*)f)?")
# extracts the race type from the EVENT_NAME field, assuming race distance has been removed from the string
RACE_TYPE_REGEX = re.compile(r"(?:R\d+)?(?:\s+)?(.*\S)")


# ---------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------
def extract_race_metadata(market_name: str) -> dict:
    """
    Extracts the race distance and type from market name, if possible.

    Returns a dict with the following key-value pairs:
        - "raceTypeFromName": Race type e.g. 'Hcap', 'Mdn', 'Stks' etc.
        - "raceDistanceMeters": Race distance in meters.
        - "raceDistanceFurlongs": Race distance in furlongs.
    """
    # extract race distance
    str_dist_tuple = next(
        (x for x in RACE_DIST_REGEX.findall(market_name) if x != ("", "")), None
    )
    if str_dist_tuple:
        try:
            m_value = float(str_dist_tuple[0])  # if 'm' factor exists
        except ValueError:  # trying to convert '' to a float raises a ValueError
            m_value = 0
        try:
            f_value = float(str_dist_tuple[1])  # if 'f' factor exists
        except ValueError:
            f_value = 0

        if f_value or (m_value < 20):
            # If f_value is present or m_value is low, units are miles & furlongs
            furlongs = (m_value * FURLONGS_PER_MILE) + f_value
            meters = furlongs * METERS_PER_FURLONG
        else:
            # For high m_value, base unit is meter
            meters = m_value
            furlongs = m_value / METERS_PER_FURLONG

        # remove distance from the original string for easier processing
        if m_value > 0:
            for unit in ("m", "M"):
                market_name = market_name.replace(f"{str_dist_tuple[0]}{unit}", "")
        if f_value > 0:
            market_name = market_name.replace(f"{str_dist_tuple[1]}f", "")
    else:
        meters = furlongs = None

    race_type_match = RACE_TYPE_REGEX.search(market_name)
    race_type = race_type_match.group(1) if race_type_match else None

    return {
        "raceTypeFromName": race_type,
        "raceDistanceMeters": meters,
        "raceDistanceFurlongs": furlongs,
    }


# ---------------------------------------------------------------------------
# CLASSES
# ---------------------------------------------------------------------------
class RacingDataProcessor:
    """
    Obtains and retrieves additional metadata for racing markets.
    """

    def __init__(self):
        self._race_metadata_lookup = {}

    @staticmethod
    def make_race_id(market_catalogue_data: dict) -> str:
        """Creates an unambiguous lookup for individual races."""
        return ",".join(
            (
                market_catalogue_data["eventType"]["id"],
                market_catalogue_data["event"]["countryCode"],
                market_catalogue_data["event"]["venue"],
                market_catalogue_data["marketStartTime"],
            )
        )

    def add(self, market: Market):
        """Adds and processes market catalogue data."""
        if market.racing:
            try:
                market_catalogue_data = market.market_catalogue_data
                if market_catalogue_data["description"]["marketType"] == WIN:
                    self._race_metadata_lookup[
                        self.make_race_id(market_catalogue_data)
                    ] = extract_race_metadata(market_catalogue_data["marketName"])
            except KeyError:
                # Incomplete or unsuitable market catalogue
                return

    def get(self, market: Market) -> dict | None:
        """
        Retrieves the racing metadata for the provided market catalogue.

        If metadata cannot be retrieved, returns None.
        """
        if market.racing:
            try:
                metadata = self._race_metadata_lookup[
                    race_id := self.make_race_id(market.market_catalogue_data)
                ]
                metadata["raceId"] = race_id
                return metadata
            except KeyError:
                # Unsuitable market catalogue
                # Also raised by make_race_id() if race_id cannot be constructed
                pass
        return None
