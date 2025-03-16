import unittest
from pathlib import Path
from unittest import mock

from betfairdatabase.market import Market, MarketCatalogueData, MarketDefinitionData
from betfairdatabase.metadata import MarketMetadata
from betfairdatabase.racing import (
    METERS_PER_FURLONG,
    RacingDataProcessor,
    extract_race_metadata,
)

TEST_DATA_DIR = Path("./tests/data/datasets")
WIN_MARKET_CATALOGUE = TEST_DATA_DIR / "zip-lzma/1.197931750.json"
WIN_MARKET_DATA = WIN_MARKET_CATALOGUE.with_suffix(".zip")
PLACE_MARKET_CATALOGUE = TEST_DATA_DIR / "zip-lzma/1.197931751.json"
PLACE_MARKET_DATA = PLACE_MARKET_CATALOGUE.with_suffix(".zip")
NON_RACING_MARKET_CATALOGUE = TEST_DATA_DIR / "uncompressed/1.216418252.json"
NON_RACING_MARKET_DATA = NON_RACING_MARKET_CATALOGUE.with_suffix("")

# WIN market names for extracing race metadata
# UK, IRE, USA events
MARKET_NAMES_1 = [
    "2m Mdn Hrd",
    "3m1f Beg Chs",
    "6f Mdn",
    "3m Grd3 Nov Chs",
    "R1 7f Claim",
    "R3 1m Stks",
    "PA Hcap",
]
DIST_F_1 = [16, 25, 6, 24, 7, 8, None]
DIST_M_1 = [(x * METERS_PER_FURLONG if x else None) for x in DIST_F_1]
RACE_TYPES_1 = ["Mdn Hrd", "Beg Chs", "Mdn", "Grd3 Nov Chs", "Claim", "Stks", "PA Hcap"]

# AUS, RSA, GH events
MARKET_NAMES_2 = [
    "R4 405m Gr3/4",
    "A2 462m",
    "D2 275m",
    "OR 500m",
    "R10 405m Gr5",
    "R2 1200m Plt",
    "R5 2185m Pace M",
    "415m",
]
DIST_M_2 = [405, 462, 275, 500, 405, 1200, 2185, 415]
DIST_F_2 = [x / METERS_PER_FURLONG for x in DIST_M_2]
RACE_TYPES_2 = ["Gr3/4", "A2", "D2", "OR", "Gr5", "Plt", "Pace M", None]

MARKET_NAMES = MARKET_NAMES_1 + MARKET_NAMES_2
DIST_METERS = DIST_M_1 + DIST_M_2
DIST_FURLONGS = DIST_F_1 + DIST_F_2
RACE_TYPES = RACE_TYPES_1 + RACE_TYPES_2


class TestRacing(unittest.TestCase):
    """
    Tests racing-specific utilities.
    """

    def test_extract_race_metadata(self):
        """Extract race metadata from WIN market name string."""
        for market_name, meters, furlongs, race_type in zip(
            MARKET_NAMES, DIST_METERS, DIST_FURLONGS, RACE_TYPES
        ):
            with self.subTest(
                market_name=market_name,
                meters=meters,
                furlongs=furlongs,
                race_type=race_type,
            ):
                data = extract_race_metadata(market_name)
                self.assertEqual(data["raceTypeFromName"], race_type)
                self.assertAlmostEqual(data["raceDistanceMeters"], meters, 3)
                self.assertAlmostEqual(data["raceDistanceFurlongs"], furlongs, 3)

    def test_make_race_id(self):
        """
        Tests that race id contains all the required ingredients to make
        it a unique identifier.
        """
        event_type_id = "4339"
        county_code = "GB"
        venue = "Newcastle"
        market_start_time = "2022-04-19T17:19:00.000Z"
        market_catalogue_data = MarketCatalogueData(
            {
                "eventType": {"id": event_type_id},
                "event": {"countryCode": county_code, "venue": venue},
                "marketStartTime": market_start_time,
            }
        )
        market_definition_data = MarketDefinitionData(
            {
                "eventTypeId": event_type_id,
                "countryCode": county_code,
                "venue": venue,
                "marketTime": market_start_time,
            }
        )
        for metadata in [market_catalogue_data, market_definition_data]:
            race_id = RacingDataProcessor.make_race_id(metadata)
            self.assertIn(event_type_id, race_id)
            self.assertIn(county_code, race_id)
            self.assertIn(venue, race_id)
            self.assertIn(market_start_time, race_id)

    def test_make_race_id_raises_type_error(self):
        """
        TypeError is raised unless the input parameter is an instance of
        MarketCatalogueData or MarketDefinitionData. Plain dict or a subclass does not work.
        """
        for arg in [{}, MarketMetadata()]:
            with self.assertRaises(TypeError):
                RacingDataProcessor.make_race_id(arg)

    def test_racing_data_processor(self):
        """Tests the racing data processor."""
        win_market = Market(WIN_MARKET_CATALOGUE, WIN_MARKET_DATA)
        place_market = Market(PLACE_MARKET_CATALOGUE, PLACE_MARKET_DATA)
        non_racing_market = Market(NON_RACING_MARKET_CATALOGUE, NON_RACING_MARKET_DATA)
        proc = RacingDataProcessor()
        for market in (win_market, place_market, non_racing_market):
            proc.add(market)  # No exception should be raised
        metadata = proc.get(win_market)
        self.assertEqual(len(metadata), 4)  # Not empty
        self.assertEqual(proc.get(place_market), metadata)  # Markets are linked
        self.assertIsNotNone(metadata["raceId"])
        self.assertEqual(metadata["raceDistanceMeters"], 280)
        self.assertAlmostEqual(metadata["raceDistanceFurlongs"], 1.391871, 3)
        self.assertEqual(metadata["raceTypeFromName"], "OR")
        self.assertIsNone(proc.get(non_racing_market))  # No exception raised

    def test_racing_data_processor_incomplete_market_metadata(self):
        """Incomplete or defective market catalogue must not raise an exception."""
        for metadata in [MarketCatalogueData(), MarketDefinitionData()]:
            with self.subTest(metadata_type=type(metadata)):
                mock_market = mock.Mock(
                    # Paths are irrelevant here since metadata is mocked
                    spec_set=Market(WIN_MARKET_CATALOGUE, WIN_MARKET_DATA)
                )
                mock_market.metadata = metadata
                mock_market.racing = True
                proc = RacingDataProcessor()
                # Test passes if no exceptions are raised
                proc.add(mock_market)
                proc.get(mock_market)
