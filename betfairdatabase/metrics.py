import logging
from dataclasses import dataclass
from typing import Literal

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLASSES
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class Counters:
    """
    Stores counters for various operations of the market file processor.
    Used to display import statistics.
    """

    total_markets: int = 0
    markets_without_data: int = 0
    markets_without_metadata: int = 0
    corrupt_files: int = 0
    rows_inserted: int = 0
    markets_updated: int = 0
    markets_skipped: int = 0

    @property
    def markets_added(self) -> int:
        """Returns the number of added markets."""
        return self.rows_inserted - self.markets_updated

    def log_info(self, action: Literal["indexing", "importing"]) -> None:
        """Logs the counters as INFO messages."""
        logger.info("Finished %s %d markets.", action, self.total_markets)
        logger.info("Added: %d", self.markets_added)
        if action == "importing":
            logger.info("Updated: %d", self.markets_updated)
            logger.info("Skipped: %d", self.markets_skipped)
        logger.info("Corrupt: %d", self.corrupt_files)
        logger.info("No data: %d", self.markets_without_data)
        logger.info("No metadata: %d", self.markets_without_metadata)
        if not self.validate():
            logger.error("Counters do not add up.")

    def validate(self) -> bool:
        """
        Performs a sanity check on the contents to ensure that the sum of components
        matches the total. Returns True if the checks passes, else False.
        """
        return self.total_markets == (
            self.rows_inserted
            + self.markets_skipped
            + self.corrupt_files
            + self.markets_without_data
            + self.markets_without_metadata
        )
