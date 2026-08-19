import logging
import sqlite3
from pathlib import Path
from typing import Callable, TypeVar

from betfairdatabase.const import (
    MARKET_METADATA_FILE_PATH,
    SQL_TABLE_NAME,
    DuplicatePolicy,
    SQLAction,
)
from betfairdatabase.market import Market
from betfairdatabase.metrics import Counters
from betfairdatabase.racing import RacingDataProcessor
from betfairdatabase.tree import DirectoryParser
from betfairdatabase.utils import (
    ProgressBarMixin,
    is_debug_logging_enabled,
)

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MAIN CLASS
# ---------------------------------------------------------------------------
class MarketFileProcessor(ProgressBarMixin):
    """
    Processes market data and metadata files (market catalogues and market definitions).
    Two public methods are provided:
        - index_database(): Indexes a directory containing market data and metadata files.
        - update_database(): Inserts files from the source directory into the existing database.

    This class is a refactor of the now-removed BetfairDatabase._process_market_data() method,
    which had grown too big and complex to maintain. The class splits the code sections
    of that method into smaller, more manageable methods.
    """

    INVALID = TypeVar("INVALID")  # Sentinel for invalid metadata.json contents

    def __init__(
        self,
        database_dir: str | Path,
        progress_bar: bool = True,
    ):
        super().__init__(progress_bar)
        self.database_dir = Path(database_dir)
        self.debug_logging_enabled = is_debug_logging_enabled(logger)

    def index_database(
        self, connection: sqlite3.Connection, merge_metadata: bool = False
    ) -> int:
        """
        Indexes the database by processing market data and metadata files.
        Returns the number of indexed markets.
        """
        return self._main(self.database_dir, connection, merge_metadata)

    def update_database(
        self,
        source_dir: str | Path,
        connection: sqlite3.Connection,
        copy: bool = False,
        import_pattern: Callable[[dict], str] | None = None,
        on_duplicates: DuplicatePolicy | None = None,
    ) -> int:
        """
        Inserts the market data and metadata files from source_dir into the existing database.
        Returns the number of inserted markets.
        """
        merge_metadata = False  # Temporarily disabled while WIP
        return self._main(
            Path(source_dir),
            connection,
            merge_metadata,
            copy,
            import_pattern,
            on_duplicates,
        )

    def _main(
        self,
        source_dir: Path,
        connection: sqlite3.Connection,
        merge_metadata: bool,
        *args,
    ) -> int:
        """
        Main routine of this class.
        Called by the public methods `index_database` and `update_database`.
        """
        counters = Counters()

        # Locate files for processing
        database_dirs = DirectoryParser(self.progress_bar_enabled).parse_directory_tree(
            source_dir
        )

        importable_markets = []
        for dd in self._progress_bar(database_dirs, "Parsing metadata", "directories"):
            importable_markets += dd.process(counters, merge_metadata)

        # Import markets into the database, optionally moving or copying files if needed
        self._import_markets_into_database(
            importable_markets, connection, counters, *args
        )
        action = "importing" if args else "indexing"
        counters.log_info(action)
        return counters.rows_inserted

    def _import_markets_into_database(
        self,
        importable_markets: list[Market],
        connection: sqlite3.Connection,
        counters: Counters,
        copy: bool = False,
        import_pattern: Callable[[Market], str] | None = None,
        on_duplicates: DuplicatePolicy | None = None,
    ) -> None:
        """
        Imports the markets with valid metadata into the database.
        Returns the number of inserted SQL table rows.
        """
        racing_data_processor = RacingDataProcessor(
            importable_markets, self.progress_bar_enabled
        )
        update_existing_database = bool(import_pattern and on_duplicates)

        for market in self._progress_bar(importable_markets, "Importing markets"):
            # Database is being updated
            if update_existing_database:
                dest_dir = self.database_dir / import_pattern(market)
                # Move and copy are conditional on the duplicate handling policy
                # and set market.sql_action accordingly
                market = (
                    market.copy(dest_dir, on_duplicates)
                    if copy
                    else market.move(dest_dir, on_duplicates)
                )
                if market.sql_action is SQLAction.SKIP:
                    counters.markets_skipped += 1
                    if self.debug_logging_enabled:
                        logger.debug("Skipping '%s'.", market.market_data_file)
                    continue
                elif market.sql_action is SQLAction.UPDATE:
                    # SQL does not support updating a whole row at a time and requires one to list
                    # individual fields and values to update. A simpler way to achieve the same
                    # outcome is to delete and re-insert the row.
                    connection.execute(
                        f"DELETE FROM {SQL_TABLE_NAME}"
                        f" WHERE {MARKET_METADATA_FILE_PATH} = '{market.market_metadata_file.resolve()}'"
                    )
                    counters.markets_updated += 1
                    if self.debug_logging_enabled:
                        logger.debug("Updating '%s'.", market.market_data_file)

            # This section is always executed, for both updating and indexing
            sql_data_map = market.create_sql_mapping(
                racing_data_processor.get(
                    market
                )  # Inject additional racing metadata, if any
            )
            connection.execute(
                f"INSERT INTO {SQL_TABLE_NAME} VALUES ({','.join('?'*len(sql_data_map))})",
                tuple(sql_data_map.values()),
            )
            counters.rows_inserted += 1
            if self.debug_logging_enabled and (market.sql_action is SQLAction.INSERT):
                logger.debug("Adding '%s'.", market.market_data_file)
