import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from betfairdatabase import BetfairDatabase


class TestBetfairDatabase(unittest.TestCase):
    """
    Additional tests for BetfairDatabase class which
    are not covered by integration tests.
    """

    TEST_DATA_DIR = Path("./tests/data")

    @patch("betfairdatabase.database.logger")
    def test_corrupt_market_catalogue(self, mock_logger: MagicMock):
        """
        Corrupt market catalogue does not break indexing.
        Corrupt file's name is logged.
        """
        # For some reason, this test breaks if invoked using "Debug Test" option
        # from Visual Studio Code's Test Explorer extension. It works fine if a
        # standard debug session is launched by pressing F5.
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir).resolve()
            shutil.copytree(self.TEST_DATA_DIR / "corrupt", tmpdir, dirs_exist_ok=True)
            shutil.copytree(
                self.TEST_DATA_DIR / "datasets/uncompressed", tmpdir, dirs_exist_ok=True
            )
            database = BetfairDatabase(tmpdir)
            database.index()
            self.assertIsNotNone(
                re.search(
                    "Error parsing '.*1\.221089567\.json'",
                    mock_logger.error.call_args[0][0],
                )
            )  # Logging module was called with the corrupt file's name
            all_rows = database.select(columns=["marketId"])
            self.assertTrue(len(all_rows) > 0)  # Valid markets got imported
            for row in all_rows:
                # Corrupt market did not get imported
                self.assertNotEqual(row["marketId"], "1.221089567")
