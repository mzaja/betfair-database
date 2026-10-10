import logging
from collections import defaultdict
from pathlib import Path

from betfairdatabase.const import (
    DATA_FILE_SUFFIXES,
    METADATA_FILE_NAME,
)
from betfairdatabase.tree.directory import DatabaseDirectory
from betfairdatabase.tree.metadatafile import BulkMetadataFile
from betfairdatabase.utils import (
    ProgressBarMixin,
)

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


class DirectoryParser(ProgressBarMixin):
    """Parses a database directory, with an optional progress bar."""

    def parse_directory_tree(self, root_dir: Path) -> list[DatabaseDirectory]:
        """
        Locates market data and metadata files in the source directory,
        linking them using a common dictionary key.
        """
        data_file_suffixes = list(DATA_FILE_SUFFIXES)
        data_file_suffixes.remove("")
        data_files = defaultdict(list)
        individual_metadata_files = defaultdict(list)
        bulk_metadata_files = {}
        # pathlib.Path.rglob does not support brace expansion to look for multiple
        # patterns at once. Therefore, all files need to be located and filtered
        # manually to avoid traversing the whole directory tree more than once.
        for file in self._progress_bar(root_dir.rglob("*"), "Locating markets"):
            file_name = file.name
            if file_name.startswith("1."):
                file_suffix = file.suffix
                # Metadata files always have a .json extension
                if file_suffix == ".json":
                    individual_metadata_files[file.parent].append(file)
                # Data files
                # Uncompressed data files do not have an extension, but
                # the numbers following 1. are treated as one.
                # They usually have 9 "decimal places"
                elif (file_suffix in data_file_suffixes) or (len(file_suffix) > 8):
                    data_files[file.parent].append(file)
            elif file_name == METADATA_FILE_NAME:
                bulk_metadata_files[file.parent] = file
        return [
            DatabaseDirectory(
                dnm,
                dfs,
                individual_metadata_files.get(dnm, []),
                (
                    BulkMetadataFile(bmf)
                    if (bmf := bulk_metadata_files.get(dnm))
                    else None
                ),
            )
            for dnm, dfs in data_files.items()
        ]
