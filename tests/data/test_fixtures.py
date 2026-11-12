import unittest
from collections import Counter

from tests.data.fixtures import DATASETS_ROOT_DIR


class TestDatasets(unittest.TestCase):
    """Sanity checks on the datasets."""

    def test_dataset_files_are_unique(self):
        """
        Verifies that all datasets, with one exception (`duplicates`),
        contain unique market data and metadata file names.

        If file names are not unique, flattening the dataset will silently overwrite some
        of the files, which must be avoided.
        """
        market_files = [
            f for f in DATASETS_ROOT_DIR.rglob("1.*") if f.parent.name != "duplicates"
        ]
        file_names = [f.name for f in market_files]
        duplicate_names = [f for f, c in Counter(file_names).items() if c > 1]
        duplicate_files = [f for f in market_files if f.name in duplicate_names]
        # Prints paths to duplicates on failed assertion
        self.assertEqual(duplicate_files, [])
