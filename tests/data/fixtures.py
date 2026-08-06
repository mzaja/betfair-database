import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

TEST_DATA_DIR = Path(__file__).parent


@dataclass(kw_only=True)
class Datasets:
    """Selects importable datasets for testing purposes."""

    compressed: bool = False
    uncompressed: bool = False
    official: bool = False
    corrupt: bool = False
    missing_data: bool = False
    missing_metadata: bool = False
    duplicates: bool = False

    def get_paths(self, absolute: bool = True) -> list[Path]:
        """Returns a list of paths to the selected datasets."""
        paths = []
        if self.compressed:
            paths.append("datasets/zip-lzma")
        if self.uncompressed:
            paths.append("datasets/uncompressed")
        if self.official:
            paths.append("datasets/official")
        if self.corrupt:
            paths.append("corrupt")
        if self.missing_data:
            paths.append("missing_data")
        if self.missing_metadata:
            paths.append("missing_metadata")
        if self.duplicates:
            paths.append("duplicates")
        if absolute:
            return [TEST_DATA_DIR / p for p in paths]
        return [Path(p) for p in paths]

    def copy_files(self, dest_dir: Path | str, flatten: bool = False) -> None:
        """Copies the selected datasets to the specified directory."""
        dest_dir = Path(dest_dir).resolve()  # Resolve needed for tempdirs
        for dataset in self.get_paths(absolute=False):
            dest = dest_dir / ("" if flatten else dataset)
            shutil.copytree(TEST_DATA_DIR / dataset, dest, dirs_exist_ok=True)


class TestFixture:
    """
    Creates a text fixture by copying select datasets to a target directory.

    When used as a context manager, the temporary directory is automatically
    generated on init and removed on exit.
    """

    def __init__(
        self,
        datasets: Datasets,
        flatten: bool = False,
    ):
        self._tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self._tempdir.name).resolve()
        datasets.copy_files(self.path, flatten=flatten)

    def close(self) -> None:
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
