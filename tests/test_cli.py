import importlib.metadata
import inspect
import platform
import subprocess
import sys
import unittest
from unittest import mock

import betfairdatabase.api as api
from betfairdatabase.cli import (
    IMPORT_PATTERNS,
    ON_DUPLICATES,
    get_parser,
    get_version,
    main,
)
from betfairdatabase.const import DuplicatePolicy
from betfairdatabase.utils import ImportPatterns

DATABASE_DIR = "./my_db_dir"


class TestCLI(unittest.TestCase):
    """
    Tests the module's command line interface.
    """

    def setUp(self):
        self.sys_argv_old = sys.argv

    def tearDown(self):
        sys.argv = self.sys_argv_old

    @staticmethod
    def call_main_with_args(*args) -> mock.Mock:
        """
        Calls the CLI with arguments and returns an API mock to test calls.

        First argument (script name) should not be provided inside args.
        """
        with mock.patch("betfairdatabase.cli.api") as mock_api:
            sys.argv = ["bfdb"] + list(args)
            main()
            return mock_api

    @mock.patch("importlib.metadata.version")
    def test_get_version(self, mock_importlib_version: mock.Mock):
        """Tests retrieving the app/package version."""
        VERSION = "x.y.z"
        mock_importlib_version.return_value = VERSION
        self.assertEqual(get_version(), VERSION)
        mock_importlib_version.assert_called_with("betfairdatabase")

    @mock.patch("importlib.metadata.version")
    def test_get_version_missing_package(self, mock_importlib_version: mock.Mock):
        """Failure in retrieving the app/package version."""
        mock_importlib_version.side_effect = importlib.metadata.PackageNotFoundError
        # Error should not be propagated but a placeholder value returned
        self.assertEqual(get_version(), "")

    def test_index_sub_command(self):
        """Tests "index" sub-command."""
        SUB_COMMAND = "index"
        # Without options
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR
        ).index.assert_called_once_with(DATABASE_DIR, False)
        # -f
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR, "-f"
        ).index.assert_called_once_with(DATABASE_DIR, True)
        # --force
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR, "--force"
        ).index.assert_called_once_with(DATABASE_DIR, True)

    def test_export_sub_command(self):
        """Tests "export" sub-command."""
        SUB_COMMAND = "export"
        DEST = "./my_dest"
        # Without options
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR, DEST
        ).export.assert_called_once_with(DATABASE_DIR, DEST)

    def test_import_patterns_valid(self):
        """
        All import patterns available through CLI ahouls be valid
        attribute names of ImportPatterns and cover all options.
        """
        # All public method names in ImportPatterns are listed in IMPORT_PATTERNS
        self.assertEqual(
            len(IMPORT_PATTERNS),
            len([mthd for mthd in dir(ImportPatterns) if not mthd.startswith("_")]),
        )
        # All import patterns can be retrieved by name
        # (test passes if no errors are raised)
        for pattern in IMPORT_PATTERNS:
            getattr(ImportPatterns, pattern)

    def test_duplicate_policies_valid(self):
        """Tests that import policy options are complete and valid."""
        self.assertEqual(len(ON_DUPLICATES), len(DuplicatePolicy))
        self.assertSetEqual(set(ON_DUPLICATES), set(x.value for x in DuplicatePolicy))

    def test_insert_sub_command(self):
        """Tests "insert" sub-command."""
        # Test setup
        SUB_COMMAND = "insert"
        SOURCE_DIR = "./my_src_dir"
        copy_default = False
        pattern_default = ImportPatterns.betfair_historical
        on_duplicates_default = DuplicatePolicy.UPDATE.value
        # Without options
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR, SOURCE_DIR
        ).insert.assert_called_once_with(
            DATABASE_DIR,
            SOURCE_DIR,
            copy_default,
            pattern_default,
            on_duplicates_default,
        )
        # Copy option
        for option in ("-c", "--copy"):
            self.call_main_with_args(
                SUB_COMMAND, DATABASE_DIR, SOURCE_DIR, option
            ).insert.assert_called_once_with(
                DATABASE_DIR, SOURCE_DIR, True, pattern_default, on_duplicates_default
            )
        # Pattern option
        for option in ("-p", "--pattern"):
            for pattern in IMPORT_PATTERNS:
                self.call_main_with_args(
                    SUB_COMMAND, DATABASE_DIR, SOURCE_DIR, option, pattern
                ).insert.assert_called_once_with(
                    DATABASE_DIR,
                    SOURCE_DIR,
                    copy_default,
                    getattr(ImportPatterns, pattern),
                    on_duplicates_default,
                )
        # Duplicates option
        for option in ("-d",):  # "--on-duplicates"):
            for policy in ON_DUPLICATES:
                self.call_main_with_args(
                    SUB_COMMAND, DATABASE_DIR, SOURCE_DIR, option, policy
                ).insert.assert_called_once_with(
                    DATABASE_DIR,
                    SOURCE_DIR,
                    copy_default,
                    pattern_default,
                    policy,
                )

    def test_clean_sub_command(self):
        """Tests "clean" sub-command."""
        SUB_COMMAND = "clean"
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR
        ).clean.assert_called_once_with(DATABASE_DIR)

    def test_parser(self):
        """Parser should return enough options to cover all API input parameters."""
        parser = get_parser()
        for cmd, method in [
            (["index", DATABASE_DIR], api.index),
            (["export", DATABASE_DIR, "dst"], api.export),
            (["insert", DATABASE_DIR, "src"], api.insert),
            (["clean", DATABASE_DIR], api.clean),
        ]:
            with self.subTest(cmd=cmd):
                args = parser.parse_args(cmd)
                self.assertEqual(
                    len(vars(args)) - 1,  # -1 because a sub-command is one arg
                    len(inspect.signature(method).parameters),
                )

    def test_calling_module(self):
        """Tests calling the module with the -m flag."""
        # Unfortunately, this test does not increase code coverage
        python_exe = "python" + ("3" if platform.system() == "Linux" else "")
        proc = subprocess.run(f"{python_exe} -m betfairdatabase --version", shell=True)
        self.assertEqual(proc.returncode, 0)
