import contextlib
import importlib.metadata
import inspect
import logging
import platform
import subprocess
import sys
import unittest
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest import mock

import betfairdatabase.api as api
from betfairdatabase.cli import (
    IMPORT_PATTERNS,
    ON_DUPLICATES,
    get_parser,
    get_version,
    logger,
    main,
)
from betfairdatabase.const import DuplicatePolicy
from betfairdatabase.imports import ImportPatterns

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
        """Test the function for retrieving the app/package version."""
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

    def test_version(self):
        """Tests retrieving the version number."""
        # Test normal arguments
        for arg in ["--version", "-V"]:
            stdout_buffer = StringIO()
            with (
                self.assertRaises(SystemExit) as ctx,
                contextlib.redirect_stdout(stdout_buffer),
            ):
                self.call_main_with_args(arg)
            self.assertEqual(ctx.exception.code, 0)
            self.assertEqual(stdout_buffer.getvalue().strip(), get_version())

        # Test deprecated argument
        stdout_buffer = StringIO()
        with (
            self.assertRaises(SystemExit) as ctx,
            contextlib.redirect_stdout(stdout_buffer),
        ):
            self.call_main_with_args("-v")
        self.assertEqual(ctx.exception.code, 0)
        version, message = stdout_buffer.getvalue().splitlines()
        self.assertEqual(version, get_version())
        self.assertIn("deprecated", message)

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
        # With specifying dest
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR, DEST
        ).export.assert_called_once_with(DATABASE_DIR, DEST)
        # Without specifying dest
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR
        ).export.assert_called_once_with(DATABASE_DIR, ".")

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

    def test_size_sub_command(self):
        """Tests "size" sub-command."""
        SUB_COMMAND = "size"
        self.call_main_with_args(
            SUB_COMMAND, DATABASE_DIR
        ).size.assert_called_once_with(DATABASE_DIR)

    def test_parser(self):
        """Parser should return enough options to cover all API input parameters."""
        parser = get_parser()
        generic_args_count = 3  # --version, --quiet apply to all sub-commands
        for cmd, method in [
            (["index", DATABASE_DIR], api.index),
            (["export", DATABASE_DIR, "dst"], api.export),
            (["insert", DATABASE_DIR, "src"], api.insert),
            (["clean", DATABASE_DIR], api.clean),
            (["size", DATABASE_DIR], api.size),
        ]:
            with self.subTest(cmd=cmd):
                args = parser.parse_args(cmd)
                self.assertEqual(
                    # -1 because a sub-command is one arg
                    len(vars(args)) - generic_args_count - 1,
                    len(inspect.signature(method).parameters),
                )

    def test_calling_module(self):
        """Tests calling the module with 'python -m <module>'."""
        # Unfortunately, this test does not increase code coverage
        python_exe = "python" + ("3" if platform.system() == "Linux" else "")
        proc = subprocess.run(
            f"{python_exe} -m betfairdatabase --version",
            shell=True,
            capture_output=True,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout.strip().decode(), get_version())

    def test_logging(self):
        """
        Tests the control of logging levels by -q/--quiet and -v/--verbose
        options and the formatter settings.
        """
        global logger
        original_logger_level = logger.getEffectiveLevel()
        original_logger_disabled = logger.disabled
        try:
            for option, logger_disabled, logger_level in [
                ("", False, logging.INFO),
                ("-v", False, logging.DEBUG),
                ("--verbose", False, logging.DEBUG),
                ("-q", True, logging.INFO),
                ("--quiet", True, logging.INFO),
            ]:
                with self.subTest(
                    option=option,
                    logger_disabled=logger_disabled,
                    logger_level=logger_level,
                ):
                    # Any command will do here - flags are the key
                    mock_api = self.call_main_with_args(
                        *([option] if option else []), "size", "."
                    )
                    self.assertEqual(logger.getEffectiveLevel(), logger_level)
                    self.assertIs(logger.disabled, logger_disabled)
                    # Disabling logging should also disable the progress bar.
                    # The opposite test cannot be performed because
                    # api.progress_bar(True) call is not made explicitly,
                    # given that the default is set to True and CLI use is one-shot.
                    if logger_disabled:
                        mock_api.progress_bar.assert_called_with(False)
        finally:
            logger.setLevel(original_logger_level)
            logger.disabled = original_logger_disabled

    def test_disable_progress_bar(self):
        """Tests disabling the progress bar via the --no-progress switch."""
        mock_api = self.call_main_with_args("size", ".")
        mock_api.progress_bar.assert_not_called()
        mock_api = self.call_main_with_args("--no-progress-bar", "size", ".")
        mock_api.progress_bar.assert_called_with(False)


@mock.patch("builtins.print")
@mock.patch("builtins.exit")
class TestErrorHandling(unittest.TestCase):
    """
    Tests error handling in the command line application.
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
        sys.argv = ["bfdb"] + list(args)
        main()

    def test_index_missing_error(
        self, mock_exit: mock.MagicMock, mock_print: mock.MagicMock
    ):
        """
        IndexMissingError is caught, a meaningful error message is
        printed and a non-zero exit code is returned.
        """
        with TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir).resolve()
            for cmd in ["clean", "size"]:
                with self.subTest(cmd=cmd):
                    self.call_main_with_args(cmd, str(tmpdir))
                    self.assertNotEqual(mock_exit.call_args[0][0], 0)
                    self.assertIn(
                        f"Betfair database index not found in '{tmpdir}'.",
                        mock_print.call_args[0][0],
                    )

    def test_index_exists_error(
        self, mock_exit: mock.MagicMock, mock_print: mock.MagicMock
    ):
        """
        IndexExistsError is caught, a meaningful error message is
        printed and a non-zero exit code is returned.
        """
        with TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir).resolve()
            self.call_main_with_args("index", str(tmpdir))  # All good
            self.call_main_with_args("index", str(tmpdir))  # Error
            self.assertNotEqual(mock_exit.call_args[0][0], 0)
            self.assertIn(
                f"Betfair database index already exists in '{tmpdir}'."
                " Use -f/--force option to reindex the database.",
                mock_print.call_args[0][0],
            )

    def test_database_directory_does_not_exist(
        self, mock_exit: mock.MagicMock, mock_print: mock.MagicMock
    ):
        """
        DatabaseDirectoryError is caught, a meaningful error
        message is printed and a non-zero exit code is returned.
        """
        database_dir = "./does/not/exist"
        for args in [
            ("index", database_dir),
            ("export", database_dir, "dst"),
            ("insert", database_dir, "src"),
            ("clean", database_dir),
            ("size", database_dir),
        ]:
            with self.subTest(args=args):
                self.call_main_with_args(*args)
                self.assertNotEqual(mock_exit.call_args[0][0], 0)
                self.assertIn(
                    f"'{database_dir}' does not exist.", mock_print.call_args[0][0]
                )

    def test_test_database_directory_is_not_a_directory(
        self, mock_exit: mock.MagicMock, mock_print: mock.MagicMock
    ):
        """
        DatabaseDirectoryError is caught, a meaningful error
        message is printed and a non-zero exit code is returned.
        """
        with NamedTemporaryFile() as tmpf:
            database_dir = tmpf.name
            for args in [
                ("index", database_dir),
                ("export", database_dir, "dst"),
                ("insert", database_dir, "src"),
                ("clean", database_dir),
                ("size", database_dir),
            ]:
                with self.subTest(args=args):
                    self.call_main_with_args(*args)
                    self.assertNotEqual(mock_exit.call_args[0][0], 0)
                    self.assertIn(
                        f"'{database_dir}' is not a directory.",
                        mock_print.call_args[0][0],
                    )
