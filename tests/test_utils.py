import unittest
from contextlib import contextmanager
from os import SEEK_SET
from tempfile import NamedTemporaryFile, TemporaryFile
from time import perf_counter_ns
from unittest import mock

from betfairdatabase.utils import (
    REVERSE_READ_STEP,
    parse_datetime,
    read_last_line_in_a_file,
)

TIMESTAMP = "2023-06-01T17:09:37.000Z"
DEFAULT_FILLER_LINE_LENGTH = 1024


@contextmanager
def create_temp_test_file(
    number_of_filler_lines: int,
    filler_line_length: int = DEFAULT_FILLER_LINE_LENGTH,
    last_line_length: int | None = None,
    named: bool = False,
):
    """
    Creates a temporary test file based on imput parameters and returns the path to it.

    All filler lines contain b"A" repeated line_length - 1 times and terminated by
    a newline, and are repeated number_of_filler_lines times.
    The last line contains b"Z" last_line_length - 1 times.
    last_line_length defaults to line_length if not specified.
    """
    last_line_length = last_line_length or filler_line_length
    lines = (b"A" * (filler_line_length - 1) + b"\n") * number_of_filler_lines + (
        b"Z" * (last_line_length - 1) + b"\n"
    )
    with (NamedTemporaryFile if named else TemporaryFile)("w+b") as f:
        f.write(lines)
        f.seek(0, SEEK_SET)
        yield f


class TestUtils(unittest.TestCase):
    """Tests utils submodule."""

    def test_parse_datetime(self):
        """Tests parsing datetime in Betfair's format."""
        dt = parse_datetime(TIMESTAMP)
        self.assertEqual(dt.year, 2023)
        self.assertEqual(dt.month, 6)
        self.assertEqual(dt.day, 1)
        self.assertEqual(dt.hour, 17)
        self.assertEqual(dt.minute, 9)
        self.assertEqual(dt.second, 37)
        self.assertEqual(dt.tzname(), "UTC")

    @mock.patch("betfairdatabase.utils.dt.datetime")
    def test_parse_datetime_3_10(self, mock_datetime):
        """
        Simulates parsing datetime on Python 3.10.
        This test is required to achieve 100 % test coverage.
        It will be removed in the future when 3.10 stops being supported.

        When Python 3.10 is actually used to run these tests, test_parse_datetime()
        validates the correct behaviour.
        """
        mock_datetime.fromisoformat.side_effect = (ValueError, mock.Mock())
        parse_datetime(TIMESTAMP)
        mock_datetime.fromisoformat.assert_called_with("2023-06-01T17:09:37.000")

    def test_read_last_line_small_files(self):
        """Tests boundary conditions when invoked on files with 0, 1 and 2 lines."""
        SAMPLE_TEXT = b"Just a single line file"
        with TemporaryFile("w+b") as f:
            # Empty file
            self.assertEqual(read_last_line_in_a_file(f), b"")
            # File with a single line
            f.write(SAMPLE_TEXT)
            f.seek(0, SEEK_SET)
            self.assertEqual(read_last_line_in_a_file(f), SAMPLE_TEXT)
            # File with a single line, terminated by a newline
            f.write(b"\n")
            f.seek(0, SEEK_SET)
            self.assertEqual(read_last_line_in_a_file(f), SAMPLE_TEXT + b"\n")
            # File with two lines, started and terminated by a newline
            f.seek(0, SEEK_SET)
            f.write(b"\n" + f.read())  # Insert a newline at the beginning of the file
            f.seek(0, SEEK_SET)
            self.assertEqual(read_last_line_in_a_file(f), SAMPLE_TEXT + b"\n")

    def test_read_last_line_in_a_file_performance(self):
        """
        Reading the last line in a file using this method should be considerably faster
        than iterating through the same file line-by-line.
        """
        # Use a named file to ensure it is actually located on the file system, and is not
        # a vritual file store in RAM
        number_of_filler_lines = 1000
        self.assertGreater(
            # Ensure the file size is greater than the read step by a significant margin
            number_of_filler_lines * DEFAULT_FILLER_LINE_LENGTH,
            5 * REVERSE_READ_STEP,
        )
        char_repeats = DEFAULT_FILLER_LINE_LENGTH - 1
        with create_temp_test_file(number_of_filler_lines, named=True) as f:
            # Measure the time for the helper function under test
            t_start = perf_counter_ns()
            line = read_last_line_in_a_file(f)
            t_jump_to_back = perf_counter_ns() - t_start
            self.assertEqual(line, b"Z" * char_repeats + b"\n")  # Test correctness

            # Measure the time for getting the last line via iteration
            f.seek(0, SEEK_SET)  # Move the cursor back to the beginning of the file
            t_start = perf_counter_ns()
            for line in f:
                pass
            t_iterate = perf_counter_ns() - t_start
            self.assertEqual(line, b"Z" * char_repeats + b"\n")  # Test correctness

            # Measure the time for getting the last line via readlines()[-1]
            f.seek(0, SEEK_SET)  # Move the cursor back to the beginning of the file
            t_start = perf_counter_ns()
            line = f.readlines()[-1]
            t_readlines = perf_counter_ns() - t_start
            self.assertEqual(line, b"Z" * char_repeats + b"\n")  # Test correctness

            for t_conventional in [t_iterate, t_readlines]:
                # The method should be at least twice as fast as conventional ones
                self.assertLess(2 * t_jump_to_back, t_conventional)

    def test_read_last_line_in_a_file_line_exceeds_read_step(self):
        """
        The last line is read even when it is very long and exceeds the initial read step.
        """
        last_line_length = REVERSE_READ_STEP * 3 + 4
        with create_temp_test_file(
            number_of_filler_lines=3, last_line_length=last_line_length
        ) as f:
            self.assertEqual(
                read_last_line_in_a_file(f), b"Z" * (last_line_length - 1) + b"\n"
            )
