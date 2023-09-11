import unittest
from unittest import mock

from betfairdatabase.api import (
    BetfairDatabase,
    clean,
    columns,
    export,
    index,
    insert,
    select,
)


class TestAPI(unittest.TestCase):
    """
    Unit-tests the module-level API.
    """

    def assert_docstrings_equal(self, docstr_1: str, docstr_2: str):
        """
        Compares two docstrings line-by-line, ignoring leading and trailing whitespace.
        """
        for s1, s2 in zip(docstr_1.splitlines(), docstr_2.splitlines()):
            self.assertEqual(s1.strip(), s2.strip())

    def test_docstring_equality(self):
        """
        Docstrings for module-level API and BetfariDatabase class must be identical.
        """
        db = BetfairDatabase(".")
        self.assert_docstrings_equal(index.__doc__, db.index.__doc__)
        self.assert_docstrings_equal(select.__doc__, db.select.__doc__)
        self.assert_docstrings_equal(columns.__doc__, db.columns.__doc__)
        self.assert_docstrings_equal(export.__doc__, db.export.__doc__)
        self.assert_docstrings_equal(insert.__doc__, db.insert.__doc__)
        self.assert_docstrings_equal(clean.__doc__, db.clean.__doc__)

    def test_delegated_calls(self):
        """
        API calls must be delegated to the correct BetfairDatabase's method.
        """
        DATABASE_DIR = "some_random_dir"
        DEFAULT_CALL_ARGS = (DATABASE_DIR, None)
        CALL_ARGS = {"clean": (DATABASE_DIR,)}  # List non-default call args only
        # Test instance methods
        for api_func_name in ("index", "select", "export", "insert", "clean"):
            with self.subTest(api_func=api_func_name), mock.patch(
                "betfairdatabase.api.BetfairDatabase"
            ) as mock_db_class:
                mock_db_instance = mock.Mock()
                mock_db_class.return_value = mock_db_instance
                globals()[api_func_name](
                    *CALL_ARGS.get(api_func_name, DEFAULT_CALL_ARGS)
                )
                mock_db_class.assert_called_with(DATABASE_DIR)
                getattr(mock_db_instance, api_func_name).assert_called()

        # Test class method
        with self.subTest(api_func="columns"), mock.patch(
            "betfairdatabase.api.BetfairDatabase"
        ) as mock_db_class:
            columns()
            mock_db_class.columns.assert_called()
