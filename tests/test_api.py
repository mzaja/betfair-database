import inspect
import unittest
from collections import OrderedDict
from unittest import mock

import betfairdatabase.api as api

# "progress_bar" is deliberately omitted because it is API-only and does not exist in
# BetfairDatabase class (the class accepts it as a constructor arguments)
API_METHOD_NAMES = ("clean", "columns", "export", "index", "insert", "select", "size")


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
        for method_name in API_METHOD_NAMES:
            self.assert_docstrings_equal(
                getattr(api, method_name).__doc__,
                getattr(api.BetfairDatabase, method_name).__doc__,
            )

    def test_signature_equality(self):
        """
        Tests equality of call signatures between module-level and OOP interfaces.
        This test covers argument names, order, default value, kind (keyword/positional/both)
        and annotations.
        """
        oop_init_param = inspect.signature(api.BetfairDatabase.__init__).parameters[
            "database_dir"
        ]
        for method_name in API_METHOD_NAMES:
            api_sign = inspect.signature(getattr(api, method_name))
            oop_sign = inspect.signature(getattr(api.BetfairDatabase, method_name))
            self.assertEqual(api_sign.return_annotation, oop_sign.return_annotation)
            # Test parameters
            api_params = OrderedDict(api_sign.parameters)
            oop_params = OrderedDict(oop_sign.parameters)
            oop_params.pop("self", None)
            if database_dir_param := api_params.pop("database_dir", None):
                self.assertEqual(database_dir_param, oop_init_param)
            self.assertEqual(api_params, oop_params)

    def test_delegated_calls(self):
        """API calls must be delegated to the correct BetfairDatabase's method."""
        DATABASE_DIR = "some_random_dir"
        # Most API methods expect the database name and the first argument, followed
        # by at least one optional argument and up to one mandatory argument.
        # Because the value of the second argument does not matter in this test,
        # it is set to None
        DEFAULT_CALL_ARGS = (DATABASE_DIR, None)
        CUSTOM_CALL_ARGS = {"clean": (DATABASE_DIR,), "size": (DATABASE_DIR,)}
        functions_under_test = list(API_METHOD_NAMES)
        functions_under_test.remove("columns")
        for api_func_name in functions_under_test:
            with (
                self.subTest(api_func=api_func_name),
                mock.patch("betfairdatabase.api.BetfairDatabase") as mock_db_class,
            ):
                mock_db_class.return_value = mock_db_instance = mock.Mock()
                getattr(api, api_func_name)(
                    *CUSTOM_CALL_ARGS.get(api_func_name, DEFAULT_CALL_ARGS)
                )
                mock_db_class.assert_called_with(DATABASE_DIR, mock.ANY)
                getattr(mock_db_instance, api_func_name).assert_called()

        # Test class method
        with (
            self.subTest(api_func="columns"),
            mock.patch("betfairdatabase.api.BetfairDatabase") as mock_db_class,
        ):
            api.columns()
            mock_db_class.columns.assert_called_with()

    def test_progress_bar_configuration(self):
        """Tests configuring the progress bar at module-level API."""
        DATABASE_DIR = "some_random_dir"
        DEFAULT_CALL_ARGS = (DATABASE_DIR, None)
        CUSTOM_CALL_ARGS = {"clean": (DATABASE_DIR,), "size": (DATABASE_DIR,)}
        functions_under_test = list(API_METHOD_NAMES)
        functions_under_test.remove("columns")
        for progress_bar_enabled in [True, False]:
            for api_func_name in functions_under_test:
                with (
                    self.subTest(
                        api_func=api_func_name,
                        progress_bar=progress_bar_enabled,
                    ),
                    mock.patch("betfairdatabase.api.BetfairDatabase") as mock_db_class,
                ):
                    mock_db_class.return_value = mock.Mock()
                    api.progress_bar(progress_bar_enabled)
                    getattr(api, api_func_name)(
                        *CUSTOM_CALL_ARGS.get(api_func_name, DEFAULT_CALL_ARGS)
                    )
                    mock_db_class.assert_called_with(DATABASE_DIR, progress_bar_enabled)
