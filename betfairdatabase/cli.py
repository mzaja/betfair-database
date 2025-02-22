import logging
from argparse import ArgumentParser, Namespace

from betfairdatabase import api
from betfairdatabase.database import logger
from betfairdatabase.exceptions import (
    DatabaseDirectoryError,
    IndexExistsError,
    IndexMissingError,
)
from betfairdatabase.utils import ImportPatterns

IMPORT_PATTERNS = ("betfair_historical", "event_id", "flat")
ON_DUPLICATES = ("skip", "replace", "update")


def get_version() -> str:
    """Returns the package version number."""
    import importlib.metadata

    try:
        return importlib.metadata.version("betfairdatabase")
    except importlib.metadata.PackageNotFoundError:
        return ""


def get_parser() -> ArgumentParser:
    """
    Creates and returns a command line argument parser.

    Call parser.parse_args() to parse arguments.
    """
    parser = ArgumentParser(
        prog="bfdb", description="betfairdatabase command line app."
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=get_version(),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Increase amount of displayed messages to debug level.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress printing to terminal, including error messages. Also hides progress bars.",
    )
    parser.add_argument(
        "--no-progress-bar", action="store_true", help="Hides progress bars."
    )

    subparsers = parser.add_subparsers(
        title="sub-commands", dest="command", required=True
    )

    # Sub-parser for index command
    parser_index = subparsers.add_parser(
        "index",
        description="Turns the target directory into a database by indexing its contents.",
    )
    parser_index.add_argument(
        "database_dir",
        help="Directory containing captured Betfair files to be turned into a database.",
    )
    parser_index.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Overwrite an index if it already exists.",
    )

    # Sub-parser for index command
    parser_export = subparsers.add_parser(
        "export", description="Exports the indexed Betfair database to a CSV file."
    )
    parser_export.add_argument(
        "database_dir",
        help="Top-level directory of an indexed Betfair database.",
    )
    parser_export.add_argument(
        "dest",
        # required=False, # This does not work for positional arguments, sadly
        nargs="?",  # Expect 0 or 1 value for this argument
        default=".",
        help="Destination directory or file to which to export the database.",
    )

    # Sub-parser for insert command
    parser_insert = subparsers.add_parser(
        "insert",
        description="Inserts market catalogue/data files from source_dir into the database.",
    )
    parser_insert.add_argument(
        "database_dir",
        help="Top-level directory of a Betfair database.",
    )
    parser_insert.add_argument(
        "source_dir",
        help="Directory containing files to import into the database.",
    )
    parser_insert.add_argument(
        "-c",
        "--copy",
        action="store_true",
        help="Copy files into the database instead of moving them.",
    )
    parser_insert.add_argument(
        "-p",
        "--pattern",
        default=IMPORT_PATTERNS[0],  # betfair_historical
        choices=IMPORT_PATTERNS,
        help="File import pattern.",
    )
    parser_insert.add_argument(
        "-d",
        "--on-duplicates",
        default=ON_DUPLICATES[-1],  # update
        choices=ON_DUPLICATES,
        help="Duplicate handling policy.",
    )

    # Sub-parser for clean command
    parser_clean = subparsers.add_parser(
        "clean",
        description="Removes entries with missing market data files from the database.",
    )
    parser_clean.add_argument(
        "database_dir",
        help="Top-level directory of an indexed Betfair database.",
    )

    # Sub-parser for size command
    parser_clean = subparsers.add_parser(
        "size",
        description="Returns the number of indexed entries in the database.",
    )
    parser_clean.add_argument(
        "database_dir",
        help="Top-level directory of an indexed Betfair database.",
    )

    return parser


def setup_logging(args: Namespace) -> None:
    """
    Configures the level of command line tool logging.
    Disables the progress bar if quiet mode is activated.
    """
    logging_level = logging.INFO  # Default logging level
    if args.verbose:
        logging_level = logging.DEBUG
    if args.quiet:
        logger.disabled = True  # quiet overrides verbose
        api.progress_bar(False)  # Disable progress bar
    logger.setLevel(logging_level)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)


def handle_deprecated_version_flag():
    """
    Handles the use of deprecated '-v' flag for '--version',
    which is now reassigned to stand for '--verbose'.
    """
    # Inspecting sys.argv is a bit messy, but the most
    # practical option in this case
    import sys

    # If '-v' is invoked with a sub-command, it must mean '--verbose'.
    # '--version' only makes sense when invoked alone
    if (len(sys.argv) == 2) and (sys.argv[1] == "-v"):
        print(get_version())
        print(
            "Using '-v' (lowercase) as a shorthand for '--version' is now deprecated. "
            "Use '-V' (uppercase) instead. '-v' (lowercase) is now a shorthand for '--verbose'."
        )
        exit(0)


def main():
    """Entry point for the command line app."""
    handle_deprecated_version_flag()
    parser = get_parser()
    args = parser.parse_args()
    setup_logging(args)
    if args.no_progress_bar:
        api.progress_bar(False)
    try:
        match args.command:
            case "index":
                try:
                    api.index(args.database_dir, args.force)
                except IndexExistsError as ex:
                    print(
                        ex.base_msg + " Use -f/--force option to reindex the database."
                    )
                    exit(1)
            case "export":
                api.export(args.database_dir, args.dest)
            case "insert":
                # Parser should catch invalid options for "pattern"
                pattern = getattr(ImportPatterns, args.pattern)
                api.insert(
                    args.database_dir,
                    args.source_dir,
                    args.copy,
                    pattern,
                    args.on_duplicates,
                )
            case "clean":
                try:
                    api.clean(args.database_dir)
                except IndexMissingError as ex:
                    print(str(ex))
                    exit(1)
            case "size":
                try:
                    print(api.size(args.database_dir))
                except IndexMissingError as ex:
                    print(str(ex))
                    exit(1)
            case _:  # pragma: no cover
                # This branch should never be reached as arg parser validates sub-commands
                print("Unsupported sub-command.")
                parser.print_help()
                exit(1)
    except DatabaseDirectoryError as ex:
        print(str(ex))
        exit(1)
    return
