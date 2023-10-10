from argparse import ArgumentParser

from betfairdatabase import api
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
        "-v",
        "--version",
        action="version",
        version=get_version(),
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

    return parser


def main():
    """Entry point for the command line app."""
    parser = get_parser()
    args = parser.parse_args()
    match args.command:
        case "index":
            api.index(args.database_dir, args.force)
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
            api.clean(args.database_dir)
        case _:  # pragma: no cover
            # This branch should never be reached as arg parser validates sub-commands
            print("Unsupported sub-command.")
            parser.print_help()
            exit(1)
    return
