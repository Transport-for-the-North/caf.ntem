"""Main module for caf.ntem."""

from __future__ import annotations

# Built-Ins
import argparse
import logging
import os
import sys
import warnings

# Third Party
import caf.toolkit as ctk
import pydantic
import tqdm.contrib.logging as tqdm_log

# Local Imports
import caf.ntem as ntem  # pylint: disable = ungrouped-imports, consider-using-from-import
from caf.ntem import build, inputs, ntem_constants

_TRACEBACK = ctk.arguments.getenv_bool("NTEM_TRACEBACK", False)
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


def _config_parse(args: argparse.Namespace) -> ntem_constants.InputBase:
    """Load parameters from config file.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments with a `config_path` attribute.
    """

    assert issubclass(args.model, ntem_constants.InputBase)
    return args.model.load_yaml(args.config_path)


def _create_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=__package__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=ntem.__doc__,
    )
    parser.add_argument(
        "-v",
        "--version",
        help="show caf.ntem version and exit",
        action="version",
        version=f"{__package__} {ctk.__version__}",
    )

    subparsers = parser.add_subparsers(
        title="caf NTEM sub-commands",
        description="List of all available sub-commands",
    )

    build_class = ctk.arguments.ModelArguments(build.BuildArgs)

    suffixes = ("", "")
    if build.check_dependencies():
        suffixes = (
            " - feature not installed.",
            " WARNING - dependencies required for this feature aren't installed,"
            " install the 'build_db' optional dependencies (caf.base[build_db]).",
        )

    build_class.add_subcommands(
        subparsers,
        "build",
        help="Build an SQLite database from NTEM MS Access files" + suffixes[0],
        description="Create an SQLite database at the path specified "
        "from specified NTEM MS Access files." + suffixes[1],
        formatter_class=ctk.arguments.TidyUsageArgumentDefaultsHelpFormatter,
    )

    query_parser = subparsers.add_parser(
        "query",
        help="Query the NTEM dataset",
        description="Query the NTEM database to get planning, car ownership or trip end data,"
        " allows filtering / aggregating each dataset to specific areas, purposes or modes.",
        formatter_class=ctk.arguments.TidyUsageArgumentDefaultsHelpFormatter,
    )

    query_args = ctk.arguments.ModelArguments(inputs.QueryArgs)
    query_args.add_config_arguments(query_parser)

    return parser


def _parse_args() -> ntem_constants.InputBase:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=ctk.arguments.TypeAnnotationWarning)
        parser = _create_arg_parser()
    args = parser.parse_args(None if len(sys.argv[1:]) > 0 else ["-h"])
    try:
        return args.dataclass_parse_func(args)
    except (pydantic.ValidationError, FileNotFoundError) as exc:
        if _TRACEBACK:
            raise
        # Switch to raising SystemExit as this doesn't include traceback
        raise SystemExit(str(exc)) from exc


def main():
    """Run the caf ntem module."""
    parameters = _parse_args()

    details = ctk.ToolDetails(
        __package__,
        ntem.__version__,  # ntem.__homepage__, ntem.__source_url__
    )
    with ctk.LogHelper(
        __package__, details, console=False, log_file=parameters.logging_path
    ) as log:
        # accessing protected attribute is bad, but we have to so we can set the logging level
        tqdm_log.logging_redirect_tqdm(
            [log.logger, log._warning_logger]  # pylint: disable="protected-access"
        )
        if _LOG_LEVEL.lower() == "debug":
            log.add_console_handler(log_level=logging.DEBUG)
        elif _LOG_LEVEL.lower() == "info":
            log.add_console_handler(log_level=logging.INFO)
        else:
            raise NotImplementedError(
                "The Environment constant 'LOG_LEVEL' should"
                " either be set to 'debug' or 'info"
            )

        try:
            parameters.run()

        except (pydantic.ValidationError, FileNotFoundError) as exc:
            if _TRACEBACK:
                raise
            # Switch to raising SystemExit as this doesn't include traceback
            raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
