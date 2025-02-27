# Built-Ins
import argparse
import logging
import os
import pathlib
import sys
import tqdm.contrib.logging as tqdm_log

# Third Party
import caf.toolkit as ctk
import pydantic

# Local Imports
import caf.ntem as ntem
from caf.ntem import inputs, ntem_constants, queries

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

    build_class = ctk.arguments.ModelArguments(ntem.build.BuildArgs)
    build_class.add_subcommands(
        subparsers,
        "build",
        help="Build an SQLite database from NTEM MS Access files",
        description="Create an SQLite database at the path specified "
        "from specified NTEM MS Access files.",
        formatter_class=ctk.arguments.TidyUsageArgumentDefaultsHelpFormatter,
    )

    query_parser = subparsers.add_parser(
        "query",
        help="Query the NTEM dataset",
        description="Query the NTEM Database to get subset of data by region and year",
        formatter_class=ctk.arguments.TidyUsageArgumentDefaultsHelpFormatter,
    )

    query_parser.add_argument(
        "config_path",
        type=pathlib.Path,
        help="path to YAML config file containing run parameters",
    )

    query_parser.set_defaults(dataclass_parse_func=_config_parse, model=inputs.QueryArgs)

    return parser


def _parse_args() -> ntem.ntem_constants.InputBase:
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
        tqdm_log.logging_redirect_tqdm([log.logger, log._warning_logger])
        if _LOG_LEVEL.lower() == "debug":
            log.add_console_handler(log_level=logging.DEBUG)
        elif _LOG_LEVEL.lower() == "info":
            log.add_console_handler(log_level=logging.INFO)
        else:
            raise NotImplementedError(
                "The Enviroment constant 'LOG_LEVEL' should"
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
