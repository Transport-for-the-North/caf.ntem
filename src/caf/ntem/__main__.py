# Built-Ins
import argparse
import logging
import os
import sys

# Third Party
import caf.toolkit as ctk
import pydantic

# Local Imports
import caf.ntem as ntem

_TRACEBACK = ctk.arguments.getenv_bool("NTEM_TRACEBACK", False)
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


def _create_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=__package__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=ntem.__doc__,
    )
    parser.add_argument(
        "-v",
        "--version",
        help="show caf.toolkit version and exit",
        action="version",
        version=f"{__package__} {ctk.__version__}",
    )

    subparsers = parser.add_subparsers(
        title="caf NTEM sub-commands",
        description="List of all available sub-commands",
    )

    translation_class = ctk.arguments.ModelArguments(ntem.build.BuildArgs)
    translation_class.add_subcommands(
        subparsers,
        "build",
        help="Build an SQLite database from NTEM MS Access files",
        description="Create an SQLite database at the path specified "
        "from specified NTEM MS Access files.",
        formatter_class=ctk.arguments.TidyUsageArgumentDefaultsHelpFormatter,
    )
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

    log_file = parameters.output_path / "caf_ntem.log"
    details = ctk.ToolDetails(
        __package__,
        ntem.__version__,  # ntem.__homepage__, ntem.__source_url__
    )
    with ctk.LogHelper(__package__, details, console=False, log_file=log_file) as log:
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
