import argparse
import pathlib
import sys

import caf.toolkit as ctk
import pydantic
import caf.ntem as ntem


_TRACEBACK = ctk.arguments.getenv_bool("NTEM_TRACEBACK", False)


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


def parse_args() -> ntem.ntem_constants.InputBase:
    parser = _create_arg_parser()
    args = parser.parse_args(None if len(sys.argv[1:]) > 0 else ["-h"])
    return args.dataclass_parse_func(args)


def main():
    parameters = parse_args()

    log_file = parameters.output_path / "caf_ntem.log"
    details = ctk.ToolDetails(
        __package__, ntem.__version__, #ntem.__homepage__, ntem.__source_url__
    )

    with ctk.LogHelper(__package__, details, log_file=log_file):
        parameters.run()


if __name__ == "__main__":
    main()
