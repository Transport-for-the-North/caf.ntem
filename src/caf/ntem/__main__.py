import argparse
import sys

import caf.toolkit as ctk
import caf.ntem as ntem


class BuildArgs(ctk.BaseConfig):
    directory: str
    output_path: str

    def run(self):
        ntem.build(self.directory, self.output_path)


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

    translation_class = ctk.arguments.ModelArguments(BuildArgs)
    translation_class.add_subcommands(
        subparsers,
        "build",
        help="Build an SQLite database from NTEM MS Access files",
        description="Create an SQLite database at the path specified "
        "from specified NTEM MS Access files.",
        formatter_class=ctk.arguments.TidyUsageArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        help="Directory containing the ntem access files",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_path",
        type=str,
        help="path to output sqlite database",
        required=True,
    )
    return parser


def parse_args() -> BuildArgs:
    parser = _create_arg_parser()
    args = parser.parse_args(None if len(sys.argv[1:]) > 0 else ["-h"])
    return args.dataclass_parse_func(args)


def main():
    parameters = parse_args()

    parameters.run()


if __name__ == "__main__":
    main()
