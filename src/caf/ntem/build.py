from __future__ import annotations

# Built-Ins
import collections
import pathlib
import re
from typing import Iterable, NamedTuple

# Third Party
import pandas as pd
import pydantic
import sqlalchemy

import caf.ntem as ntem


class FileType(NamedTuple):
    """A named tuple for storing the scenario and version of a file."""

    scenario: ntem.ntem_constants.Scenarios
    version: str


class BuildArgs(ntem.ntem_constants.InputBase):
    directory: pydantic.DirectoryPath = pydantic.Field(
        description="Directory containing NTEM MS Access files"
    )
    output_path: pydantic.DirectoryPath = pydantic.Field(
        description="Path to directory to output SQLite database file"
    )

    def run(self):
        build_db(self.directory, self.output_path)


def access_to_df(engine: sqlalchemy.Engine, table_name: str) -> pd.DataFrame:
    """Accesses a table in the database and returns it as a pandas DataFrame.

    Parameters
    ----------
    engine : sqlalchemy.Engine
        The engine to use to access the database.
    table_name : str
        The name of the table to access.

    returns
    -------
    pd.DataFrame
        The entire table as a pandas DataFrame.
    """
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)


def process_scenario(output_path, label: FileType, paths: list[pathlib.Path]):
    """Processes a scenario."""


    for path in paths:
        url = f"access+pyodbc:///?odbc_connect=DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={path.resolve()}"
        access_engine = sqlalchemy.create_engine(url)

        planning = access_to_df(access_engine, ntem.ntem_constants.AccessTables.PLANNING.value)
        # connect
        # insert
        # del df


def process_data(dir: pathlib.Path, output_path: pathlib.Path, scenario: str):
    """Processes the data."""

    scenario_paths, lookup_path = sort_files(dir.glob("*.mdb"))

    sql_engine = sqlalchemy.create_engine(ntem.db_structure.connection_string(output_path))

    


    for label, paths in scenario_paths.items():
        process_scenario(output_path,label, paths)

        # connect
        # insert
        # del planning_data
    print("staop")
    # return df




def sort_files(
    files: Iterable[pathlib.Path],
) -> tuple[dict[FileType, list[pathlib.Path]], pathlib.Path]:
    """Sorts the files based on the scenario."""
    sorted_files = collections.defaultdict(lambda: [])
    for file in files:
        for scenario in ntem.ntem_constants.Scenarios.__members__.values():
            if scenario.value in file.stem:
                version_digits = re.search(r"_(\d)(\d)_", file.stem)
                if version_digits is None:
                    raise ValueError(f"Could not find version in {file.stem} when matching for _[0-9][0-9]_.")
                sorted_files[
                    FileType(scenario, f"{version_digits.group(0)}.{version_digits.group(1)}")
                ].append(file)

                break
        if "Lookup" in file.stem:
            lookup = file

    return sorted_files, lookup


def build_db(dir: pathlib.Path, out_path: pathlib.Path):

    process_data(dir, out_path/"Nice_NTEM.db", "Core")
