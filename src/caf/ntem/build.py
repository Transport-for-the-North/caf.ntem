from __future__ import annotations

# Built-Ins
import collections
import logging
import pathlib
import re
from typing import Iterable, NamedTuple

# Third Party
import pandas as pd
import pydantic
import sqlalchemy
from sqlalchemy import orm
import caf.toolkit as ctk
import tqdm

import caf.ntem as ntem

LOG = logging.getLogger(__name__)

_CLEAN_DATABASE = ctk.arguments.getenv_bool("NTEM_CLEAN_DATABASE", False)


ACCESS_CONNCECTION_STRING = (
    "access+pyodbc:///?odbc_connect=DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}"
)



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


def access_to_df(path: pathlib.Path, table_name: str, substitute: dict[str, str]|None = None) -> pd.DataFrame:
    """Accesses a table in the database and returns it as a pandas DataFrame.

    Parameters
    ----------
    engine : sqlalchemy.Engine
        The engine to use to access the database.
    table_name : str
        The name of the table to access.
    substitute : dict[str, str]|None
        A dictionary to substitute column names. If a column name is not in the dictionary,
        it is removed from the DataFrame. If None, no substitutions are made.

    returns
    -------
    pd.DataFrame
        The entire table as a pandas DataFrame.
    """
    engine = sqlalchemy.create_engine(ACCESS_CONNCECTION_STRING.format(path.resolve()))
    query = f"SELECT * FROM {table_name}"

    df = pd.read_sql(query, engine)
    if substitute is not None:
        try:
            df =  df.rename(columns=substitute).loc[:, substitute.values()]
        except KeyError as e:
            raise KeyError(f"Could not find columns {substitute.values()} in {table_name}.") from e
    return df


def process_scenario(
    session: orm.Session,
    label: FileType,
    metadata_id: int,
    paths: list[pathlib.Path],
):
    """Processes a scenario."""

    for path in tqdm.tqdm(paths, desc=F"Processing: {label.scenario.value} - Version:{label.version}"):

        planning: pd.DataFrame = process_planning_data(session, path,  metadata_id)




def process_planning_data(session: orm.Session, path: pathlib.Path, metadata_id:int) -> pd.DataFrame:
    """Processes the planning data."""
    planning = access_to_df(path, ntem.ntem_constants.AccessTables.PLANNING.value)
    planning["metadata_id"] = metadata_id
    planning = planning.rename(columns={"ZoneID": "zone_id", "Purpose": "purpose_id", "Mode": "mode_id", "CarAvailibility": "car_availability_id"})

    


def process_data(dir: pathlib.Path, output_path: pathlib.Path):
    """Processes the data."""

    data_paths, lookup_path = sort_files(dir.glob("*.mdb"))
    LOG.info("Retreived and sorted file paths")
    output_engine = sqlalchemy.create_engine(ntem.db_structure.connection_string(output_path))

    if _CLEAN_DATABASE:
        ntem.db_structure.Base.metadata.drop_all(output_engine)
    
    ntem.db_structure.Base.metadata.create_all(output_engine, checkfirst=False)

    LOG.info("Created database tables")
    
    with orm.Session(output_engine) as session:
        LOG.info("Creating Lookup Tables")
        create_lookup_tables(session, lookup_path)
        LOG.info("Created Lookup Tables")
        for label, paths in data_paths.items():
            LOG.info(f"Processing {label.scenario.value} - Version:{label.version}")
            metadata = ntem.db_structure.MetaData(
                scenario=label.scenario.value, version=label.version, share_type_id=1
            )
            session.add(metadata)
            LOG.info("Added metadata scenario and version to metadata table")
            process_scenario(session, label, metadata.id, paths)
        
        session.commit()


def create_lookup_tables(session: orm.Session, lookup_path: pathlib.Path):
    """Creates the lookup tables."""

    for table in tqdm.tqdm(ntem.db_structure.LOOKUP_TABLES, desc="Creating Lookup Tables"):

        lookup = access_to_df(
            lookup_path,
            ntem.db_structure.DB_TO_ACCESS_TABLE_LOOKUP[table],
            ntem.db_structure.ACCESS_TO_DB_COLUMNS[table],
        )
        # Some tables we dont want all the columns
            
        session.execute(sqlalchemy.insert(table), lookup.to_dict(orient="records"))



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
                    raise ValueError(
                        f"Could not find version in {file.stem} when matching for _[0-9][0-9]_."
                    )
                sorted_files[
                    FileType(scenario, f"{version_digits.group(1)}.{version_digits.group(2)}")
                ].append(file)

                break
        if "Lookup" in file.stem:
            lookup = file

    return sorted_files, lookup


def build_db(dir: pathlib.Path, out_path: pathlib.Path):

    process_data(dir, out_path / "Nice_NTEM.db")
