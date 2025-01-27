from __future__ import annotations

# Built-Ins
import collections
import logging
import pathlib
import re
from typing import Iterable, NamedTuple

# Third Party
import caf.toolkit as ctk
import pandas as pd
import pydantic
import sqlalchemy
import sqlalchemy.connectors
import tqdm
from sqlalchemy import orm

# Local Imports
import caf.ntem as ntem

LOG = logging.getLogger(__name__)

_CLEAN_DATABASE = ctk.arguments.getenv_bool("NTEM_CLEAN_DATABASE", False)


ACCESS_CONNCECTION_STRING = (
    "access+pyodbc:///?odbc_connect=DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}"
)

CHUNK_SIZE = 1e5

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


def access_to_df(
    path: pathlib.Path, table_name: str, substitute: dict[str, str] | None = None
) -> pd.DataFrame:
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
            df = df.rename(columns=substitute).loc[:, substitute.values()]
        except KeyError as e:
            raise KeyError(
                f"Could not find columns {substitute.values()} in {table_name}."
            ) from e
    return df


def process_scenario(
    connection: sqlalchemy.Connection, 
    label: FileType,
    metadata_id: int,
    paths: list[pathlib.Path],
):
    """Processes a scenario."""

    for path in tqdm.tqdm(
        paths, desc=f"Processing: {label.scenario.value} - Version:{label.version}"
    ):

        # TODO These functions do the samething on different columns, make them one function
        LOG.debug("Proccessing Planning Data")
        process_ntem_access_data(
            connection,
            ntem.db_structure.Planning,
            path,
            ntem.ntem_constants.AccessTables.PLANNING.value,
            metadata_id,
            ["zone_id", "planning_data_type"],
            {"ZoneID": "zone_id", "PlanningDataType": "planning_data_type"},
        )
        LOG.debug(msg="Proccessing Car Ownership Data")
        process_ntem_access_data(
            connection,
            ntem.db_structure.CarOwnership,
            path,
            ntem.ntem_constants.AccessTables.CAR_OWNERSHIP.value,
            metadata_id,
            ["zone_id", "car_ownership_type"],
            {"ZoneID": "zone_id", "CarOwnershipType": "car_ownership_type"},
        )
        LOG.debug("Proccessing TE Car Availability Data")
        process_ntem_access_data(
            connection,
            ntem.db_structure.TripEndDataByCarAvailability,
            path,
            ntem.ntem_constants.AccessTables.TE_CAR_AVAILABILITY.value,
            metadata_id,
            ["zone_id", "purpose", "mode", "car_availability_type"],
            {
                "ZoneID": "zone_id",
                "Purpose": "purpose",
                "Mode": "mode",
                "CarAvailability": "car_availability_type",
            },
        )
        LOG.debug("Proccessing TE Direction Data")
        process_ntem_access_data(
            connection,
            ntem.db_structure.TripEndDataByDirection,
            path,
            ntem.ntem_constants.AccessTables.TE_DIRECTION.value,
            metadata_id,
            ["zone_id", "purpose", "mode", "time_period", "trip_type"],
            {
                "ZoneID": "zone_id",
                "Purpose": "purpose",
                "Mode": "mode",
                "TimePeriod": "time_period",
                "TripType": "trip_type",
            },
        )



def process_ntem_access_data(
    connection: sqlalchemy.Connection,
    out_table: type[ntem.db_structure.Base],
    path: pathlib.Path,
    access_table_name: str,
    metadata_id: int,
    id_columns: list[str],
    rename_cols: dict[str, str],
) -> None:
    """Processes the planning data."""
    LOG.debug("Reading access data")
    data = access_to_df(path, access_table_name)
    LOG.debug("Processing data")
    # Adjust so the column names match the database structure
    data["metadata_id"] = metadata_id
    data["zone_type_id"] = 1

    id_columns = ["metadata_id", "zone_type_id"] + id_columns

    data = data.rename(columns=rename_cols).melt(
        id_columns,
        var_name="year",
        value_name="value",
    )
    
    LOG.debug("Writing data to database")
    data.to_sql(out_table.__tablename__,connection, if_exists="append", index=False)
    

def process_data(dir: pathlib.Path, output_path: pathlib.Path):
    """Processes the data."""

    data_paths, lookup_path = sort_files(dir.glob("*.mdb"))
    LOG.info("Retreived and sorted file paths")
    output_engine = sqlalchemy.create_engine(ntem.db_structure.connection_string(output_path))

    if _CLEAN_DATABASE:
        ntem.db_structure.Base.metadata.drop_all(output_engine)

    ntem.db_structure.Base.metadata.create_all(output_engine, checkfirst=False)

    LOG.info("Created database tables")

    with sqlalchemy.Connection(output_engine) as connection:
        LOG.info("Creating Lookup Tables")
        create_lookup_tables(connection.connection(), lookup_path)
        LOG.info("Created Lookup Tables")
        connection.commit()
    
    for label, paths in data_paths.items():
        with orm.Session(output_engine) as session:
            
            LOG.info(f"Processing {label.scenario.value} - Version:{label.version}")
            metadata = ntem.db_structure.MetaData(
                scenario=label.scenario.value, version=label.version, share_type_id=1
            )
            session.add(metadata)
            # We need to flush so we can access the metadata id below
            session.flush()
            

            LOG.info("Added metadata scenario and version to metadata table")
            process_scenario(session.connection(), label, metadata.id, paths)
            session.commit()


def create_lookup_tables(connection: sqlalchemy.Connection, lookup_path: pathlib.Path):
    """Creates the lookup tables."""

    for table in tqdm.tqdm(ntem.db_structure.LOOKUP_TABLES, desc="Creating Lookup Tables"):

        lookup = access_to_df(
            lookup_path,
            ntem.db_structure.DB_TO_ACCESS_TABLE_LOOKUP[table],
            ntem.db_structure.ACCESS_TO_DB_COLUMNS[table],
        )
        # Some tables we dont want all the columns

        lookup.to_sql(table.__tablename__, connection, if_exists="append", index=False)
        


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
