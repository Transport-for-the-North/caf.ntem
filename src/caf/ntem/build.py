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


_CLEAN_DATABASE = ctk.arguments.getenv_bool("NTEM_CLEAN_DATABASE", False)

LOG = logging.getLogger(__name__)

ACCESS_CONNCECTION_STRING = (
    "access+pyodbc:///?odbc_connect=DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}"
)


class FileType(NamedTuple):
    """A named tuple for storing the scenario and version of a file."""

    scenario: ntem.ntem_constants.Scenarios
    """The scenario of the file."""
    version: str
    """The version of the file."""


class BuildArgs(ntem.ntem_constants.InputBase):
    """Input areguments for the build command."""

    directory: pydantic.DirectoryPath = pydantic.Field(
        description="Directory containing NTEM MS Access files."
    )
    """Directory containing NTEM MS Access files"""
    output_path: pydantic.DirectoryPath = pydantic.Field(
        description="Path to directory to output SQLite database file"
    )
    """Path to directory to output SQLite database file."""

    def run(self):
        """Run the build functionality using the args defined."""
        build_db(self.directory, self.output_path)


def access_to_df(
    path: pathlib.Path, table_name: str, substitute: dict[str, str] | None = None
) -> pd.DataFrame:
    """Accesses a table in the database and returns it as a pandas DataFrame.

    Parameters
    ----------
    path: pathlib.Path
        Path to the Access file to unpack.
    table_name : str
        The name of the table to unpack.
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
    """Process data for a scenario and version and insert in into the database.

    Paramerters
    -----------
    connection : sqlalchemy.Connection
        The connection to the database to insert into.
    label : FileType
        The scenario and version of the data.
    metadata_id : int
        The id of the metadata for the data to insert.
    paths : list[pathlib.Path]
        The paths to the data to unpack and insert.
    """

    for path in tqdm.tqdm(
        paths, desc=f"Processing: {label.scenario.value} - Version:{label.version}"
    ):

        LOG.debug("Proccessing Planning Data")
        _process_ntem_access_file(
            connection,
            ntem.db_structure.Planning,
            path,
            ntem.ntem_constants.AccessTables.PLANNING.value,
            metadata_id,
            ["zone_id", "planning_data_type"],
            {"ZoneID": "zone_id", "PlanningDataType": "planning_data_type"},
        )

        LOG.debug(msg="Proccessing Car Ownership Data")
        _process_ntem_access_file(
            connection,
            ntem.db_structure.CarOwnership,
            path,
            ntem.ntem_constants.AccessTables.CAR_OWNERSHIP.value,
            metadata_id,
            ["zone_id", "car_ownership_type"],
            {"ZoneID": "zone_id", "CarOwnershipType": "car_ownership_type"},
        )

        LOG.debug("Proccessing TE Car Availability Data")
        _process_ntem_access_file(
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
        _process_ntem_access_file(
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


def _process_ntem_access_file(
    connection: sqlalchemy.Connection,
    out_table: type[ntem.db_structure.Base],
    path: pathlib.Path,
    access_table_name: str,
    metadata_id: int,
    id_columns: list[str],
    rename_cols: dict[str, str],
) -> None:
    """Reads, formats and inserts data from the access file path and table given.

    Parameters
    ----------
    connection : sqlalchemy.Connection
        The connection to the database to insert into.
    out_table : type[ntem.db_structure.Base]
        The table to insert the data into.
    path : pathlib.Path
        The path to the access file to unpack and insert into the database.
    access_table_name : str
        The name of the table in the access file to unpack.
    metadata_id : int
        The id of the metadata for the data to insert.
    id_columns : list[str]
        The ID columns of the data in the table. Note: if the column has been renamed, use the new name.
    rename_cols : dict[str, str]
        One to one map between column name in the table and the name to replace it.
    """
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
    data.to_sql(out_table.__tablename__, connection, if_exists="append", index=False)


def build_db(dir: pathlib.Path, output_dir: pathlib.Path):
    """Processes the NTEM data from the access files and outputs a SQLite database.

    Parameters
    ----------
    dir : pathlib.Path
        The directory containing the access files.
    output_dir : pathlib.Path
        The path to the directory to output the SQLite database.
    """
    output_path = output_dir / "Nice_NTEM.db"

    LOG.info("Retreiving and sorted file paths")
    data_paths, lookup_path = _sort_files(dir.glob("*.mdb"))

    LOG.info("Created database tables")
    output_engine = sqlalchemy.create_engine(ntem.db_structure.connection_string(output_path))

    if _CLEAN_DATABASE:
        ntem.db_structure.Base.metadata.drop_all(output_engine)

    ntem.db_structure.Base.metadata.create_all(output_engine, checkfirst=False)

    with sqlalchemy.Connection(output_engine) as connection:
        LOG.info("Creating Lookup Tables")
        create_lookup_tables(connection, lookup_path)
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
    """Insert lookup tables into the database.

    Parameters
    ----------
    connection : sqlalchemy.Connection
        The connection to the database to insert into.
    lookup_path : pathlib.Path
        The path to the access file containing the lookup tables.
    """

    for table in tqdm.tqdm(ntem.db_structure.LOOKUP_TABLES, desc="Creating Lookup Tables"):

        lookup = access_to_df(
            lookup_path,
            ntem.db_structure.DB_TO_ACCESS_TABLE_LOOKUP[table],
            ntem.db_structure.ACCESS_TO_DB_COLUMNS[table],
        )
        lookup.to_sql(table.__tablename__, connection, if_exists="append", index=False)


def _sort_files(
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
