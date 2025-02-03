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
INVALID_ZONE_ID = 9999


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
    id_sub: dict[int, int],
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
            id_sub,
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
            id_sub,
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
            id_sub,
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
            id_sub,
        )


def _process_ntem_access_file(
    connection: sqlalchemy.Connection,
    out_table: type[ntem.db_structure.Base],
    path: pathlib.Path,
    access_table_name: str,
    metadata_id: int,
    id_columns: list[str],
    rename_cols: dict[str, str],
    id_substitution: dict[int, int],
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

    data = data[data["zone_id"] != INVALID_ZONE_ID]

    id_columns = ["metadata_id", "zone_type_id"] + id_columns

    data = data.rename(columns=rename_cols).melt(
        id_columns,
        var_name="year",
        value_name="value",
    )
    data["zone_id"] = data["zone_id"].replace(id_substitution)

    LOG.debug("Writing data to database")
    data.to_sql(out_table.__tablename__, connection, if_exists="append", index=False)


def build_db(
    dir: pathlib.Path,
    output_dir: pathlib.Path,
    scenarios: Iterable[ntem.ntem_constants.Scenarios] | None = None,
):
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
    data_paths, lookup_path = _sort_files(dir.glob("*.mdb"), scenarios)

    LOG.info("Created database tables")
    output_engine = sqlalchemy.create_engine(ntem.db_structure.connection_string(output_path))

    if _CLEAN_DATABASE:
        ntem.db_structure.Base.metadata.drop_all(output_engine)

    ntem.db_structure.Base.metadata.create_all(output_engine, checkfirst=False)

    with orm.Session(output_engine) as session:

        LOG.info("Creating Lookup Tables")
        create_lookup_tables(session.connection(), lookup_path)
        ntem_to_db_conversion = create_geo_lookup_table(session, lookup_path, "NTEM", "8.0")
        LOG.info("Created Lookup Tables")
        session.commit()

        for label, paths in data_paths.items():
            LOG.info(f"Processing {label.scenario.value} - Version:{label.version}")
            metadata = ntem.db_structure.MetaData(
                scenario=label.scenario.value, version=label.version, share_type_id=1
            )
            session.add(metadata)
            # We need to flush so we can access the metadata id below
            session.flush()

            LOG.info("Added metadata scenario and version to metadata table")
            process_scenario(
                session.connection(), label, metadata.id, paths, ntem_to_db_conversion
            )
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
            ntem.db_structure.DB_TO_ACCESS_TABLE_LOOKUP[table.__tablename__],
            ntem.db_structure.ACCESS_TO_DB_COLUMNS[table.__tablename__],
        )
        lookup.to_sql(table.__tablename__, connection, if_exists="append", index=False)


def create_geo_lookup_table(
    session: orm.Session, lookup_path: pathlib.Path, source: str, version: str
) -> pd.DataFrame:
    """Creates and inserts geo lookup tables using the access data.

    Parameters
    ----------
    session : orm.Session
        Session to write geo-lookup tables to.
    lookup_path : pathlib.Path
        Path to lookup Access file.
    source : str
        Name of the source.
    version : str
        Version of the source.

    Returns
    -------
    pd.DataFrame
        lookup between NTEM zone ids and the IDs in the database
    """
    # add zone types so we can access IDs later
    zone_type = ntem.db_structure.ZoneType(name="zone", source=source, version=version)
    session.add(zone_type)

    authority_type = ntem.db_structure.ZoneType(
        name="authority", source=source, version=version
    )
    session.add(authority_type)

    county_type = ntem.db_structure.ZoneType(name="county", source=source, version=version)
    session.add(county_type)

    region_type = ntem.db_structure.ZoneType(name="region", source=source, version=version)
    session.add(region_type)

    session.flush()

    zones_id_lookup = _process_geo_lookup_data(
        "ntem_zoning", zone_type.id, lookup_path, session
    )

    system_id_lookup: dict[str, int] = {
        "region": region_type.id,
        "county": county_type.id,
        "authority": authority_type.id,
    }

    # lookup data will be used to create the geolookup table
    lookup_data = access_to_df(
        lookup_path,
        ntem.db_structure.DB_TO_ACCESS_TABLE_LOOKUP["ntem_zoning"],
        ntem.db_structure.ACCESS_TO_DB_COLUMNS["ntem_zoning"],
    )
    lookup_data["ntem_zoning_id"] = lookup_data["ntem_zoning_id"].replace(zones_id_lookup)
    lookup_data = lookup_data.rename(columns={"ntem_zoning_id": "from_zone_id"})
    lookup_data["from_zone_type_id"] = zone_type.id

    for system, id in system_id_lookup.items():
        id_lookup = _process_geo_lookup_data(system, id, lookup_path, session)
        system_col = f"{system}_id"

        system_lookup = lookup_data.rename(columns={system_col: "to_zone_id"})
        system_lookup["to_zone_id"] = system_lookup["to_zone_id"].replace(id_lookup)
        system_lookup["to_zone_type_id"] = id
        system_lookup = system_lookup[
            ["from_zone_id", "from_zone_type_id", "to_zone_id", "to_zone_type_id"]
        ]

        system_lookup.to_sql(
            ntem.db_structure.GeoLookup.__tablename__,
            session.connection(),
            if_exists="replace",
            index=False,
        )

    return zones_id_lookup


def _process_geo_lookup_data(
    system: str, system_id: int, lookup_path: pathlib.Path, session: orm.Session
) -> dict[int, int]:
    """reads zoning lookups ands adds data to Zones table. Returns NTEM -> db conversion."""
    # need to pass the session since we query data immediately after writing so we need to flush
    system_data = access_to_df(
        lookup_path,
        ntem.db_structure.DB_TO_ACCESS_TABLE_LOOKUP[system],
        ntem.db_structure.ACCESS_TO_DB_COLUMNS[system],
    )
    system_data["zone_type_id"] = system_id

    if "source_id_or_code" in system_data.columns:
        write_columns = ["zone_type_id", "name", "source_id_or_code"]
        join_col = "source_id_or_code"
    else:
        write_columns = ["zone_type_id", "name"]
        join_col = "name"

    system_data[write_columns].to_sql(
        ntem.db_structure.Zones.__tablename__,
        session.connection(),
        if_exists="append",
        index=False,
    )

    session.flush()

    id_lookup = pd.read_sql(
        sqlalchemy.select(ntem.db_structure.Zones).where(
            ntem.db_structure.Zones.zone_type_id == system_id
        ),
        session.connection(),
    )
    id_lookup = id_lookup.merge(
        system_data[["ntem_zoning_id", join_col]],
        how="left",
        on=join_col,
        validate="one_to_one",
    )

    id_lookup = id_lookup.rename(columns={"ntem_zoning_id": f"{system}_id"})

    return id_lookup.set_index(f"{system}_id")["id"].to_dict()


def _sort_files(
    files: Iterable[pathlib.Path],
    run_scenarios: Iterable[ntem.ntem_constants.Scenarios] | None = None,
) -> tuple[dict[FileType, list[pathlib.Path]], pathlib.Path]:
    """Sorts the files based on the scenario."""
    sorted_files = collections.defaultdict(lambda: [])
    if run_scenarios is None:
        run_scenarios = ntem.ntem_constants.Scenarios.__members__.values()
    for file in files:
        for scenario in run_scenarios:
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
