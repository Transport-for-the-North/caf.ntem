"""Build an NTEM database to be used by the query module for extracting data."""

from __future__ import annotations

# Built-Ins
import collections
import enum
import logging
import pathlib
import re
import sqlite3
from typing import Iterable, NamedTuple, Optional

# Third Party
import caf.toolkit as ctk
import pandas as pd
import pydantic
import sqlalchemy
import sqlalchemy.connectors
import tqdm
from sqlalchemy import orm

# Local Imports
from caf.ntem import ntem_constants, structure

_CLEAN_DATABASE = ctk.arguments.getenv_bool("NTEM_CLEAN_DATABASE", False)
INVALID_ZONE_ID = 9999


LOG = logging.getLogger(__name__)

ACCESS_CONNECTION_STRING = (
    "access+pyodbc:///?odbc_connect=DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}"
)

METADATA_ID_COLUMN = ntem_constants.BuildColumnNames.METADATA_ID.value
"""Name of the metadata ID column in the database."""
ZONE_SYSTEM_ID_COLUMN = ntem_constants.BuildColumnNames.ZONE_SYSTEM_ID.value
"""Name of the zone system ID column in the database."""
ZONE_ID_COLUMN = ntem_constants.BuildColumnNames.ZONE_ID.value
"""Name of the zone ID column in the database."""


def check_dependencies() -> bool:
    """Check if the dependencies are installed.

    Returns
    -------
    bool
        True if the dependencies are installed, False otherwise.
    """
    try:
        # Third Party
        import sqlalchemy_access  # pylint: disable=unused-import, import-outside-toplevel

        return True
    except (ImportError, ModuleNotFoundError) as exc:
        LOG.debug("Error importing sqlalchemy-access: %s", exc)
        return False


class AccessTables(enum.Enum):
    """Defines the names of the access data tables."""

    PLANNING = "Planning"
    CAR_OWNERSHIP = "CarOwnership"
    TE_CAR_AVAILABILITY = "TripEndDataByCarAvailability"
    TE_DIRECTION = "TripEndDataByDirection"

    @property
    def id_columns(self) -> list[str]:
        """The ID columns of the table."""
        id_cols = {
            AccessTables.PLANNING: [ZONE_ID_COLUMN, "planning_data_type"],
            AccessTables.CAR_OWNERSHIP: [ZONE_ID_COLUMN, "car_ownership_type"],
            AccessTables.TE_CAR_AVAILABILITY: [
                ZONE_ID_COLUMN,
                "purpose",
                "mode",
                "car_availability_type",
            ],
            AccessTables.TE_DIRECTION: [
                ZONE_ID_COLUMN,
                "purpose",
                "mode",
                "time_period",
                "trip_type",
            ],
        }
        return id_cols[self]

    @property
    def replace_columns(self) -> dict[str, str]:
        """The names columns to replace in the table and the substitution to use."""
        replace_cols = {
            AccessTables.CAR_OWNERSHIP: {
                "ZoneID": ZONE_ID_COLUMN,
                "CarOwnershipType": "car_ownership_type",
            },
            AccessTables.PLANNING: {
                "ZoneID": ZONE_ID_COLUMN,
                "PlanningDataType": "planning_data_type",
            },
            AccessTables.TE_CAR_AVAILABILITY: {
                "ZoneID": ZONE_ID_COLUMN,
                "Purpose": "purpose",
                "Mode": "mode",
                "CarAvailability": "car_availability_type",
            },
            AccessTables.TE_DIRECTION: {
                "ZoneID": ZONE_ID_COLUMN,
                "Purpose": "purpose",
                "Mode": "mode",
                "TimePeriod": "time_period",
                "TripType": "trip_type",
            },
        }
        return replace_cols[self]


@sqlalchemy.event.listens_for(sqlalchemy.Engine, "connect")
def set_sqlite_pragma(dbapi_connection, _):
    """Set the foreign key pragma for SQLite."""
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class FileType(NamedTuple):
    """A named tuple for storing the scenario and version of a file."""

    scenario: ntem_constants.Scenarios
    """The scenario of the file."""
    version: str
    """The version of the file."""


class BuildArgs(ntem_constants.InputBase):
    """Input arguments for the build command."""

    output_path: pathlib.Path = pydantic.Field(description="Path to the output directory.")
    """Path to directory to output SQLite database file."""
    directory: pydantic.DirectoryPath = pydantic.Field(
        description="Directory containing NTEM MS Access files."
    )
    """Directory containing NTEM MS Access files"""

    scenarios: Optional[list[ntem_constants.Scenarios]] = pydantic.Field(
        default=None,
        description="Scenarios to input into the database, valid scenarios are: "
        + ", ".join(i.value for i in ntem_constants.Scenarios),
    )
    """Scenarios to port into the database"""

    def run(self):
        """Run the build functionality using the args defined."""
        build_db(self.directory, self.output_path, self.scenarios)

    @property
    def logging_path(self) -> pathlib.Path:
        """Path to log file for the module."""
        return self.output_path / "caf_ntem.log"


def _access_to_df(
    path: pathlib.Path, table_name: str, substitute: dict[str, str] | None = None
) -> pd.DataFrame:
    """Access a table in the database and returns it as a pandas DataFrame.

    Parameters
    ----------
    path: pathlib.Path
        Path to the Access file to unpack.
    table_name : str
        The name of the table to unpack.
    substitute : dict[str, str]|None
        A dictionary to substitute column names. If a column name is not in the dictionary,
        it is removed from the DataFrame. If None, no substitutions are made.

    Returns
    -------
    pd.DataFrame
        The entire table as a pandas DataFrame.
    """
    engine = sqlalchemy.create_engine(ACCESS_CONNECTION_STRING.format(path.resolve()))

    df = pd.read_sql(table_name, engine)
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

    Parameters
    ----------
    connection : sqlalchemy.Connection
        The connection to the database to insert into.
    label : FileType
        The scenario and version of the data.
    metadata_id : int
        The id of the metadata for the data to insert.
    paths : list[pathlib.Path]
        The paths to the data to unpack and insert. These should point to the
        NTEM access database files for each region that fall under
        the same metadata ID (i.e. same scenario and version).
    id_sub: dict[int, int]
        Dictionary to map NTEM zone IDs to database IDs.
        This is used to replace the zone IDs in the data with the database IDs.
    """

    for path in tqdm.tqdm(
        paths, desc=f"Processing: {label.scenario.value} - Version:{label.version}"
    ):
        for access_table, output_table in {
            AccessTables.PLANNING: structure.Planning,
            AccessTables.CAR_OWNERSHIP: structure.CarOwnership,
            AccessTables.TE_CAR_AVAILABILITY: structure.TripEndDataByCarAvailability,
            AccessTables.TE_DIRECTION: structure.TripEndDataByDirection,
        }.items():
            LOG.debug("Processing %s", access_table.value)
            _process_ntem_access_file(
                connection,
                output_table,
                path,
                access_table_name=access_table.value,
                metadata_id=metadata_id,
                id_columns=access_table.id_columns,
                rename_cols=access_table.replace_columns,
                id_substitution=id_sub,
            )


def _process_ntem_access_file(
    connection: sqlalchemy.Connection,
    out_table: type[structure.Base],
    path: pathlib.Path,
    *,
    access_table_name: str,
    metadata_id: int,
    id_columns: list[str],
    rename_cols: dict[str, str],
    id_substitution: dict[int, int],
) -> None:
    """Read, format and insert data from the access file path and table given.

    Parameters
    ----------
    connection : sqlalchemy.Connection
        The connection to the database to insert into.
    out_table : type[structure.Base]
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
    id_substitution: dict[int, int]
        Dictionary to map NTEM zone IDs to database IDs.
        This is used to replace the zone IDs in the data with the database IDs.
    """
    LOG.debug("Reading access data")
    data = _access_to_df(path, access_table_name).rename(columns=rename_cols)
    LOG.debug("Processing data")
    # Adjust so the column names match the database structure
    data[METADATA_ID_COLUMN] = metadata_id
    data[ZONE_SYSTEM_ID_COLUMN] = 1

    data = data[data[ZONE_ID_COLUMN] != INVALID_ZONE_ID]

    id_columns = [METADATA_ID_COLUMN, ZONE_SYSTEM_ID_COLUMN] + id_columns

    data = data.melt(
        id_columns,
        var_name="year",
        value_name="value",
    )
    data["zone_id"] = data["zone_id"].replace(id_substitution)

    LOG.debug("Writing data to database")
    data.to_sql(out_table.__tablename__, connection, if_exists="append", index=False)


def build_db(
    access_dir: pathlib.Path,
    output_dir: pathlib.Path,
    scenarios: Iterable[ntem_constants.Scenarios] | None = None,
):
    """Process the NTEM data from the access files and outputs a SQLite database.

    Parameters
    ----------
    dir : pathlib.Path
        The directory containing the access files.
    output_dir : pathlib.Path
        The path to the directory to output the SQLite database.
    """
    if not output_dir.is_dir():
        raise NotADirectoryError(output_dir.resolve())
    output_path = output_dir / "NTEM.sqlite"

    LOG.info("Retrieving and sorted file paths")
    data_paths, lookup_path = _sort_files(access_dir.glob("*.mdb"), scenarios)

    LOG.info("Created database tables")
    output_engine = sqlalchemy.create_engine(structure.connection_string(output_path))

    if _CLEAN_DATABASE:
        confirm = input("Cleaning NTEM data from database, are you sure? Y/N ")
        if confirm.lower().strip() in ("y", "yes"):
            structure.Base.metadata.drop_all(output_engine)

    structure.Base.metadata.create_all(output_engine, checkfirst=True)

    with orm.Session(output_engine) as session:
        LOG.info("Creating Lookup Tables")
        create_lookup_tables(session.connection(), lookup_path)
        ntem_to_db_conversion = create_geo_lookup_table(session, lookup_path, "NTEM", "8.0")
        LOG.info("Created Lookup Tables")
        session.commit()

        for label, paths in data_paths.items():
            LOG.info("Processing %s - Version:%s", label.scenario.value, label.version)
            # TODO(kf): Once we start retrieving IDs from DB in queries module change metadata
            # back to autoincremented ids.
            metadata_id = ntem_constants.Scenarios(label.scenario.value).id(
                ntem_constants.Versions(label.version)
            )
            metadata = structure.MetaData(
                id=metadata_id,
                scenario=label.scenario.value,
                version=label.version,
                share_type_id=1,
            )
            session.add(metadata)
            # We need to flush so we can access the metadata id below
            session.flush()
            session.commit()

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

    for table in tqdm.tqdm(structure.LOOKUP_TABLES, desc="Creating Lookup Tables"):
        if structure.DB_TO_ACCESS_TABLE_LOOKUP[table.__tablename__] == "NtemTripTypeLookup":
            lookup = structure.NtemTripTypeLookup().to_dataframe()
            lookup.to_sql(table.__tablename__, connection, if_exists="append", index=False)

        else:
            lookup = _access_to_df(
                lookup_path,
                structure.DB_TO_ACCESS_TABLE_LOOKUP[table.__tablename__],
                structure.ACCESS_TO_DB_COLUMNS[table.__tablename__],
            )
            lookup.to_sql(table.__tablename__, connection, if_exists="append", index=False)


def create_geo_lookup_table(
    session: orm.Session, lookup_path: pathlib.Path, source: str, version: str
) -> pd.DataFrame:
    """Create and insert geo lookup tables using the access data.

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
    # TODO(kf): Update function to handle:
    # - zone systems already exist in the database
    # - user defined names for any new zone systems being created
    # - user defined ids for existing zone systems when creating lookup

    # add zone types so we can access IDs later
    zone_type = structure.ZoneType(name="zone", source=source, version=version)
    session.add(zone_type)

    authority_type = structure.ZoneType(name="authority", source=source, version=version)
    session.add(authority_type)

    county_type = structure.ZoneType(name="county", source=source, version=version)
    session.add(county_type)

    region_type = structure.ZoneType(name="region", source=source, version=version)
    session.add(region_type)

    session.flush()
    session.expunge_all()

    zones_id_lookup = _process_geo_lookup_data(
        "ntem_zoning", zone_type.id, lookup_path, session.connection()
    )

    system_id_lookup: dict[str, int] = {
        "region": region_type.id,
        "county": county_type.id,
        "authority": authority_type.id,
    }

    # lookup data will be used to create the geolookup table
    lookup_data = _access_to_df(
        lookup_path,
        structure.DB_TO_ACCESS_TABLE_LOOKUP["ntem_zoning"],
        structure.ACCESS_TO_DB_COLUMNS["ntem_zoning"],
    )
    lookup_data["ntem_zoning_id"] = lookup_data["ntem_zoning_id"].replace(zones_id_lookup)
    lookup_data = lookup_data.rename(
        columns={"ntem_zoning_id": structure.GeoLookup.from_zone_id.name}
    )
    lookup_data[structure.GeoLookup.from_zone_type_id.name] = zone_type.id

    for system, id_ in system_id_lookup.items():
        id_lookup = _process_geo_lookup_data(system, id_, lookup_path, session.connection())
        session.flush()
        system_lookup = lookup_data.rename(
            columns={f"{system}_id": structure.GeoLookup.to_zone_id.name}
        )
        system_lookup[structure.GeoLookup.to_zone_id.name] = system_lookup[
            structure.GeoLookup.to_zone_id.name
        ].replace(id_lookup)
        system_lookup[structure.GeoLookup.to_zone_type_id.name] = id_
        system_lookup = system_lookup[
            [
                structure.GeoLookup.from_zone_id.name,
                structure.GeoLookup.from_zone_type_id.name,
                structure.GeoLookup.to_zone_id.name,
                structure.GeoLookup.to_zone_type_id.name,
            ]
        ]

        system_lookup.to_sql(
            structure.GeoLookup.__tablename__,
            session.connection(),
            if_exists="append",
            index=False,
        )

    return zones_id_lookup


def _process_geo_lookup_data(
    system: str,
    system_id: int,
    lookup_path: pathlib.Path,
    connection: sqlalchemy.Connection,
) -> dict[int, int]:
    """Read zoning lookups and add data to Zones table. Returns NTEM -> db conversion."""
    # need to pass the session since we query data immediately after writing so we need to flush
    max_id = connection.execute(sqlalchemy.func.max(structure.Zones.id)).scalar()
    if max_id is None:
        max_id = 0

    system_data = _access_to_df(
        lookup_path,
        structure.DB_TO_ACCESS_TABLE_LOOKUP[system],
        structure.ACCESS_TO_DB_COLUMNS[system],
    )
    system_data["zone_type_id"] = system_id

    if system_data["ntem_zoning_id"].min() == 0:
        system_data["ntem_zoning_id"] += 1

    system_data["id"] = system_data["ntem_zoning_id"] + max_id

    if "source_id_or_code" in system_data.columns:
        write_columns = [
            structure.Zones.id.name,
            structure.Zones.zone_type_id.name,
            structure.Zones.name.name,
            structure.Zones.source_id_or_code.name,
        ]

    else:
        write_columns = [
            structure.Zones.id.name,
            structure.Zones.zone_type_id.name,
            structure.Zones.name.name,
        ]

    system_data[write_columns].to_sql(
        structure.Zones.__tablename__,
        connection,
        if_exists="append",
        index=False,
    )

    id_lookup = system_data[["ntem_zoning_id", "id"]]
    return id_lookup.set_index("ntem_zoning_id")["id"].to_dict()


def _sort_files(
    files: Iterable[pathlib.Path],
    run_scenarios: Iterable[ntem_constants.Scenarios] | None = None,
) -> tuple[dict[FileType, list[pathlib.Path]], pathlib.Path]:
    """Sorts the files based on the scenario."""
    sorted_files = collections.defaultdict(lambda: [])
    lookup = None
    if run_scenarios is None:
        run_scenarios = ntem_constants.Scenarios.__members__.values()
    for file in files:
        for scenario in run_scenarios:
            if scenario.value.lower() in file.stem.lower():
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
            if lookup is not None:
                raise ValueError(
                    "Multiple lookup files found in the directory. Only one file can be labelled 'Lookup'."
                )
            lookup = file

    if lookup is None:
        raise FileNotFoundError(
            "No lookup file was found when scanning the provided directory."
        )

    return sorted_files, lookup
