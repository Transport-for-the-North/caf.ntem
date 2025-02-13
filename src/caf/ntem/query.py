from __future__ import annotations

# Built-Ins
import abc
import dataclasses
import pathlib

# Third Party
import pandas as pd
import pydantic
import sqlalchemy
from sqlalchemy import orm

# Local Imports
from caf.ntem import ntem_constants, structure


class QueryArgs(ntem_constants.InputBase):
    output_path: pathlib.Path = pydantic.Field(description="Path to the output directory.")
    """Path to directory to output the processed NTEM data."""
    db_path: pydantic.FilePath = pydantic.Field(description="Path to NTEM database.")
    """Path to NTEM database, which has been outputted by the build module."""
    planning_runs: list[PlanningParams] | None = None

    @property
    def logging_path(self) -> pathlib.Path:
        return self.output_path / "caf_ntem.log"

    def run(self) -> None:
        perform_query(self)


@dataclasses.dataclass
class QueryParams(abc.ABC):
    year: int
    """Years to produce outputs."""
    scenario: ntem_constants.Scenarios
    """Scenarios to produce outputs"""
    filter_zoning_system: ntem_constants.ZoningSystems
    """The zoning system to use when filtering data down"""
    filter_zone_names: list[str]
    """Zones to select from the data, must be in the names column of the zoning zones table."""
    output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE
    """Zoning system to output the data in."""
    version: ntem_constants.Versions = ntem_constants.Versions.EIGHT
    """Version to produce outputs for."""

    def metadata_id(self) -> int:
        return self.scenario.id(self.version)


@dataclasses.dataclass
class PlanningQuery(QueryParams):
    residential: bool = True
    employment: bool = True
    household: bool = True

    def query(self, connection: sqlalchemy.Connection) -> pd.DataFrame:

        data = planning_data(
            connection,
            self.metadata_id(),
            self.filter_zoning_system.id,
            self.filter_zone_names,
            self.year,
        )

        return data


@dataclasses.dataclass
class RunParams(abc.ABC):
    years: list[int]
    """Years to produce outputs."""
    scenarios: list[ntem_constants.Scenarios]
    """Scenarios to produce outputs"""
    filter_zoning_system: ntem_constants.ZoningSystems
    """The zoning system to use when filtering data down"""
    filter_zone_names: list[str]
    """Zones to select from the data, must be in the names column of the zoning zones table."""
    output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE
    """Zoning system to output the data in."""
    version: ntem_constants.Versions = ntem_constants.Versions.EIGHT
    """Version to produce outputs for."""


@dataclasses.dataclass
class PlanningParams(RunParams):
    residential: bool = True
    employment: bool = True
    household: bool = True

    def __iter__(self):
        for s in self.scenarios:
            for y in self.years:
                yield PlanningQuery(
                    y,
                    s,
                    self.filter_zoning_system,
                    self.filter_zone_names,
                    self.output_zoning,
                    self.version,
                    self.residential,
                    self.employment,
                    self.household,
                )


def _interpolation_years(year) -> tuple[int, int] | None:
    """Calculates years required for interpolation"""

    if year in ntem_constants.NTEM_YEARS:
        return None

    upper_year = int(ntem_constants.NTEM_YEARS[ntem_constants.NTEM_YEARS > year].min())
    lower_year = int(ntem_constants.NTEM_YEARS[ntem_constants.NTEM_YEARS < year].max())

    return (lower_year, upper_year)


def _zone_subset(zone_names: list[str], zoning_id: int) -> sqlalchemy.Select:
    """SQL query which returns the subset of zones."""
    return (
        sqlalchemy.select(structure.GeoLookup.from_zone_id)
        .join(
            structure.Zones,
            (structure.GeoLookup.to_zone_id == structure.Zones.id)
            & (structure.GeoLookup.to_zone_type_id == structure.Zones.zone_type_id),
            isouter=True,
        )
        .where(
            (structure.Zones.name.in_(zone_names))
            & (structure.Zones.zone_type_id == zoning_id)
        )
    )


def filter_planning_data_query(
    connection: sqlalchemy.Connection,
    metadata_id: int,
    filter_zoning_system: int,
    filter_zone: list[str],
    year: int,
) -> pd.DataFrame:
    return pd.read_sql(
        sqlalchemy.select(structure.Planning).where(
            structure.Planning.zone_id.in_(_zone_subset(filter_zone, filter_zoning_system))
            & (structure.Planning.year == year)
            & (structure.Planning.metadata_id == metadata_id)
        ),
        connection,
        [
            structure.Planning.metadata_id.name,
            structure.Planning.zone_id.name,
            structure.Planning.planning_data_type.name,
        ],
    )[structure.Planning.value.name]


def _interpolate(
    lower_values: pd.DataFrame,
    upper_values: pd.DataFrame,
    year: int,
    lower_year: int,
    upper_year: int,
) -> pd.DataFrame:

    gradient = (upper_values - lower_values) / (upper_year - lower_year)

    return gradient * (year - lower_year) + lower_values


def planning_data(
    connection: sqlalchemy.Connection,
    metadata_id: int,
    filter_zoning_system: int,
    filter_zone: list[str],
    year: int,
) -> pd.DataFrame:
    interp_years = _interpolation_years(year)
    if interp_years is None:
        return filter_planning_data_query(
            connection, metadata_id, filter_zoning_system, filter_zone, year
        )

    upper = filter_planning_data_query(
        connection, metadata_id, filter_zoning_system, filter_zone, interp_years[0]
    )
    lower = filter_planning_data_query(
        connection, metadata_id, filter_zoning_system, filter_zone, interp_years[1]
    )

    return _interpolate(upper, lower, year, interp_years[0], interp_years[1])


def perform_query(args: QueryArgs):

    engine = sqlalchemy.create_engine(structure.connection_string(args.db_path))
    with sqlalchemy.Connection(engine) as connection:
        if args.planning_runs is not None:
            for run in args.planning_runs:
                for query in run:
                    query.query(connection)
