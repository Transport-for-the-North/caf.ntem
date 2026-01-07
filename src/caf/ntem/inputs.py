"""Handles input configs for running the queries module."""

from __future__ import annotations

# Built-Ins
import abc
import logging
import pathlib
from typing import Generator

# Third Party
import pydantic
import sqlalchemy
import tqdm
from pydantic import dataclasses

# Local Imports
from caf.ntem import ntem_constants, queries, structure

LOG = logging.getLogger(__name__)


class QueryArgs(ntem_constants.InputBase):
    """Queries config that defines the specification of the outputted data."""

    output_path: pathlib.Path = pydantic.Field(description="Path to the output directory.")
    """Path to directory to output the processed NTEM data."""
    db_path: pydantic.FilePath = pydantic.Field(description="Path to NTEM database.")
    """Path to NTEM database, which has ben outputted by the build module."""
    planning_runs: list[PlanningParams] | None = None
    """Define the planning queries."""
    trip_end_by_direction_runs: list[TripEndByDirectionRunParams] | None = None
    """Define the trip end by direction queries."""
    car_ownership_runs: list[CarOwnershipParams] | None = None
    """Define the car ownership queries."""
    trip_end_by_car_availability_runs: list[TripEndByCarAvailabilityRunParams] | None = None
    """Define the trip end by car availability queries."""

    @property
    def logging_path(self) -> pathlib.Path:
        """Return the logging path for the module."""
        return self.output_path / "caf_ntem.log"

    def run(self) -> None:
        """Run the query process."""
        engine = sqlalchemy.create_engine(structure.connection_string(self.db_path))
        # no member error is raised despite correct type hint as it has been set to a pydantic field.
        self.output_path.mkdir(parents=True, exist_ok=True)  # pylint: disable = "no-member"

        run_params: list[RunParams] = []

        if self.planning_runs is not None:
            run_params.extend(self.planning_runs)

        if self.trip_end_by_direction_runs is not None:
            run_params.extend(self.trip_end_by_direction_runs)

        if self.car_ownership_runs is not None:
            run_params.extend(self.car_ownership_runs)

        if self.trip_end_by_car_availability_runs is not None:
            run_params.extend(self.trip_end_by_car_availability_runs)

        if len(run_params) == 0:
            raise ValueError("No queries have been defined.")

        with engine.connect() as conn:
            for run in run_params:
                for query in tqdm.tqdm(run, desc=f"Running {run.label}"):
                    LOG.info("Running query: %s", query.name)
                    query.query(conn).to_csv(
                        (self.output_path / query.name).with_suffix(".csv")
                    )


@dataclasses.dataclass
class RunParams(abc.ABC):
    """Base class that defines the specification of queries for each data type."""

    years: list[int]
    """Years to produce outputs."""
    scenarios: list[ntem_constants.Scenarios]
    """Scenarios to produce outputs"""
    output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE
    """Zoning system to output the data in."""
    version: ntem_constants.Versions = ntem_constants.Versions.EIGHT
    """Version to produce outputs for."""
    filter_zoning_system: ntem_constants.ZoningSystems | None = None
    """The zoning system to use when filtering data down"""
    filter_zone_names: list[str] | None = None
    """Zones to select from the data, must be in the names column of the zoning zones table."""
    label: str | None = None

    @abc.abstractmethod
    def __iter__(self) -> Generator[queries.QueryParams, None, None]:
        """Iterate through queries, split by scenario.

        Yields
        ------
        Generator[queries.QueryParams, None, None]
            Query.
        """


@dataclasses.dataclass
class PlanningParams(RunParams):
    """Planning query parameters."""

    residential: bool = True
    """Whether to include residential data in the output."""
    employment: bool = True
    """Whether to include employment data in the output."""
    household: bool = True
    """Whether to include household data in the output."""

    def __iter__(self) -> Generator[queries.PlanningQuery, None, None]:
        """Iterate through planning queries, split by scenario.

        Yields
        ------
        Generator[queries.PlanningQuery, None, None]
            Planning query.
        """
        for s in self.scenarios:
            yield queries.PlanningQuery(
                *self.years,
                scenario=s,
                version=self.version,
                filter_zoning_system=self.filter_zoning_system,
                filter_zone_names=self.filter_zone_names,
                output_zoning=self.output_zoning,
                label=self.label,
                residential=self.residential,
                employment=self.employment,
                household=self.household,
            )


@dataclasses.dataclass
class TripEndByDirectionRunParams(RunParams):
    """Trip End by Direction query parameters."""

    trip_type: ntem_constants.TripType = ntem_constants.TripType.OD
    """Trip types to retrieve."""
    purpose_filter: list[ntem_constants.Purpose] | None = None
    """Purposes to retrieve, if None all are retrieved."""
    aggregate_purpose: bool = True
    """"Whether to aggregate purposes."""
    mode_filter: list[ntem_constants.Mode] | None = None
    """Modes to retrieve, if None all are retrieved."""
    aggregate_mode: bool = True
    """Whether to aggregate modes."""
    time_period_filter: list[ntem_constants.TimePeriod] | None = None
    """Time periods to retrieve, if None all are given"""

    def __iter__(self) -> Generator[queries.TripEndByDirectionQuery, None, None]:
        """Iterate through trip end by direction queries, split by scenario.

        Yields
        ------
        Generator[queries.PlanningQuery, None, None]
            Trip end by direction query.
        """
        for s in self.scenarios:
            yield queries.TripEndByDirectionQuery(
                *self.years,
                scenario=s,
                version=self.version,
                label=self.label,
                filter_zoning_system=self.filter_zoning_system,
                filter_zone_names=self.filter_zone_names,
                output_zoning=self.output_zoning,
                trip_type=self.trip_type,
                purpose_filter=self.purpose_filter,
                aggregate_purpose=self.aggregate_purpose,
                mode_filter=self.mode_filter,
                aggregate_mode=self.aggregate_mode,
                time_period_filter=self.time_period_filter,
            )


@dataclasses.dataclass
class TripEndByCarAvailabilityRunParams(RunParams):
    """Trip end by car availability query params."""

    purpose_filter: list[ntem_constants.Purpose] | None = None
    """Purposes to retrieve, if None all are retrieved."""
    aggregate_purpose: bool = True
    """"Whether to aggregate purposes."""
    mode_filter: list[ntem_constants.Mode] | None = None
    """Modes to retrieve, if None all are retrieved."""
    aggregate_mode: bool = True
    """Whether to aggregate modes."""

    def __iter__(self) -> Generator[queries.TripEndByCarAvailabilityQuery, None, None]:
        """Iterate through trip end by car availability queries, split by scenario.

        Yields
        ------
        Generator[queries.PlanningQuery, None, None]
            Trip end by car availability query.
        """
        for s in self.scenarios:
            yield queries.TripEndByCarAvailabilityQuery(
                *self.years,
                scenario=s,
                version=self.version,
                output_zoning=self.output_zoning,
                filter_zoning_system=self.filter_zoning_system,
                filter_zone_names=self.filter_zone_names,
                label=self.label,
                purpose_filter=self.purpose_filter,
                aggregate_purpose=self.aggregate_purpose,
                mode_filter=self.mode_filter,
                aggregate_mode=self.aggregate_mode,
            )


@dataclasses.dataclass
class CarOwnershipParams(RunParams):
    """Car ownership query params."""

    def __iter__(self) -> Generator[queries.CarOwnershipQuery, None, None]:
        """Iterate through car ownership queries, split by scenario.

        Yields
        ------
        Generator[queries.PlanningQuery, None, None]
            Car ownership query.
        """
        for s in self.scenarios:
            yield queries.CarOwnershipQuery(
                *self.years,
                scenario=s,
                version=self.version,
                filter_zoning_system=self.filter_zoning_system,
                filter_zone_names=self.filter_zone_names,
                output_zoning=self.output_zoning,
                label=self.label,
            )
