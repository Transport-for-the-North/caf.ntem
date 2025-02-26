from __future__ import annotations

# Built-Ins
import abc
import dataclasses
import pathlib
from typing import Generator

# Third Party
import pydantic

# Local Imports
from caf.ntem import ntem_constants, query, structure


class QueryArgs(ntem_constants.InputBase):
    output_path: pathlib.Path = pydantic.Field(description="Path to the output directory.")
    """Path to directory to output the processed NTEM data."""
    db_path: pydantic.FilePath = pydantic.Field(description="Path to NTEM database.")
    """Path to NTEM database, which has ben outputted by the build module."""
    label: str | None = None
    planning_runs: list[PlanningParams] | None = None
    trip_end_by_direction_runs: list[TripEndByDirectionRunParams] | None = None
    car_ownership_runs: list[CarOwnershipParams] | None = None

    @property
    def logging_path(self) -> pathlib.Path:
        return self.output_path / "caf_ntem.log"

    def run(self) -> None:
        db_handler = structure.DataBaseHandler(self.db_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

        run_params: list[RunParams] = []

        if self.planning_runs is not None:
            run_params.extend(self.planning_runs)
            
        if self.trip_end_by_direction_runs is not None: 
            run_params.extend(self.trip_end_by_direction_runs)

        if self.car_ownership_runs is not None: 
            run_params.extend(self.car_ownership_runs)

        for run in run_params:
            for query in run:
                query.query(db_handler).to_csv(
                    (self.output_path / query.name).with_suffix(".csv")
                )


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
    label: str | None = None

    @abc.abstractmethod
    def __iter__(self) -> Generator[query.QueryParams, None, None]:
        pass


@dataclasses.dataclass
class PlanningParams(RunParams):
    residential: bool = True
    employment: bool = True
    household: bool = True

    def __iter__(self) -> Generator[query.PlanningQuery, abc.Any, None]:
        for s in self.scenarios:
            for y in self.years:
                yield query.PlanningQuery(
                    y,
                    s,
                    self.filter_zoning_system,
                    self.filter_zone_names,
                    self.output_zoning,
                    self.version,
                    self.label,
                    self.residential,
                    self.employment,
                    self.household,
                )


@dataclasses.dataclass
class TripEndByDirectionRunParams(RunParams):
    trip_type: ntem_constants.TripType = ntem_constants.TripType.OD
    purpose_filter: list[ntem_constants.Purpose] | None = None
    aggregate_purpose: bool = True
    mode_filter: list[ntem_constants.Mode] | None = None
    aggregate_mode: bool = True
    time_period_filter: list[ntem_constants.TimePeriod] | None = None

    def __iter__(self) -> Generator[query.TripEndByDirectionQuery, abc.Any, None]:
        for s in self.scenarios:
            for y in self.years:
                yield query.TripEndByDirectionQuery(
                    y,
                    s,
                    self.filter_zoning_system,
                    self.filter_zone_names,
                    self.output_zoning,
                    self.version,
                    self.label,
                    self.trip_type,
                    self.purpose_filter,
                    self.aggregate_purpose,
                    self.mode_filter,
                    self.aggregate_mode,
                    self.time_period_filter,
                )

@dataclasses.dataclass
class TripEndByCarAvailbilityRunParams(RunParams):
    purpose_filter: list[ntem_constants.Purpose] | None = None
    aggregate_purpose: bool = True
    mode_filter: list[ntem_constants.Mode] | None = None
    aggregate_mode: bool = True

    def __iter__(self) -> Generator[query.TripEndByDirectionQuery, abc.Any, None]:
        for s in self.scenarios:
            for y in self.years:
                yield query.TripEndByDirectionQuery(
                    y,
                    s,
                    self.filter_zoning_system,
                    self.filter_zone_names,
                    self.output_zoning,
                    self.version,
                    self.label,
                    self.trip_type,
                    self.purpose_filter,
                    self.aggregate_purpose,
                    self.mode_filter,
                    self.aggregate_mode,
                    self.time_period_filter,
                )


@dataclasses.dataclass
class CarOwnershipParams(RunParams):

    def __iter__(self):
        for s in self.scenarios:
            for y in self.years:
                yield query.CarOwnershipQuery(
                    y,
                    s,
                    self.filter_zoning_system,
                    self.filter_zone_names,
                    self.output_zoning,
                    self.version,
                    self.label,
                )
