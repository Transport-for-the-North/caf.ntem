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
    label: str | None = None

    def metadata_id(self) -> int:
        return self.scenario.id(self.version)


@dataclasses.dataclass
class PlanningQuery(QueryParams):
    residential: bool = True
    employment: bool = True
    household: bool = True

    def query(self, db_handler: structure.DataBaseHandler) -> pd.DataFrame:

        data = planning_data(
            db_handler=db_handler,
            metadata_id=self.metadata_id(),
            filter_zoning_system=self.filter_zoning_system.id,
            filter_zone=self.filter_zone_names,
            output_zoning_system=self.output_zoning.id,
            year=self.year,
        )

        if not self.residential:
            data = data.drop(columns=["16 to 74", "Less than 16", "75 +"])
        if not self.employment:
            data = data.drop(columns=["Jobs", "Workers"])
        if not self.household:
            data = data.drop(columns=["Households"])

        return data

    @property
    def name(self) -> str:
        if self.label is None:
            return f"planning_{self.year}_{self.scenario.value}_{self.version.value}"
        return f"planning_{self.label}_{self.year}_{self.scenario.value}_{self.version.value}"


@dataclasses.dataclass
class TripEndByDirectionRunQuery(QueryParams):
    trip_type: ntem_constants.TripType = ntem_constants.TripType.OD
    purpose_filter: list[ntem_constants.Purpose] | None = None
    aggregate_purpose: bool = True
    mode_filter: list[ntem_constants.Mode] | None = None
    aggregate_mode: bool = True
    time_period_filter: list[ntem_constants.TimePeriod] | None = None

    @property
    def name(self) -> str:
        if self.label is None:
            return f"trip_ends_{self.trip_type.value}_{self.year}_{self.scenario.value}_{self.version.value}"
        return f"trip_ends_{self.trip_type.value}_{self.label}_{self.year}_{self.scenario.value}_{self.version.value}"

    def query(self, db_handler: structure.DataBaseHandler) -> pd.DataFrame:

        if self.purpose_filter is None:
            purpose_filter_ids = [p.value for p in ntem_constants.Purpose]
        else:
            purpose_filter_ids = [p.value for p in self.purpose_filter]

        if self.mode_filter is None:
            mode_filter_ids = [m.id() for m in ntem_constants.Mode]
        else:
            mode_filter_ids = [m.id() for m in self.mode_filter]

        if self.time_period_filter is None:
            tp_filter_ids = [tp.id() for tp in ntem_constants.TimePeriod]
        else:
            tp_filter_ids = [tp.id() for tp in self.time_period_filter]

        data = trip_end_by_direction_data(
            db_handler,
            self.metadata_id(),
            self.filter_zoning_system.id,
            self.filter_zone_names,
            self.output_zoning.id,
            year=self.year,
            trip_types=self.trip_type.id(),
            aggregate_purpose=self.aggregate_purpose,
            aggregate_mode=self.aggregate_mode,
            purpose_filter=purpose_filter_ids,
            mode_filter=mode_filter_ids,
            time_period_filter=tp_filter_ids,
        )
        return data


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


def _filter_planning_data_query(
    db_handler: structure.DataBaseHandler,
    metadata_id: int,
    filter_zoning_system: int,
    filter_zone: list[str],
    output_zoning_system: int,
    *,
    year: int,
) -> pd.DataFrame:
    if output_zoning_system == ntem_constants.ZoningSystems.NTEM_ZONE.id:

        query = sqlalchemy.select(
            structure.Zones.source_id_or_code,
            structure.Zones.name,
            structure.PlanningDataTypes.name,
            structure.Planning.value,
        ).where(
            structure.Planning.zone_id.in_(_zone_subset(filter_zone, filter_zoning_system))
            & (structure.Planning.year == year)
            & (structure.Planning.metadata_id == metadata_id)
            & (structure.Zones.id == structure.Planning.zone_id)
            & (structure.PlanningDataTypes.id == structure.Planning.planning_data_type)
        )

    else:
        query = (
            sqlalchemy.select(
                structure.Zones.source_id_or_code,
                structure.Zones.name,
                structure.PlanningDataTypes.name,
                sqlalchemy.func.sum(structure.Planning.value),
            )
            .where(
                structure.Planning.zone_id.in_(_zone_subset(filter_zone, filter_zoning_system))
                & (structure.Planning.year == year)
                & (structure.Planning.metadata_id == metadata_id)
                & (structure.PlanningDataTypes.id == structure.Planning.planning_data_type)
                & (structure.GeoLookup.from_zone_id == structure.Planning.zone_id)
                & (
                    structure.GeoLookup.from_zone_type_id
                    == ntem_constants.ZoningSystems.NTEM_ZONE.id
                )
                & (structure.GeoLookup.to_zone_type_id == output_zoning_system)
                & (structure.Zones.id == structure.GeoLookup.to_zone_id)
            )
            .group_by(structure.Zones.id, structure.PlanningDataTypes.id)
        )

    data = db_handler.query_to_pandas(
        query,
        column_names=["zone_code", "zone_name", "data_type", "value"],
    )

    if data["zone_code"].isna().any():
        data["zone"] = data["zone_name"]
    else:
        data["zone"] = data["zone_code"]

    return data.pivot(
        index="zone",
        columns="data_type",
        values="value",
    )


def process_te_by_direction_data_query(
    db_handler: structure.DataBaseHandler,
    metadata_id: int,
    filter_zoning_system: int,
    filter_zone: list[str],
    output_zoning_system: int,
    *,
    year: int,
    trip_type: list[int],
    aggregate_purpose: bool,
    aggregate_mode: bool,
    purpose_filter: list[int],
    mode_filter: list[int],
    time_period_filter: list[int],
) -> pd.DataFrame:

    select_cols = [
        structure.Zones.source_id_or_code.label("zone_code"),
        structure.Zones.name.label("zone_name"),
        structure.TimePeriodTypes.name.label("time_period"),
        sqlalchemy.func.sum(
            structure.TripEndDataByDirection.value / structure.TimePeriodTypes.divide_by
        ).label("value"),
    ]

    groupby_cols = [structure.Zones.id, structure.TripEndDataByDirection.time_period]

    if not aggregate_purpose:
        select_cols.append(structure.PurposeTypes.name.label("purpose"))
        groupby_cols.append(structure.TripEndDataByDirection.purpose)

    if not aggregate_mode:
        select_cols.append(structure.ModeTypes.name.label("mode"))
        groupby_cols.append(structure.TripEndDataByDirection.mode)

    base_filter = (
        structure.Planning.zone_id.in_(_zone_subset(filter_zone, filter_zoning_system))
        & (structure.TripEndDataByDirection.year == year)
        & (structure.TripEndDataByDirection.metadata_id == metadata_id)
        & (structure.TripEndDataByDirection.purpose.in_(purpose_filter))
        & (structure.TripEndDataByDirection.mode.in_(mode_filter))
        & (structure.TripEndDataByDirection.time_period.in_(time_period_filter))
        & (structure.TripEndDataByDirection.trip_type.in_(trip_type))
    )

    if output_zoning_system == ntem_constants.ZoningSystems.NTEM_ZONE.id:
        if not aggregate_mode and not aggregate_purpose:
            query = sqlalchemy.select(*select_cols).where(base_filter)
        else:

            query = (
                sqlalchemy.select(
                    structure.Zones.source_id_or_code.label("zone_code"),
                    structure.Zones.name.label("zone_name"),
                    *select_cols,
                    sqlalchemy.func.sum(
                        structure.TripEndDataByDirection.value
                        / structure.TimePeriodTypes.divide_by
                    ),
                )
                .where(base_filter)
                .group_by(*groupby_cols)
            )

    else:

        query = (
            sqlalchemy.select(*select_cols)
            .where(
                base_filter
                & (
                    structure.GeoLookup.from_zone_id
                    == structure.TripEndDataByDirection.zone_id
                )
                & (
                    structure.GeoLookup.from_zone_type_id
                    == ntem_constants.ZoningSystems.NTEM_ZONE.id
                )
                & (structure.GeoLookup.to_zone_type_id == output_zoning_system)
                & (structure.Zones.id == structure.GeoLookup.to_zone_id)
            )
            .group_by(*groupby_cols)
        )

    data = db_handler.query_to_pandas(
        query,
    )

    if data["zone_code"].isna().any():
        data["zone"] = data["zone_name"]
    else:
        data["zone"] = data["zone_code"]

    return data


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
    db_handler: structure.Data,
    metadata_id: int,
    filter_zoning_system: int,
    filter_zone: list[str],
    output_zoning_system: int,
    year: int,
) -> pd.DataFrame:

    interp_years = _interpolation_years(year)

    if interp_years is None:
        return _filter_planning_data_query(
            db_handler,
            metadata_id=metadata_id,
            filter_zoning_system=filter_zoning_system,
            filter_zone=filter_zone,
            output_zoning_system=output_zoning_system,
            year=year,
        )

    upper = _filter_planning_data_query(
        db_handler,
        metadata_id=metadata_id,
        filter_zoning_system=filter_zoning_system,
        filter_zone=filter_zone,
        output_zoning_system=output_zoning_system,
        year=interp_years[0],
    )
    lower = _filter_planning_data_query(
        db_handler,
        metadata_id=metadata_id,
        filter_zoning_system=filter_zoning_system,
        filter_zone=filter_zone,
        output_zoning_system=output_zoning_system,
        year=interp_years[1],
    )

    return _interpolate(upper, lower, year, interp_years[0], interp_years[1])


def trip_end_by_direction_data(
    db_handler: structure.DataBaseHandler,
    metadata_id: int,
    filter_zoning_system: int,
    filter_zone: list[str],
    output_zoning_system: int,
    *,
    year: int,
    trip_types: list[int],
    aggregate_purpose: bool,
    aggregate_mode: bool,
    purpose_filter: list[int],
    mode_filter: list[int],
    time_period_filter: list[int],
) -> pd.DataFrame:

    interp_years = _interpolation_years(year)

    if interp_years is None:
        return process_te_by_direction_data_query(
            db_handler,
            metadata_id,
            filter_zoning_system,
            filter_zone,
            output_zoning_system,
            year=year,
            trip_type=trip_types,
            aggregate_purpose=aggregate_purpose,
            aggregate_mode=aggregate_mode,
            purpose_filter=purpose_filter,
            mode_filter=mode_filter,
            time_period_filter=time_period_filter,
        )

    upper = process_te_by_direction_data_query(
        db_handler,
        metadata_id,
        filter_zoning_system,
        filter_zone,
        output_zoning_system,
        year=interp_years[0],
        trip_type=trip_types,
        aggregate_purpose=aggregate_purpose,
        aggregate_mode=aggregate_mode,
        purpose_filter=purpose_filter,
        mode_filter=mode_filter,
        time_period_filter=time_period_filter,
    )

    lower = process_te_by_direction_data_query(
        db_handler,
        metadata_id,
        filter_zoning_system,
        filter_zone,
        output_zoning_system,
        year=interp_years[1],
        trip_type=trip_types,
        aggregate_purpose=aggregate_purpose,
        aggregate_mode=aggregate_mode,
        purpose_filter=purpose_filter,
        mode_filter=mode_filter,
        time_period_filter=time_period_filter,
    )

    return _interpolate(upper, lower, year, interp_years[0], interp_years[1])
