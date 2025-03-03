from __future__ import annotations

# Built-Ins
import abc
import dataclasses
import logging
from typing import Callable, Iterable

# Third Party
import pandas as pd
import sqlalchemy

# Local Imports
from caf.ntem import ntem_constants, structure

LOG = logging.getLogger(__name__)


def _linear_interpolate(func: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
    """Interpolates between years for the given function."""

    def wrapper_func(*args, years: Iterable[int], **kwargs):
        query_years: set[int] = set()
        interpolations: dict[int, tuple[int, int] | None] = {}

        for y in years:

            interp_years = _interpolation_years(y)
            if interp_years is not None:
                LOG.debug("Interpolating year %s from %s and %s", y, *interp_years)
                query_years.update(interp_years)
                interpolations[y] = interp_years
            else:
                LOG.debug("Interpolating quering year %s", y)
                query_years.add(y)
                interpolations[y] = None

        try:

            query_out = func(
                *args,
                years=query_years,
                **kwargs,
            )

            if len(query_out) == 0:
                raise ValueError("No data returned from query")

            index_levels = list(query_out.index.names)
            if "year" not in index_levels:
                raise KeyError("'year' not in index levels")

            # this is to ensure that the year is the last index level
            # stops any weirdness when concatenating
            index_levels.remove("year")
            index_levels.append("year")
            query_out = query_out.reorder_levels(index_levels)

            output_stack = []
            for y in years:
                interp_years = interpolations[y]

                if interp_years is None:
                    output_stack.append(query_out.xs(y, level="year", drop_level=False))

                else:
                    # Linear interpolation = (year - year_0) * ((value_1 - value_0) / (year_1 - year_0)) + value_0
                    interp = (
                        (
                            query_out.xs(interp_years[1], level="year")
                            - query_out.xs(interp_years[1], level="year")
                        )
                        / (interp_years[0] - interp_years[1])
                    ) * (y - interp_years[0]) + query_out.xs(interp_years[1], level="year")
                    interp["year"] = y
                    interp = interp.set_index("year", append=True)
                    output_stack.append(interp)

            return pd.concat(output_stack)

        except MemoryError as e:
            raise MemoryError(
                f"Memory error raised when trying to interpolate data for years {years}. "
                "Consider reducing the number of years queried"
                " or reducing the number of segments"
            ) from e

    return wrapper_func


class QueryParams(abc.ABC):
    def __init__(
        self,
        years: Iterable[int],
        scenario: ntem_constants.Scenarios,
        output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE,
        version: ntem_constants.Versions = ntem_constants.Versions.EIGHT,
        filter_zoning_system: ntem_constants.ZoningSystems | None = None,
        filter_zone_names: list[str] | None = None,
    ):

        self._years: Iterable[int] = years
        self._scenario: int = int(scenario.id(version))
        self._output_zoning: int = int(output_zoning.id)
        self._metadata_id: int = int(scenario.id(version))
        self._filter_zoning_system: int | None = (
            int(filter_zoning_system.id) if filter_zoning_system is not None else None
        )
        self._filter_zone_names: list[str] | None = filter_zone_names

    @abc.abstractmethod
    def query(self, db_handler: structure.DataBaseHandler) -> pd.DataFrame:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass


class PlanningQuery(QueryParams):

    def __init__(
        self,
        *years: int,
        scenario: ntem_constants.Scenarios,
        version: ntem_constants.Versions = ntem_constants.Versions.EIGHT,
        label: str | None = None,
        output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE,
        filter_zoning_system: ntem_constants.ZoningSystems | None = None,
        filter_zone_names: list[str] | None = None,
        residential: bool = True,
        employment: bool = True,
        household: bool = True,
    ):

        if label is None:
            self._name: str = f"Planning_{scenario.value}_{version.value}"
        else:
            self._name = f"Planning_{label}_{scenario.value}_{version.value}"
        super().__init__(
            years=years,
            scenario=scenario,
            output_zoning=output_zoning,
            version=version,
            filter_zoning_system=filter_zoning_system,
            filter_zone_names=filter_zone_names,
        )

        self._residential: bool = residential
        self._employment: bool = employment
        self._household: bool = household

    def query(self, db_handler: structure.DataBaseHandler) -> pd.DataFrame:

        data = self._data_query(
            db_handler=db_handler,
            years=self._years,
        )

        if not self._residential:
            data = data.drop(columns=["16 to 74", "Less than 16", "75 +"])
        if not self._employment:
            data = data.drop(columns=["Jobs", "Workers"])
        if not self._household:
            data = data.drop(columns=["Households"])

        return data

    @_linear_interpolate
    def _data_query(
        self,
        *,
        db_handler: structure.DataBaseHandler,
        years: Iterable[int],
    ) -> pd.DataFrame:
        LOG.debug("Building query for year %s", years)

        data_filter = (
            (structure.Planning.year.in_(years))
            & (structure.Planning.metadata_id == self._metadata_id)
            & (structure.PlanningDataTypes.id == structure.Planning.planning_data_type)
        )

        if self._filter_zoning_system is not None and self._filter_zone_names is not None:
            data_filter &= structure.Planning.zone_id.in_(
                _zone_subset(self._filter_zone_names, self._filter_zoning_system)
            )

        if self._output_zoning == ntem_constants.ZoningSystems.NTEM_ZONE.id:

            query = sqlalchemy.select(
                structure.Zones.source_id_or_code.label("zone_code"),
                structure.Zones.name.label("zone_name"),
                structure.PlanningDataTypes.name.label("data_type"),
                structure.Planning.year.label("year"),
                structure.Planning.value.label("value"),
            ).where(
                data_filter
                & (structure.Planning.zone_id == structure.Zones.id)
                & (structure.Planning.zone_type_id == structure.Zones.zone_type_id)
            )

        else:
            query = (
                sqlalchemy.select(
                    structure.Zones.source_id_or_code.label("zone_code"),
                    structure.Zones.name.label("zone_name"),
                    structure.PlanningDataTypes.name.label("data_type"),
                    structure.Planning.year.label("year"),
                    sqlalchemy.func.sum(structure.Planning.value).label("value"),
                )
                .where(
                    data_filter
                    & (structure.GeoLookup.from_zone_id == structure.Planning.zone_id)
                    & (
                        structure.GeoLookup.from_zone_type_id
                        == ntem_constants.ZoningSystems.NTEM_ZONE.id
                    )
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning)
                    & (structure.Zones.id == structure.GeoLookup.to_zone_id)
                )
                .group_by(
                    structure.Zones.id, structure.PlanningDataTypes.id, structure.Planning.year
                )
            )

        LOG.debug(f"Running query")
        data = db_handler.query_to_pandas(
            query,
        )
        LOG.debug(f"Query complete - post-processing data")
        if data["zone_code"].isna().any():
            data["zone"] = data["zone_name"]
        else:
            data["zone"] = data["zone_code"]

        return data.pivot(
            index=["zone", "year"],
            columns="data_type",
            values="value",
        )

    @property
    def name(self) -> str:
        return self._name


class CarOwnershipQuery(QueryParams):

    def __init__(
        self,
        *years: int,
        scenario: ntem_constants.Scenarios,
        version: ntem_constants.Versions = ntem_constants.Versions.EIGHT,
        label: str | None = None,
        output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE,
        filter_zoning_system: ntem_constants.ZoningSystems | None = None,
        filter_zone_names: list[str] | None = None,
    ):

        if label is None:
            self._name: str = f"Car_Ownership_{scenario.value}_{version.value}"
        else:
            self._name = f"Car_Ownership_{label}_{scenario.value}_{version.value}"
        super().__init__(
            years=years,
            scenario=scenario,
            output_zoning=output_zoning,
            version=version,
            filter_zoning_system=filter_zoning_system,
            filter_zone_names=filter_zone_names,
        )

    def query(self, db_handler: structure.DataBaseHandler) -> pd.DataFrame:

        return self._data_query(
            db_handler=db_handler,
            years=self._years,
        )

    @_linear_interpolate
    def _data_query(
        self,
        *,
        db_handler: structure.DataBaseHandler,
        years: Iterable[int],
    ) -> pd.DataFrame:
        LOG.debug("Building query for year %s", years)

        data_filter = structure.CarOwnership.year.in_(years) & (
            structure.CarOwnership.metadata_id == self._metadata_id
        )

        if self._filter_zoning_system is not None and self._filter_zone_names is not None:
            data_filter &= structure.CarOwnership.zone_id.in_(
                _zone_subset(self._filter_zone_names, self._filter_zoning_system)
            )

        if self._output_zoning == ntem_constants.ZoningSystems.NTEM_ZONE.id:

            query = (
                sqlalchemy.select(
                    structure.Zones.source_id_or_code.label("zone_code"),
                    structure.Zones.name.label("zone_name"),
                    structure.CarOwnershipTypes.name.label("car_ownership_type"),
                    structure.CarOwnership.year.label("year"),
                    structure.CarOwnership.value.label("value"),
                )
                .join(
                    structure.CarOwnershipTypes,
                    structure.CarOwnership.car_ownership_type
                    == structure.CarOwnershipTypes.id,
                )
                .join(
                    structure.Zones,
                    (structure.Zones.id == structure.CarOwnership.zone_id)
                    & (structure.Zones.zone_type_id == structure.CarOwnership.zone_type_id),
                )
            ).where(data_filter)

        else:
            query = (
                sqlalchemy.select(
                    structure.Zones.source_id_or_code.label("zone_code"),
                    structure.Zones.name.label("zone_name"),
                    structure.CarOwnershipTypes.name.label("car_ownership_type"),
                    structure.CarOwnership.year.label("year"),
                    sqlalchemy.func.sum(structure.CarOwnership.value).label("value"),
                )
                .join(
                    structure.CarOwnershipTypes,
                    structure.CarOwnership.car_ownership_type
                    == structure.CarOwnershipTypes.id,
                )
                .join(
                    structure.GeoLookup,
                    (
                        (structure.GeoLookup.from_zone_id == structure.CarOwnership.zone_id)
                        & (
                            structure.GeoLookup.from_zone_type_id
                            == structure.CarOwnership.zone_type_id
                        )
                    ),
                    isouter=True,
                )
                .join(
                    structure.Zones,
                    (structure.Zones.id == structure.GeoLookup.to_zone_id)
                    & (structure.Zones.zone_type_id == structure.GeoLookup.to_zone_type_id),
                    isouter=True,
                )
                .where(
                    data_filter
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning)
                    & (
                        structure.GeoLookup.from_zone_type_id
                        == ntem_constants.ZoningSystems.NTEM_ZONE.id
                    )
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning)
                )
                .group_by(
                    structure.Zones.id,
                    structure.CarOwnershipTypes.id,
                    structure.CarOwnership.year,
                )
            )
        LOG.debug(f"Running query")
        data = db_handler.query_to_pandas(
            query,
        )
        LOG.debug(f"Query complete - post-processing data")
        if data["zone_code"].isna().any():
            data["zone"] = data["zone_name"]
        else:
            data["zone"] = data["zone_code"]

        return data.pivot(
            index=["year", "zone"],
            columns="car_ownership_type",
            values="value",
        )

    @property
    def name(self) -> str:
        return self._name


class TripEndByDirectionQuery(QueryParams):
    def __init__(
        self,
        *year: int,
        scenario: ntem_constants.Scenarios,
        version: ntem_constants.Versions = ntem_constants.Versions.EIGHT,
        label: str | None = None,
        output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE,
        filter_zoning_system: ntem_constants.ZoningSystems | None = None,
        filter_zone_names: list[str] | None = None,
        trip_type: ntem_constants.TripType = ntem_constants.TripType.OD,
        purpose_filter: list[ntem_constants.Purpose] | None = None,
        aggregate_purpose: bool = True,
        mode_filter: list[ntem_constants.Mode] | None = None,
        aggregate_mode: bool = True,
        time_period_filter: list[ntem_constants.TimePeriod] | None = None,
        output_names: bool = True,
    ):

        if label is None:
            self._name: str = f"trip_ends_{trip_type.value}"
            f"_{scenario.value}_{version.value}"
        else:
            self._name = f"trip_ends_{trip_type.value}_{label}"
            f"_{scenario.value}_{version.value}"

        super().__init__(
            years=year,
            scenario=scenario,
            output_zoning=output_zoning,
            version=version,
            filter_zoning_system=filter_zoning_system,
            filter_zone_names=filter_zone_names,
        )
        self._purpose_filter: list[int] | None = None
        self._aggregate_purpose: bool = aggregate_purpose
        self._mode_filter: list[int] | None = None
        self._aggregate_mode: bool = aggregate_mode
        self._time_period_filter: list[int] | None = None
        self._replace_names = output_names

        self._trip_type = trip_type.id()
        if purpose_filter is not None:
            # TODO(kf) int to to stop linting complaining - probs fix this later
            self._purpose_filter = [int(p.value) for p in purpose_filter]

        if mode_filter is not None:
            self._mode_filter = [m.id() for m in mode_filter]

        if self._time_period_filter is not None:
            self._time_period_filter = [tp.id() for tp in time_period_filter]

    @property
    def name(self) -> str:
        return self._name

    def query(self, db_handler: structure.DataBaseHandler) -> pd.DataFrame:

        data = self._data_query(
            db_handler=db_handler,
            years=self._years,
        )

        data = self.apply_lookups(data, db_handler, self._replace_names)

        return data

    def apply_lookups(
        self, data: pd.DataFrame, db_handler: structure.DataBaseHandler, replace_ids: bool
    ) -> pd.DataFrame:

        LOG.debug(f"Applying lookups")
        data_values = data.copy()

        replacements: dict[str, dict[int, str]] = {}

        zones_lookup = db_handler.query_to_pandas(
            sqlalchemy.select(
                structure.Zones.id.label("id"), structure.Zones.source_id_or_code.label("name")
            ).where(structure.Zones.zone_type_id == self._output_zoning),
            index_columns=["id"],
        )

        if not zones_lookup["name"].isna().any():
            replacements["zone"] = zones_lookup["name"].to_dict()
        else:
            replacements["zone"] = db_handler.query_to_pandas(
                sqlalchemy.select(
                    structure.Zones.id.label("id"), structure.Zones.name.label("name")
                ).where(structure.Zones.zone_type_id == self._output_zoning),
                index_columns=["id"],
            )

        if replace_ids:

            replacements["time_period"] = db_handler.query_to_pandas(
                sqlalchemy.select(
                    structure.TimePeriodTypes.id.label("id"),
                    structure.TimePeriodTypes.name.label("name"),
                ),
                index_columns=["id"],
            )["name"].to_dict()

            if not self._aggregate_purpose:
                replacements["purpose"] = db_handler.query_to_pandas(
                    sqlalchemy.select(
                        structure.PurposeTypes.id.label("id"),
                        structure.PurposeTypes.name.label("name"),
                    ),
                    index_columns=["id"],
                )["name"].to_dict()

            if not self._aggregate_mode:
                replacements["mode"] = db_handler.query_to_pandas(
                    sqlalchemy.select(
                        structure.ModeTypes.id.label("id"),
                        structure.ModeTypes.name.label("name"),
                    ),
                    index_columns=["id"],
                )["name"].to_dict()

        for level, lookup in replacements.items():
            data_values = data_values.rename(index=lookup, level=level)

        return data_values

    @_linear_interpolate
    def _data_query(
        self,
        *,
        db_handler: structure.DataBaseHandler,
        years: Iterable[int],
    ) -> pd.DataFrame:
        LOG.debug("Building query for year %s", years)
        select_cols = [
            structure.TripEndDataByDirection.time_period,
            structure.TripEndDataByDirection.year,
            sqlalchemy.func.sum(
                structure.TripEndDataByDirection.value / structure.TimePeriodTypes.divide_by
            ).label("value"),
        ]

        index_cols = [
            "zone",
            "time_period",
            "year",
        ]

        groupby_cols = [
            structure.TripEndDataByDirection.time_period,
            structure.TripEndDataByDirection.year,
        ]

        if not self._aggregate_purpose:
            select_cols.append(structure.TripEndDataByDirection.purpose)
            groupby_cols.append(structure.TripEndDataByDirection.purpose)
            index_cols.append("purpose")

        if not self._aggregate_mode:
            select_cols.append(structure.TripEndDataByDirection.mode)
            groupby_cols.append(structure.TripEndDataByDirection.mode)
            index_cols.append("mode")

        if self._output_zoning == ntem_constants.ZoningSystems.NTEM_ZONE.id:
            select_cols.insert(0, structure.TripEndDataByDirection.zone_id.label("zone"))
            groupby_cols.insert(0, structure.TripEndDataByDirection.zone_id)

        else:
            select_cols.insert(0, structure.GeoLookup.to_zone_id.label("zone"))
            groupby_cols.insert(0, structure.GeoLookup.to_zone_id)

        base_filter = (
            (structure.TripEndDataByDirection.year.in_(years))
            & (structure.TripEndDataByDirection.metadata_id == self._metadata_id)
            & (structure.TripEndDataByDirection.trip_type.in_(self._trip_type))
        )

        query = sqlalchemy.select(*select_cols).join(
            structure.TimePeriodTypes,
            structure.TimePeriodTypes.id == structure.TripEndDataByDirection.time_period,
            isouter=True,
        )

        if self._filter_zoning_system is not None and self._filter_zone_names is not None:
            base_filter &= structure.TripEndDataByDirection.zone_id.in_(
                _zone_subset(self._filter_zone_names, self._filter_zoning_system)
            )

        elif (self._filter_zoning_system is not None and self._filter_zone_names is None) or (
            self._filter_zoning_system is None and self._filter_zone_names is not None
        ):
            raise ValueError(
                "Both filter_zoning_system and filter_zone must be provided "
                "or neither provided if no spatial filter is to be performed."
            )

        if self._purpose_filter is not None:
            base_filter &= structure.TripEndDataByDirection.purpose.in_(self._purpose_filter)

        if self._mode_filter is not None:
            base_filter &= structure.TripEndDataByDirection.mode.in_(self._mode_filter)

        if self._time_period_filter is not None:
            base_filter &= structure.TripEndDataByDirection.time_period.in_(
                self._time_period_filter
            )

        if self._output_zoning == ntem_constants.ZoningSystems.NTEM_ZONE.id:
            query = query.where(base_filter)

            if self._aggregate_mode or self._aggregate_purpose:
                query = query.group_by(*groupby_cols)

        else:
            query = (
                query.join(
                    structure.GeoLookup,
                    structure.GeoLookup.from_zone_id
                    == structure.TripEndDataByDirection.zone_id,
                    isouter=True,
                )
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
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning)
                    & (structure.Zones.id == structure.GeoLookup.to_zone_id)
                )
                .group_by(*groupby_cols)
            )
        LOG.debug(f"Running query")
        data = db_handler.query_to_pandas(
            query,
            index_columns=index_cols,
        )
        LOG.debug(f"Query complete")

        return data


@dataclasses.dataclass
class TripEndByCarAvailbilityQuery(QueryParams):
    purpose_filter: list[ntem_constants.Purpose] | None = None
    aggregate_purpose: bool = True
    mode_filter: list[ntem_constants.Mode] | None = None
    aggregate_mode: bool = True

    def __init__(
        self,
        *years: int,
        scenario: ntem_constants.Scenarios,
        version: ntem_constants.Versions = ntem_constants.Versions.EIGHT,
        label: str | None = None,
        output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE,
        filter_zoning_system: ntem_constants.ZoningSystems | None = None,
        filter_zone_names: list[str] | None = None,
        purpose_filter: list[ntem_constants.Purpose] | None = None,
        aggregate_purpose: bool = True,
        mode_filter: list[ntem_constants.Mode] | None = None,
        aggregate_mode: bool = True,
        output_names: bool = True,
    ):

        if label is None:
            self._name: str = f"trip_ends_{years}"
            f"_{scenario.value}_{version.value}"
        else:
            self._name = f"trip_ends_{label}"
            f"_{years}_{scenario.value}_{version.value}"

        super().__init__(
            years=years,
            scenario=scenario,
            output_zoning=output_zoning,
            version=version,
            filter_zoning_system=filter_zoning_system,
            filter_zone_names=filter_zone_names,
        )
        self._purpose_filter: list[int] | None = None
        self._aggregate_purpose: bool = aggregate_purpose
        self._mode_filter: list[int] | None = None
        self._aggregate_mode: bool = aggregate_mode
        self._replace_names = output_names

        if purpose_filter is not None:
            # TODO(kf) int to to stop linting complaining - probs fix this later
            self._purpose_filter = [int(p.value) for p in purpose_filter]

        if mode_filter is not None:
            self._mode_filter = [m.id() for m in mode_filter]

    @property
    def name(self) -> str:
        return self._name

    def query(self, db_handler: structure.DataBaseHandler) -> pd.DataFrame:

        data = self._data_query(
            db_handler=db_handler,
            years=self._years,
        )

        data = self.apply_lookups(data, db_handler, self._replace_names)

        return data

    def apply_lookups(
        self, data: pd.DataFrame, db_handler: structure.DataBaseHandler, replace_ids: bool
    ) -> pd.DataFrame:
        LOG.debug(f"Applying lookups")
        data_values = data.copy()

        replacements: dict[str, dict[int, str]] = {}

        zones_lookup = db_handler.query_to_pandas(
            sqlalchemy.select(
                structure.Zones.id.label("id"), structure.Zones.source_id_or_code.label("name")
            ).where(structure.Zones.zone_type_id == self._output_zoning),
            index_columns=["id"],
        )

        if not zones_lookup["name"].isna().any():
            replacements["zone"] = zones_lookup["name"].to_dict()
        else:
            replacements["zone"] = db_handler.query_to_pandas(
                sqlalchemy.select(
                    structure.Zones.id.label("id"), structure.Zones.name.label("name")
                ).where(structure.Zones.zone_type_id == self._output_zoning),
                index_columns=["id"],
            )

        if replace_ids:

            if not self._aggregate_purpose:
                replacements["purpose"] = db_handler.query_to_pandas(
                    sqlalchemy.select(
                        structure.PurposeTypes.id.label("id"),
                        structure.PurposeTypes.name.label("name"),
                    ),
                    index_columns=["id"],
                )["name"].to_dict()

            if not self._aggregate_mode:
                replacements["mode"] = db_handler.query_to_pandas(
                    sqlalchemy.select(
                        structure.ModeTypes.id.label("id"),
                        structure.ModeTypes.name.label("name"),
                    ),
                    index_columns=["id"],
                )["name"].to_dict()

        for col, lookup in replacements.items():
            data_values[col] = data_values[col].replace(lookup)

        return data_values

    @_linear_interpolate
    def _data_query(
        self,
        *,
        db_handler: structure.DataBaseHandler,
        years: Iterable[int],
    ) -> pd.DataFrame:
        LOG.debug("Building query for year %s", years)

        index_cols: list[str] = ["zone", "year"]

        select_cols = [
            structure.TripEndDataByCarAvailability.year.label("year"),
            sqlalchemy.func.sum(structure.TripEndDataByCarAvailability.value).label("value"),
        ]

        groupby_cols = [structure.TripEndDataByCarAvailability.year]

        if not self._aggregate_purpose:
            index_cols.append("purpose")
            select_cols.insert(0, structure.TripEndDataByCarAvailability.purpose)
            groupby_cols.append(structure.TripEndDataByCarAvailability.purpose)

        if not self._aggregate_mode:
            index_cols.append("mode")
            select_cols.insert(0, structure.TripEndDataByCarAvailability.mode)
            groupby_cols.append(structure.TripEndDataByCarAvailability.mode)

        if self._output_zoning == ntem_constants.ZoningSystems.NTEM_ZONE.id:
            select_cols.insert(0, structure.TripEndDataByCarAvailability.zone_id.label("zone"))
            groupby_cols.insert(0, structure.TripEndDataByCarAvailability.zone_id)

        else:
            select_cols.insert(0, structure.GeoLookup.to_zone_id.label("zone"))
            groupby_cols.insert(0, structure.GeoLookup.to_zone_id)

        base_filter = (structure.TripEndDataByCarAvailability.year.in_(years)) & (
            structure.TripEndDataByCarAvailability.metadata_id == self._metadata_id
        )

        query = sqlalchemy.select(*select_cols)

        if self._filter_zoning_system is not None and self._filter_zone_names is not None:
            base_filter &= structure.TripEndDataByCarAvailability.zone_id.in_(
                _zone_subset(self._filter_zone_names, self._filter_zoning_system)
            )

        elif (self._filter_zoning_system is not None and self._filter_zone_names is None) or (
            self._filter_zoning_system is None and self._filter_zone_names is not None
        ):
            raise ValueError(
                "Both filter_zoning_system and filter_zone must be provided "
                "or neither provided if no spatial filter is to be performed."
            )

        if self._purpose_filter is not None:
            base_filter &= structure.TripEndDataByCarAvailability.purpose.in_(
                self._purpose_filter
            )

        if self._mode_filter is not None:
            base_filter &= structure.TripEndDataByCarAvailability.mode.in_(self._mode_filter)

        if self._output_zoning == ntem_constants.ZoningSystems.NTEM_ZONE.id:
            query = query.where(base_filter)

            if self._aggregate_mode or self._aggregate_purpose:
                query = query.group_by(*groupby_cols)

        else:
            query = (
                query.join(
                    structure.GeoLookup,
                    structure.GeoLookup.from_zone_id
                    == structure.TripEndDataByDirection.zone_id,
                    isouter=True,
                )
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
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning)
                    & (structure.Zones.id == structure.GeoLookup.to_zone_id)
                )
                .group_by(*groupby_cols)
            )
        LOG.debug(f"Running query")
        data = db_handler.query_to_pandas(
            query,
        )
        LOG.debug(f"Query complete")
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
