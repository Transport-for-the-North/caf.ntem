"""Queries the database produced by build module to retrieve formatted NTEM datasets."""

from __future__ import annotations

# Built-Ins
import abc
import collections.abc
import logging
import warnings
from typing import Callable, Iterable

# Third Party
import caf.base as base  # pylint: disable = consider-using-from-import
import pandas as pd
import sqlalchemy

# Local Imports
from caf.ntem import ntem_constants, structure

LOG = logging.getLogger(__name__)


def _linear_interpolate(
    func: Callable[..., pd.DataFrame],
) -> Callable[..., pd.DataFrame]:
    """Interpolates between years for the given function."""

    def wrapper_func(*args, years: collections.abc.Collection[int], **kwargs) -> pd.DataFrame:
        query_years: set[int] = set()
        interpolations: dict[int, tuple[int, int]] = {}

        for y in years:
            interp_years = _interpolation_years(y)
            if interp_years is not None:
                LOG.debug("Interpolating year %s from %s and %s", y, *interp_years)
                query_years.update(interp_years)
                interpolations[y] = interp_years
            else:
                LOG.debug("Extracting year %s", y)
                query_years.add(y)
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
                if y not in interpolations:
                    output_stack.append(query_out.xs(y, level="year", drop_level=False))
                    continue

                lower, upper = interpolations[y]
                output_stack.append(
                    linear_interpolation_calculation(query_out, y, upper, lower)
                )

            return pd.concat(output_stack)

        except MemoryError as e:
            raise MemoryError(
                f"Memory error raised when trying to interpolate data for years {years}. "
                "Consider reducing the number of years queried"
                " or reducing the number of segments"
            ) from e

    return wrapper_func


def linear_interpolation_calculation(
    data: pd.DataFrame, output_year: int, upper_year: int, lower_year: int
) -> pd.DataFrame:
    """Perform linear interpolation between two years to produce a dataset for output year.

    Linear interpolation = ((output_year - lower_year) *
        ((upper_val - lower_val) / (upper_year - lower_year)) + lower_val)

    Parameters
    ----------
    data : pd.DataFrame
        Dataframe to perform interpolation on.
        Should have segmentations as multi-indices, including 'year' (int)
        as one of the levels. 'year' must contain both `upper_year` and `lower_year`.
    output_year : int
        Output year to produce data, should be between upper_year and lower_year.
    upper_year : int
        year that exists in the data that is after output_year.
    lower_year : int
        year that exists in the data that is before output_year.

    Returns
    -------
    pd.DataFrame
        Linearly interpolated data for the output year.
    """
    if upper_year < lower_year:
        raise ValueError("upper_year must be greater than lower_year")
    if upper_year < output_year or lower_year > output_year:
        raise ValueError("output_year must be between upper_year and lower_year")

    upper_data = data.xs(upper_year, level="year")
    lower_data = data.xs(lower_year, level="year")

    if len(upper_data) == 0 or len(lower_data) == 0:
        raise ValueError("No data for upper and/or lower year")

    if isinstance(upper_data.index, pd.MultiIndex):
        if not upper_data.index.equal_levels(lower_data.index):
            raise KeyError("Data for upper and lower year do not have the same index levels")
    else:
        if not upper_data.index.equals(lower_data.index):
            raise KeyError("Data for upper and lower year do not have the same index levels")

    interp = ((upper_data - lower_data) / (upper_year - lower_year)) * (
        output_year - lower_year
    ) + lower_data
    interp["year"] = output_year
    interp = interp.set_index("year", append=True)
    return interp


class QueryParams(abc.ABC):
    """Abstract base class for query classes."""

    def __init__(
        self,
        *years: int,
        scenario: ntem_constants.Scenarios,
        output_zoning: ntem_constants.ZoningSystems = ntem_constants.ZoningSystems.NTEM_ZONE,
        version: ntem_constants.Versions = ntem_constants.Versions.EIGHT,
        filter_zoning_system: ntem_constants.ZoningSystems | None = None,
        filter_zone_names: list[str] | None = None,
    ):
        """Initialise QueryParams.

        Parameters
        ----------
        years : int
            Year to provide data / interpolate.
        scenario : ntem_constants.Scenarios
            Scenario to provide data.
        output_zoning : ntem_constants.ZoningSystems, optional
            Zoning system to output data in, NTEM zoning is default.
        version : ntem_constants.Versions, optional
            Version of NTEM data to use, version 8.0 by default
        filter_zoning_system : ntem_constants.ZoningSystems | None, optional
            Zoning system to filter by, if None no spatial filter is performed.
        filter_zone_names : list[str] | None, optional
            Zones to filter for, if None no spatial filter is performed.
        """

        self._years: list[int] = list(years)
        self._scenario: int = int(scenario.id(version))
        self._output_zone_system = output_zoning
        self._output_zoning_id: int = int(output_zoning.id)
        self._metadata_id: int = int(scenario.id(version))
        self._filter_zoning_system: int | None = (
            int(filter_zoning_system.id) if filter_zoning_system is not None else None
        )
        self._filter_zone_names: list[str] | None = filter_zone_names

    @property
    def output_zone_system(self) -> ntem_constants.ZoningSystems:
        """Zone system query outputs at."""
        return self._output_zone_system

    @abc.abstractmethod
    def query(
        self, conn: sqlalchemy.Connection, include_zone_name: bool = False
    ) -> pd.DataFrame:
        """Query NTEM database using parameters defined on initialisation.

        Parameters
        ----------
        conn
            Connection to the database containing the NTEM data.
        include_zone_name
            If True, include "zone_name" column in output.

        Returns
        -------
        pd.DataFrame
            Queried data
        """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Name of the query."""


class PlanningQuery(QueryParams):
    """Query class for NTEM planning data.

    Parameters
    ----------
    years : int
        Years to provide data / interpolate.
    scenario : ntem_constants.Scenarios
        Scenario to provide data.
    output_zoning : ntem_constants.ZoningSystems, optional
        Zoning system to output data in, NTEM zoning is default.
    version : ntem_constants.Versions
        Version of NTEM data to use, version 8.0 by default
    filter_zoning_system : ntem_constants.ZoningSystems | None, optional
        Zoning system to filter by, if None no spatial filter is performed.
    filter_zone_names : list[str] | None, optional
        Zones to filter for, if None no spatial filter is performed.
    residential: bool
        Whether to include residential data in the output data set.
        True by default.
    employment: bool
        Whether to include employment data in the output data set.
        True by default.
    household: bool
        Whether to include household data in the output data set.
        True by default.
    """

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
            *years,
            scenario=scenario,
            output_zoning=output_zoning,
            version=version,
            filter_zoning_system=filter_zoning_system,
            filter_zone_names=filter_zone_names,
        )

        self._residential: bool = residential
        self._employment: bool = employment
        self._household: bool = household

    def query(
        self, conn: sqlalchemy.Connection, include_zone_name: bool = False
    ) -> pd.DataFrame:
        """Query NTEM database for Planning data using parameters defined on initialisation.

        Parameters
        ----------
        conn
            Connection to the database containing the NTEM data.
        include_zone_name
            If True, include "zone_name" column in output.

        Returns
        -------
        pd.DataFrame
        Planning data with columns "zone", "year", "data_type", "value"
        """
        # structure.query_to_dataframe(conn,sqlalchemy.Select(structure.MetaData.id).where(structure.MetaData.scenario==ntem_constants.Scenarios.CORE.value.lower()))
        data = self._data_query(
            conn=conn,
            years=self._years,
            include_zone_name=include_zone_name,
        )
        # TODO(kf) move these filters to where statement in the query
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
        conn: sqlalchemy.Connection,
        years: Iterable[int],
        include_zone_name: bool = False,
    ) -> pd.DataFrame:
        LOG.debug("Building planning query for year %s", years)

        data_filter = (
            (structure.Planning.year.in_(years))
            & (structure.Planning.metadata_id == self._metadata_id)
            & (structure.PlanningDataTypes.id == structure.Planning.planning_data_type)
        )

        if self._filter_zoning_system is not None and self._filter_zone_names is not None:
            data_filter &= structure.Planning.zone_id.in_(
                _zone_subset(self._filter_zone_names, self._filter_zoning_system)
            )
        elif (self._filter_zoning_system is not None and self._filter_zone_names is None) or (
            self._filter_zoning_system is None and self._filter_zone_names is not None
        ):
            raise ValueError(
                "Both filter_zoning_system and filter_zone must be provided "
                "or neither provided if no spatial filter is to be performed."
            )

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id:
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
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning_id)
                    & (structure.Zones.id == structure.GeoLookup.to_zone_id)
                )
                .group_by(
                    structure.Zones.id,
                    structure.PlanningDataTypes.id,
                    structure.Planning.year,
                )
            )

        LOG.debug("Running query")
        data = structure.query_to_dataframe(
            conn,
            query,
        )
        LOG.debug("Query complete - post-processing data")
        if data["zone_code"].isna().any():
            data["zone"] = data["zone_name"]
            warnings.warn(
                "The zone system you have chosen to output does not have a code column. Outputting zone name instead"
            )
        else:
            data["zone"] = data["zone_code"]

        if include_zone_name:
            index_cols = ["zone", "zone_name", "year"]
        else:
            index_cols = ["zone", "year"]

        return data.pivot(
            index=index_cols,
            columns="data_type",
            values="value",
        )

    @property
    def name(self) -> str:  # noqa: D102
        # Docstring inherited
        return self._name


class CarOwnershipQuery(QueryParams):
    """Defines CarOwnership queries on the NTEM database.

    Parameters
    ----------
    years : int
        Years to provide data / interpolate.
    scenario : ntem_constants.Scenarios
        Scenario to provide data.
    output_zoning : ntem_constants.ZoningSystems, optional
        Zoning system to output data in, NTEM zoning is default.
    version : ntem_constants.Versions, optional
        Version of NTEM data to use, version 8.0 by default
    filter_zoning_system : ntem_constants.ZoningSystems | None, optional
        Zoning system to filter by, if None no spatial filter is performed.
    filter_zone_names : list[str] | None, optional
        Zones to filter for, if None no spatial filter is performed.
    """

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
            *years,
            scenario=scenario,
            output_zoning=output_zoning,
            version=version,
            filter_zoning_system=filter_zoning_system,
            filter_zone_names=filter_zone_names,
        )

    def query(
        self, conn: sqlalchemy.Connection, include_zone_name: bool = False
    ) -> pd.DataFrame:
        """Query NTEM database for Car Ownership data using parameters defined on initialisation.

        Parameters
        ----------
        conn
            Connection to the database containing the NTEM data.
        include_zone_name
            If True, include "zone_name" column in output.

        Returns
        -------
        pd.DataFrame
        Car Ownership data with columns "zone", "year", "car_ownership_type", "value"
        """

        return self._data_query(conn, years=self._years, include_zone_name=include_zone_name)

    @_linear_interpolate
    def _data_query(
        self,
        conn: sqlalchemy.Connection,
        *,
        years: Iterable[int],
        include_zone_name: bool = False,
    ) -> pd.DataFrame:
        LOG.debug("Building car ownership query for year %s", years)

        data_filter = structure.CarOwnership.year.in_(years) & (
            structure.CarOwnership.metadata_id == self._metadata_id
        )

        if self._filter_zoning_system is not None and self._filter_zone_names is not None:
            data_filter &= structure.CarOwnership.zone_id.in_(
                _zone_subset(self._filter_zone_names, self._filter_zoning_system)
            )

        elif (self._filter_zoning_system is not None and self._filter_zone_names is None) or (
            self._filter_zoning_system is None and self._filter_zone_names is not None
        ):
            raise ValueError(
                "Both filter_zoning_system and filter_zone must be provided "
                "or neither provided if no spatial filter is to be performed."
            )

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id:
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
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning_id)
                    & (
                        structure.GeoLookup.from_zone_type_id
                        == ntem_constants.ZoningSystems.NTEM_ZONE.id
                    )
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning_id)
                )
                .group_by(
                    structure.Zones.id,
                    structure.CarOwnershipTypes.id,
                    structure.CarOwnership.year,
                )
            )
        LOG.debug("Running query")
        data = structure.query_to_dataframe(conn, query)
        LOG.debug("Query complete - post-processing data")
        if data["zone_code"].isna().any():
            data["zone"] = data["zone_name"]
            warnings.warn(
                "The zone system you have chosen to output does not have a code column. Outputting zone name instead"
            )
        else:
            data["zone"] = data["zone_code"]

        if include_zone_name:
            index_cols = ["zone", "zone_name", "year"]
        else:
            index_cols = ["zone", "year"]

        return data.pivot(
            index=index_cols,
            columns="car_ownership_type",
            values="value",
        )

    @property
    def name(self) -> str:  # noqa: D102
        # Docstring inherited
        return self._name


class TripEndByDirectionQuery(QueryParams):
    """Define and perform Trip End by Direction queries on the NTEM database.

    Parameters
    ----------
    years : int
        Years to provide data / interpolate.
    scenario : ntem_constants.Scenarios
        Scenario to provide data.
    output_zoning : ntem_constants.ZoningSystems, optional
        Zoning system to output data in, NTEM zoning is default.
    version : ntem_constants.Versions, optional
        Version of NTEM data to use, version 8.0 by default.
    filter_zoning_system : ntem_constants.ZoningSystems | None, optional
        Zoning system to filter by, if None no spatial filter is performed.
    filter_zone_names : list[str] | None, optional
        Zones to filter for, if None no spatial filter is performed.
    trip_type: ntem_constants.TripType, optional
        The trip type to retrieve.
    purpose_filter: list[ntem_constants.Purpose] | None, optional
        The purposes to filter the data by if None, no filter is performed.
    aggregate_purpose: bool
        Whether to aggregate purpose when retrieving data.
    mode_filter: list[ntem_constants.Mode] | None = None,
        The modes to filter the data by if None, no filter is performed.
    aggregate_mode: bool = True,
        Whether to aggregate mode when retrieving data.
    time_period_filter: list[ntem_constants.TimePeriod] | None = None,
        The time periods to filter the data by if None, no filter is performed.
    output_names: bool = True,
        Whether to convert the segmentation IDs to names after outputting.
    """

    def __init__(  # pylint: disable = too-many-arguments
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
        # TODO(KF) Sensible way to batch up some of theses args?
        # is this fine because most are optional?

        # Pylint does not seem to be able to interpret multiline strings.
        if label is None:
            self._name: str = f"trip_ends_{trip_type.value}_{scenario.value}_{version.value}"
        else:
            self._name = (
                f"trip_ends_{trip_type.value}_{label}_{scenario.value}_{version.value}"
            )

        super().__init__(
            *year,
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

        if time_period_filter is not None:
            self._time_period_filter = [tp.id() for tp in time_period_filter]

    @property
    def name(self) -> str:  # noqa: D102
        # Docstring inherited
        return self._name

    def query(
        self, conn: sqlalchemy.Connection, include_zone_name: bool = False
    ) -> pd.DataFrame:
        """Query NTEM database for Trip End by Direction data using parameters defined on initialisation.

        Note the outputs are total time period e.g AM is 3hr 7AM-10AM.

        Parameters
        ----------
        conn
            Connection to the database containing the NTEM data.
        include_zone_name
            If True, include "zone_name" column in output.

        Returns
        -------
        pd.DataFrame
        Trip End by Direction data with columns "zone", "year", "time_period",
        "mode" (if aggregate_mode was set to False), "purpose" (if aggregate_purpose was set to False)
        and "value"
        """
        data = self._data_query(conn, years=self._years)

        data = self._apply_lookups(
            data, conn, self._replace_names, include_zone_name=include_zone_name
        )

        return data

    def query_to_dvec(self, conn: sqlalchemy.Connection) -> dict[int, dict[str, base.DVector]]:
        """Produce Dvectors containing trip end by direction data.

        Note the outputs are total time period e.g AM is 3hr 7AM-10AM.

        Parameters
        ----------
        conn
            Connection to the database containing the NTEM data.

        Returns
        -------
        dict[int, dict[str, base.DVector]]:
            Trip end by direction data formatted as dict[year, dict[trip_type, data]].
        """

        data = self._data_query(conn, years=self._years)

        data = self._apply_lookups(data, conn, False)

        data.index = data.index.rename({"time_period": "tp", "purpose": "p", "mode": "m"})

        seg_names, subsets = self._segmentation

        segmentation = base.Segmentation(
            base.SegmentationInput(
                enum_segments=[base.segments.SegmentsSuper(e) for e in seg_names],
                naming_order=seg_names,
                subsets=subsets,
            )
        )

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id:
            zoning = base.ZoningSystem.get_zoning("ntem")
        elif self._output_zoning_id == ntem_constants.ZoningSystems.REGION.id:
            zoning = base.ZoningSystem.get_zoning("ntem_region")
        elif self._output_zoning_id == ntem_constants.ZoningSystems.AUTHORITY.id:
            zoning = base.ZoningSystem.get_zoning("ntem_authority")
        else:
            raise NotImplementedError(
                "Query to dvec does not support the output zoning system selected."
            )

        outputs = {}

        for year in self._years:
            year_output = {}
            for col in data.columns:
                year_output[col] = base.DVector(
                    import_data=data.xs(year, level="year")[col].unstack(level="zone"),
                    segmentation=segmentation,
                    zoning_system=zoning,
                )
            outputs[year] = year_output

        return outputs

    @property
    def _segmentation(self) -> tuple[list[str], dict[str, list[int]]]:
        seg = ["tp"]
        seg_subset = {}

        if self._time_period_filter is not None:
            seg_subset["tp"] = self._time_period_filter

        if not self._aggregate_purpose:
            seg.insert(0, "p")
            if self._purpose_filter is not None:
                seg_subset["p"] = self._purpose_filter

        if not self._aggregate_mode:
            seg.insert(0, "m")
            if self._mode_filter is not None:
                seg_subset["m"] = self._mode_filter

        return seg, seg_subset

    def _apply_lookups(
        self,
        data: pd.DataFrame,
        conn: sqlalchemy.Connection,
        replace_ids: bool,
        include_zone_name: bool = False,
    ) -> pd.DataFrame:
        LOG.debug("Applying lookups")
        data_values = data.copy()

        replacements: dict[str, dict[int, str]] = {}

        zones_lookup = structure.query_to_dataframe(
            conn,
            sqlalchemy.select(
                structure.Zones.id.label("id"),
                structure.Zones.source_id_or_code.label("name"),
            ).where(structure.Zones.zone_type_id == self._output_zoning_id),
            index_columns=["id"],
        )

        if not zones_lookup["name"].isna().any():
            replacements["zone"] = zones_lookup["name"].to_dict()
        else:
            warnings.warn(
                "The zone system you have chosen to output does not have a code column. Outputting zone name instead"
            )
            replacements["zone"] = structure.query_to_dataframe(
                conn,
                sqlalchemy.select(
                    structure.Zones.id.label("id"), structure.Zones.name.label("name")
                ).where(structure.Zones.zone_type_id == self._output_zoning_id),
                index_columns=["id"],
            )

        if replace_ids:
            replacements["time_period"] = structure.query_to_dataframe(
                conn,
                sqlalchemy.select(
                    structure.TimePeriodTypes.id.label("id"),
                    structure.TimePeriodTypes.name.label("name"),
                ),
                index_columns=["id"],
            )["name"].to_dict()

            if not self._aggregate_purpose:
                replacements["purpose"] = structure.query_to_dataframe(
                    conn,
                    sqlalchemy.select(
                        structure.PurposeTypes.id.label("id"),
                        structure.PurposeTypes.name.label("name"),
                    ),
                    index_columns=["id"],
                )["name"].to_dict()

            if not self._aggregate_mode:
                replacements["mode"] = structure.query_to_dataframe(
                    conn,
                    sqlalchemy.select(
                        structure.ModeTypes.id.label("id"),
                        structure.ModeTypes.name.label("name"),
                    ),
                    index_columns=["id"],
                )["name"].to_dict()

        if include_zone_name:
            data_values = _insert_zone_names(conn, data_values, self._output_zoning_id)

        for level, lookup in replacements.items():
            data_values = data_values.rename(index=lookup, level=level)

        return data_values

    @_linear_interpolate
    def _data_query(  # pylint: disable = too-many-branches
        self,
        conn: sqlalchemy.Connection,
        *,
        years: Iterable[int],
    ) -> pd.DataFrame:
        # TODO(KF) tidy/split this up to reduce number of branches
        LOG.debug("Building trip end by direction query for year %s", years)
        select_cols = [
            structure.TripType.name.label("trip_type"),
            structure.TripEndDataByDirection.time_period,
            structure.TripEndDataByDirection.year,
        ]

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id and not (
            self._aggregate_mode or self._aggregate_purpose
        ):
            select_cols.append(
                (
                    structure.TripEndDataByDirection.value
                    / structure.TimePeriodTypes.divide_by
                ).label("value")
            )

        else:
            select_cols.append(
                sqlalchemy.func.sum(
                    structure.TripEndDataByDirection.value
                    / structure.TimePeriodTypes.divide_by
                ).label("value")
            )

        index_cols = [
            "zone",
            "time_period",
            "year",
        ]

        groupby_cols = [
            structure.TripEndDataByDirection.time_period,
            structure.TripEndDataByDirection.year,
            structure.TripEndDataByDirection.trip_type,
        ]

        if not self._aggregate_purpose:
            select_cols.append(structure.TripEndDataByDirection.purpose)
            groupby_cols.append(structure.TripEndDataByDirection.purpose)
            index_cols.append("purpose")

        if not self._aggregate_mode:
            select_cols.append(structure.TripEndDataByDirection.mode)
            groupby_cols.append(structure.TripEndDataByDirection.mode)
            index_cols.append("mode")

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id:
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

        query = (
            sqlalchemy.select(*select_cols)
            .join(
                structure.TimePeriodTypes,
                structure.TimePeriodTypes.id == structure.TripEndDataByDirection.time_period,
                isouter=True,
            )
            .join(
                structure.TripType,
                structure.TripType.id == structure.TripEndDataByDirection.trip_type,
            )
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

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id:
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
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning_id)
                    & (structure.Zones.id == structure.GeoLookup.to_zone_id)
                )
                .group_by(*groupby_cols)
            )
        LOG.debug("Running query")
        data = structure.query_to_dataframe(conn, query)
        LOG.debug("Query complete")

        return data.pivot(
            index=index_cols,
            columns="trip_type",
            values="value",
        )


class TripEndByCarAvailabilityQuery(QueryParams):
    """Define and perform Trip End by Direction queries on the NTEM database.

    Parameters
    ----------
    years : int
        Years to provide data / interpolate.
    scenario : ntem_constants.Scenarios
        Scenario to provide data.
    output_zoning : ntem_constants.ZoningSystems, optional
        Zoning system to output data in, NTEM zoning is default.
    version : ntem_constants.Versions, optional
        Version of NTEM data to use, version 8.0 by default.
    filter_zoning_system : ntem_constants.ZoningSystems | None, optional
        Zoning system to filter by, if None no spatial filter is performed.
    filter_zone_names : list[str] | None, optional
        Zones to filter for, if None no spatial filter is performed.
    trip_type: ntem_constants.TripType, optional
        The trip type to retrieve.
    purpose_filter: list[ntem_constants.Purpose] | None, optional
        The purposes to filter the data by if None, no filter is performed.
    aggregate_purpose: bool
        Whether to aggregate purpose when retrieving data.
    mode_filter: list[ntem_constants.Mode] | None = None,
        The modes to filter the data by if None, no filter is performed.
    aggregate_mode: bool = True,
        Whether to aggregate mode when retrieving data.
    time_period_filter: list[ntem_constants.TimePeriod] | None = None,
        The time periods to filter the data by if None, no filter is performed.
    output_names: bool = True,
        Whether to convert the segmentation IDs to names after outputting.
    """

    def __init__(  # pylint: disable = too-many-arguments
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
        # TODO(KF) See above todo discussing batching inputs.
        # Pylint does not seem to be able to interpret multiline strings.
        if label is None:
            self._name: str = (
                f"trip_ends_by_car_availability_{years}" f"_{scenario.value}_{version.value}"
            )
        else:
            self._name = (
                f"trip_ends_by_car_availability{label}"
                f"_{years}_{scenario.value}_{version.value}"
            )

        super().__init__(
            *years,
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
    def name(self) -> str:  # noqa: D102
        # Docstring inherited
        return self._name

    def query(
        self, conn: sqlalchemy.Connection, include_zone_name: bool = False
    ) -> pd.DataFrame:
        """Query NTEM database for Trip End by Car Availability data using parameters defined on initialisation.

        Output values are weekly total trips.

        Parameters
        ----------
        conn
            Connection to the database containing the NTEM data.
        include_zone_name
            If True, include "zone_name" column in output.

        Returns
        -------
        pd.DataFrame
        Trip End by Car Availability with columns "zone", "year", "car_availability_type",
        "mode" (if aggregate_mode was set to False), "purpose" (if aggregate_purpose was set to False)
        and "value"
        """

        data = self._data_query(conn, years=self._years)

        data = self._apply_lookups(
            data, conn, self._replace_names, include_zone_name=include_zone_name
        )

        return data

    def _apply_lookups(
        self,
        data: pd.DataFrame,
        conn: sqlalchemy.Connection,
        replace_ids: bool,
        include_zone_name: bool = False,
    ) -> pd.DataFrame:
        LOG.debug("Applying lookups")
        data_values = data.copy()

        replacements: dict[str, dict[int, str]] = {}

        zones_lookup = structure.query_to_dataframe(
            conn,
            sqlalchemy.select(
                structure.Zones.id.label("id"),
                structure.Zones.source_id_or_code.label("name"),
            ).where(structure.Zones.zone_type_id == self._output_zoning_id),
            index_columns=["id"],
        )

        if not zones_lookup["name"].isna().any():
            replacements["zone"] = zones_lookup["name"].to_dict()
        else:
            warnings.warn(
                "The zone system you have chosen to output does not have a code column. Outputting zone name instead"
            )
            replacements["zone"] = structure.query_to_dataframe(
                conn,
                sqlalchemy.select(
                    structure.Zones.id.label("id"), structure.Zones.name.label("name")
                ).where(structure.Zones.zone_type_id == self._output_zoning_id),
                index_columns=["id"],
            )

        if replace_ids:
            replacements["car_availability_type"] = structure.query_to_dataframe(
                conn,
                sqlalchemy.select(
                    structure.CarAvailabilityTypes.id.label("id"),
                    structure.CarAvailabilityTypes.name.label("name"),
                ),
                index_columns=["id"],
            )["name"].to_dict()

            if not self._aggregate_purpose:
                replacements["purpose"] = structure.query_to_dataframe(
                    conn,
                    sqlalchemy.select(
                        structure.PurposeTypes.id.label("id"),
                        structure.PurposeTypes.name.label("name"),
                    ),
                    index_columns=["id"],
                )["name"].to_dict()

            if not self._aggregate_mode:
                replacements["mode"] = structure.query_to_dataframe(
                    conn,
                    sqlalchemy.select(
                        structure.ModeTypes.id.label("id"),
                        structure.ModeTypes.name.label("name"),
                    ),
                    index_columns=["id"],
                )["name"].to_dict()

        if include_zone_name:
            data_values = _insert_zone_names(conn, data_values, self._output_zoning_id)

        for col, lookup in replacements.items():
            data_values = data_values.rename(index=lookup, level=col)

        return data_values

    @_linear_interpolate
    def _data_query(  # pylint: disable = too-many-branches
        self,
        conn: sqlalchemy.Connection,
        *,
        years: Iterable[int],
    ) -> pd.DataFrame:
        LOG.debug("Building trip end car availability query for year %s", years)
        # TODO(KF) tidy/split this up to reduce number of branches

        index_cols: list[str] = ["zone", "car_availability_type", "year"]

        select_cols: list[sqlalchemy.Label] = [
            structure.TripEndDataByCarAvailability.car_availability_type.label(
                "car_availability_type"
            ),
            structure.TripEndDataByCarAvailability.year.label("year"),
        ]

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id and not (
            self._aggregate_mode or self._aggregate_purpose
        ):
            select_cols.append(structure.TripEndDataByCarAvailability.value.label("value"))

        else:
            select_cols.append(
                sqlalchemy.func.sum(structure.TripEndDataByCarAvailability.value).label(
                    "value"
                )
            )

        groupby_cols = [
            structure.TripEndDataByCarAvailability.car_availability_type,
            structure.TripEndDataByCarAvailability.year,
        ]

        if not self._aggregate_purpose:
            index_cols.append("purpose")
            select_cols.insert(
                0, structure.TripEndDataByCarAvailability.purpose.label("purpose")
            )
            groupby_cols.append(structure.TripEndDataByCarAvailability.purpose)

        if not self._aggregate_mode:
            index_cols.append("mode")
            select_cols.insert(0, structure.TripEndDataByCarAvailability.mode.label("mode"))
            groupby_cols.append(structure.TripEndDataByCarAvailability.mode)

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id:
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

        if self._output_zoning_id == ntem_constants.ZoningSystems.NTEM_ZONE.id:
            query = query.where(base_filter)

            if self._aggregate_mode or self._aggregate_purpose:
                query = query.group_by(*groupby_cols)

        else:
            query = (
                query.join(
                    structure.GeoLookup,
                    structure.GeoLookup.from_zone_id
                    == structure.TripEndDataByCarAvailability.zone_id,
                    isouter=True,
                )
                .where(
                    base_filter
                    & (
                        structure.GeoLookup.from_zone_id
                        == structure.TripEndDataByCarAvailability.zone_id
                    )
                    & (
                        structure.GeoLookup.from_zone_type_id
                        == ntem_constants.ZoningSystems.NTEM_ZONE.id
                    )
                    & (structure.GeoLookup.to_zone_type_id == self._output_zoning_id)
                    & (structure.Zones.id == structure.GeoLookup.to_zone_id)
                )
                .group_by(*groupby_cols)
            )
        LOG.debug("Running query")
        data = structure.query_to_dataframe(conn, query, index_columns=index_cols)
        LOG.debug("Query complete")
        return data


def _interpolation_years(year) -> tuple[int, int] | None:
    """Calculate years required for interpolation."""

    if year in ntem_constants.NTEM_YEARS:
        return None

    upper_year = int(ntem_constants.NTEM_YEARS[ntem_constants.NTEM_YEARS > year].min())
    lower_year = int(ntem_constants.NTEM_YEARS[ntem_constants.NTEM_YEARS < year].max())

    return (lower_year, upper_year)


def _zone_subset(zone_names: list[str], zoning_id: int) -> sqlalchemy.Select:
    """Query which returns the subset of zones."""
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


def _insert_zone_names(conn: sqlalchemy.Connection, data: pd.DataFrame, zone_system: int):
    """Add zone names as an index level."""
    level_name = "zone"
    levels = list(data.index.names)
    zone_name = "zone_name"
    levels.insert(levels.index(level_name) + 1, zone_name)

    stmt = sqlalchemy.select(structure.Zones.id, structure.Zones.name).where(
        structure.Zones.zone_type_id == zone_system
    )

    result = conn.execute(stmt)
    # Comprehension doesn't work correctly so comprehension is needed
    replace = {i: j for i, j in result.tuples()}  # pylint: disable=unnecessary-comprehension
    names = data.index.get_level_values(level_name).to_series().replace(replace)

    data = data.set_index(pd.Index(names, name=zone_name), append=True)
    return data.reorder_levels(levels)
