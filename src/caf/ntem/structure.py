"""Defines the structures of the database tables."""

from __future__ import annotations

# Built-Ins
import dataclasses
import pathlib
from typing import Optional

# Third Party
import pandas as pd
import sqlalchemy
from sqlalchemy import orm

# Almost all classes in here are to define the DB structure,
# so have no public methods as a subclass of DeclarativeBase.
# Therefore I am turning off this warning for this module.
# pylint: disable = too-few-public-methods


def connection_string(path: pathlib.Path, driver_name: str = "sqlite") -> sqlalchemy.URL:
    """Create a connection string to the database."""
    return sqlalchemy.URL.create(
        drivername=driver_name,
        database=str(path.resolve()),
    )


def schema_connection_string(output_path: pathlib.Path) -> str:
    """Create a connection string to the database."""
    return f"ATTACH DATABASE {output_path.resolve()} AS ntem"


def query_to_dataframe(
    conn: sqlalchemy.Connection,
    query: sqlalchemy.Selectable,
    *,
    column_names: dict[str, str] | None = None,
    index_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Query database using an sqlalchemy query and returns a dataframe."""

    data = pd.read_sql(query, conn)

    if column_names is not None:
        data = data.rename(columns=column_names)

    if index_columns is not None:
        data = data.set_index(index_columns)

    return data


class Base(orm.DeclarativeBase):
    """Base class for metadata tables."""

    # __table_args__ = {"schema": "ntem"}


class MetaData(Base):
    """Metadata table for the database."""

    __tablename__ = "metadata"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    share_type_id: orm.Mapped[Optional[int]]
    version: orm.Mapped[str]
    scenario: orm.Mapped[str]


class PlanningDataTypes(Base):
    """Lookup Table for planning data types."""

    __tablename__ = "planning_data_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class CarOwnershipTypes(Base):
    """Lookup Table for car ownership types."""

    __tablename__ = "car_ownership_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class CarAvailabilityTypes(Base):
    """Lookup Table for car availability types."""

    __tablename__ = "car_availability_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class PurposeTypes(Base):
    """Lookup Table for purpose types."""

    __tablename__ = "purpose_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class ModeTypes(Base):
    """Lookup Table for mode types."""

    __tablename__ = "mode_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class TimePeriodTypes(Base):
    """Lookup Table for time period types."""

    __tablename__ = "time_period_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    divide_by: orm.Mapped[int]
    name: orm.Mapped[str]


class TripType(Base):
    """Lookup Table for trip types."""

    __tablename__ = "trip_type"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class ZoneType(Base):
    """Zone system table."""

    __tablename__ = "zone_type_list"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    name: orm.Mapped[str]
    source: orm.Mapped[str]
    version: orm.Mapped[str]


class Zones(Base):
    """Zoning definition table."""

    __tablename__ = "zones"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    zone_type_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey(ZoneType.id), primary_key=True
    )
    name: orm.Mapped[str]
    source_id_or_code: orm.Mapped[Optional[str]]


class GeoLookup(Base):
    """Lookup Table between zoning systems."""

    __tablename__ = "geo_lookup"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    from_zone_id: orm.Mapped[int]
    from_zone_type_id: orm.Mapped[str]
    to_zone_id: orm.Mapped[int]
    to_zone_type_id: orm.Mapped[str]

    __table_args__ = (  # type: ignore[var-annotated]
        # defining composite foreign key constraints - have to defined in table args
        # because you cannot defined composite foreign key constraints on each mapped_column
        sqlalchemy.ForeignKeyConstraint(
            ["from_zone_id", "from_zone_type_id"], [Zones.id, Zones.zone_type_id]
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["to_zone_id", "to_zone_type_id"], [Zones.id, Zones.zone_type_id]
        ),
        {},
    )


class TripEndDataByCarAvailability(Base):
    """Table for trip end data by car availability."""

    __tablename__ = "trip_end_data_by_car_availability"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    metadata_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(MetaData.id))
    zone_id: orm.Mapped[int]
    zone_type_id: orm.Mapped[int]
    purpose: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(PurposeTypes.id))
    mode: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(ModeTypes.id))
    car_availability_type: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey(CarAvailabilityTypes.id)
    )
    year: orm.Mapped[int]
    value: orm.Mapped[float]
    __table_args__ = (  # type: ignore[var-annotated]
        # defining composite foreign key constraints - have to defined in table args
        # because you cannot defined composite foreign key constraints on each mapped_column
        sqlalchemy.ForeignKeyConstraint(
            ["zone_id", "zone_type_id"], [Zones.id, Zones.zone_type_id]
        ),
        {},
    )


class TripEndDataByDirection(Base):
    """Table for trip end data by direction."""

    __tablename__ = "trip_end_data_by_direction"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    metadata_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(MetaData.id))
    zone_id: orm.Mapped[int]
    zone_type_id: orm.Mapped[int]
    purpose: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(PurposeTypes.id))
    mode: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(ModeTypes.id))
    time_period: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(TimePeriodTypes.id))
    trip_type: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(TripType.id))
    year: orm.Mapped[int]
    value: orm.Mapped[float]
    __table_args__ = (  # type: ignore[var-annotated]
        # defining composite foreign key constraints - have to defined in table args
        # because you cannot defined composite foreign key constraints on each mapped_column
        sqlalchemy.ForeignKeyConstraint(
            ["zone_id", "zone_type_id"], [Zones.id, Zones.zone_type_id]
        ),
        {},
    )


class CarOwnership(Base):
    """Table for car ownership data."""

    __tablename__ = "car_ownership"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    metadata_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(MetaData.id))
    zone_id: orm.Mapped[int]
    zone_type_id: orm.Mapped[int]
    car_ownership_type: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey(CarOwnershipTypes.id)
    )
    year: orm.Mapped[int]
    value: orm.Mapped[float]
    __table_args__ = (  # type: ignore[var-annotated]
        # defining composite foreign key constraints - have to defined in table args
        # because you cannot defined composite foreign key constraints on each mapped_column
        sqlalchemy.ForeignKeyConstraint(
            ["zone_id", "zone_type_id"], [Zones.id, Zones.zone_type_id]
        ),
        {},
    )


class Planning(Base):
    """Table for planning data."""

    __tablename__ = "planning"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    metadata_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(MetaData.id))
    zone_id: orm.Mapped[int]
    zone_type_id: orm.Mapped[int]
    planning_data_type: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey(PlanningDataTypes.id)
    )
    year: orm.Mapped[int]
    value: orm.Mapped[float]
    __table_args__ = (  # type: ignore[var-annotated]
        # defining composite foreign key constraints - have to defined in table args
        # because you cannot defined composite foreign key constraints on each mapped_column
        sqlalchemy.ForeignKeyConstraint(
            ["zone_id", "zone_type_id"], [Zones.id, Zones.zone_type_id]
        ),
        {},
    )


@dataclasses.dataclass
class NtemTripTypeLookup:
    """Table for Trip type lookup."""

    production_trip_end: int = 1
    attraction_trip_end: int = 2
    origin_trip_end: int = 3
    destination_trip_end: int = 4

    def to_dataframe(self) -> pd.DataFrame:
        """Convert lookup names and ids into a pandas DataFrame."""
        lookup = pd.Series(
            {int(value): str(key) for key, value in dataclasses.asdict(self).items()},
            name="name",
        ).to_frame()

        lookup.index.name = "id"
        return lookup.reset_index()


# -------------- build module definitions -------------
DB_TO_ACCESS_TABLE_LOOKUP: dict[str, str] = {
    CarAvailabilityTypes.__tablename__: "tblLookUpCarAvailability",
    CarOwnershipTypes.__tablename__: "tblLookUpCarOwnershipType",
    ModeTypes.__tablename__: "tblLookUpTransport",
    PurposeTypes.__tablename__: "tblLookUpTripPurpose",
    TimePeriodTypes.__tablename__: "tblLookUpTimePeriod",
    TripType.__tablename__: "NtemTripTypeLookup",
    PlanningDataTypes.__tablename__: "tblLookUpPlanning83",
    Planning.__tablename__: "Planning",
    "region": "tblLookupRegion",
    "county": "tblLookupCounty83",
    "authority": "tblLookupAuthority82",
    "ntem_zoning": "tblLookupGeo83",
}
"""Lookup between database tables and MS Access table names."""

ACCESS_TO_DB_COLUMNS: dict[str, dict[str, str]] = {
    CarAvailabilityTypes.__tablename__: {
        "CarAvID": "id",
        "CarAvDesc": "name",
    },
    CarOwnershipTypes.__tablename__: {
        "CarOwnID": "id",
        "CarOwnDesc": "name",
    },
    PurposeTypes.__tablename__: {
        "PurposeID": "id",
        "PurposeDesc": "name",
    },
    ModeTypes.__tablename__: {
        "TransportID": "id",
        "TransportDesc": "name",
    },
    TimePeriodTypes.__tablename__: {
        "TimePeriodID": "id",
        "DivideBy": "divide_by",
        "TimePeriodDesc": "name",
    },
    TripType.__tablename__: {
        "TEtypeID": "id",
        "TEtypeDesc": "name",
    },
    PlanningDataTypes.__tablename__: {
        "PlanID": "id",
        "PlanDesc": "name",
    },
    "region": {
        "RegionID": "ntem_zoning_id",
        "LongRegionName": "name",
        "RegionName": "source_id_or_code",
    },
    "county": {
        "CountyID": "ntem_zoning_id",
        "CountyName": "name",
    },
    "authority": {
        "AuthorityID": "ntem_zoning_id",
        "AuthorityName": "name",
        "ControlAreaID": "source_id_or_code",
    },
    "ntem_zoning": {
        "TemproZoneID": "ntem_zoning_id",
        "ZoneName": "name",
        "RegionID": "region_id",
        "AuthorityID": "authority_id",
        "CountyID": "county_id",
        "NTEM7ZoneCode": "source_id_or_code",
    },
}
"""Lookup between MS Access columns and database columns."""

LOOKUP_TABLES: list[type[Base]] = [
    CarAvailabilityTypes,
    CarOwnershipTypes,
    ModeTypes,
    PurposeTypes,
    TimePeriodTypes,
    TripType,
    PlanningDataTypes,
]
"""List of lookup tables."""

# -------------- query module definitions -------------
