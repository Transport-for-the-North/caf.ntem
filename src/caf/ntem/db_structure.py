"""Defines the stuctures of the database tables."""

from __future__ import annotations

# Built-Ins
import pathlib

# Third Party
import sqlalchemy as sqlalchemy
from sqlalchemy import orm


def connection_string(output_path: pathlib.Path) -> str:
    """Creates a connection string to the database."""
    return f"sqlite:///{output_path.resolve()}"


def schema_connection_string(output_path: pathlib.Path) -> str:
    """Creates a connection string to the database."""
    return f"ATTACH DATABASE {output_path.resolve()} AS ntem"


class Base(orm.DeclarativeBase):
    pass
    # __table_args__ = {"schema": "ntem"}


class MetaData(Base):
    __tablename__ = "metadata"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    share_type_id: orm.Mapped[int | None]
    version: orm.Mapped[str]
    scenario: orm.Mapped[str]


class PlanningDataTypes(Base):
    __tablename__ = "planning_data_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class CarOwnershipTypes(Base):
    __tablename__ = "car_ownership_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class CarAvailabilityTypes(Base):
    __tablename__ = "car_availability_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class PurposeTypes(Base):
    __tablename__ = "purpose_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class ModeTypes(Base):
    __tablename__ = "mode_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class TimePeriodTypes(Base):
    __tablename__ = "time_period_types"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    divide_by: orm.Mapped[int]
    name: orm.Mapped[str]


class TripType(Base):
    __tablename__ = "trip_type"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class Region(Base):
    __tablename__ = "region"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    abbreviation: orm.Mapped[str]
    name: orm.Mapped[str]


class Authority(Base):
    __tablename__ = "authority"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    code: orm.Mapped[str]
    name: orm.Mapped[str]


class County(Base):
    __tablename__ = "county"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]


class Years(Base):
    __tablename__ = "years"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    code: orm.Mapped[str]
    year: orm.Mapped[int]


class GeoLookup(Base):
    __tablename__ = "geo_lookup"
    zone_id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    zone_name: orm.Mapped[str]
    zone_code: orm.Mapped[str]
    authority_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(Authority.id))
    county_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(County.id))
    region_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(Region.id))


class TripEndDataByCarAvailability(Base):
    __tablename__ = "trip_end_data_by_car_availability"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    metadata_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(MetaData.id))
    zone_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(GeoLookup.zone_id))
    zone_type_id: orm.Mapped[int]
    purpose: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(PurposeTypes.id))
    mode: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(ModeTypes.id))
    car_availability_type: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey(CarAvailabilityTypes.id)
    )
    year: orm.Mapped[int]
    value: orm.Mapped[float]


class TripEndDataByDirection(Base):
    __tablename__ = "trip_end_data_by_direction"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    metadata_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(MetaData.id))
    zone_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(GeoLookup.zone_id))
    zone_type_id: orm.Mapped[int]
    purpose: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(PurposeTypes.id))
    mode: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(ModeTypes.id))
    time_period: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(TimePeriodTypes.id))
    trip_type: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(TripType.id))
    year: orm.Mapped[int]
    value: orm.Mapped[float]


class CarOwnership(Base):
    __tablename__ = "car_ownership"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    metadata_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(MetaData.id))
    zone_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(GeoLookup.zone_id))
    zone_type_id: orm.Mapped[int]
    car_ownership_type: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey(CarOwnershipTypes.id)
    )
    year: orm.Mapped[int]
    value: orm.Mapped[float]


class Planning(Base):
    __tablename__ = "planning"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    metadata_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(MetaData.id))
    zone_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.ForeignKey(GeoLookup.zone_id))
    zone_type_id: orm.Mapped[int]
    planning_data_type: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey(PlanningDataTypes.id)
    )
    year: orm.Mapped[int]
    value: orm.Mapped[float]


DB_TO_ACCESS_TABLE_LOOKUP: dict[type[Base], str] = {
    CarAvailabilityTypes: "tblLookUpCarAvailability",
    CarOwnershipTypes: "tblLookUpCarOwnershipType",
    ModeTypes: "tblLookUpTransport",
    PurposeTypes: "tblLookUpTripPurpose",
    TimePeriodTypes: "tblLookUpTimePeriod",
    TripType: "tblLookUpTEType",
    PlanningDataTypes: "tblLookUpPlanning83",
    Region: "tblLookUpRegion",
    Authority: "tblLookUpAuthority82",
    County: "tblLookUpCounty83",
    GeoLookup: "tblLookupGeo83",
    Planning: "Planning",
    Years: "tblLookUpYrs83",
}

ACCESS_TO_DB_COLUMNS: dict[type[Base], dict[str, str]] = {
    CarAvailabilityTypes: {
        "CarAvID": "id",
        "CarAvDesc": "name",
    },
    CarOwnershipTypes: {
        "CarOwnID": "id",
        "CarOwnDesc": "name",
    },
    PurposeTypes: {
        "PurposeID": "id",
        "PurposeDesc": "name",
    },
    ModeTypes: {
        "TransportID": "id",
        "TransportDesc": "name",
    },
    TimePeriodTypes: {
        "TimePeriodID": "id",
        "DivideBy": "divide_by",
        "TimePeriodDesc": "name",
    },
    TripType: {
        "TEtypeID": "id",
        "TEtypeDesc": "name",
    },
    Years: {
        "YearID": "id",
        "YrNo": "year",
    },
    PlanningDataTypes: {
        "PlanID": "id",
        "PlanDesc": "name",
    },
    Region: {
        "RegionID": "id",
        "LongRegionName": "name",
        "RegionName": "abbreviation",
    },
    Authority: {
        "AuthorityID": "id",
        "AuthorityName": "name",
        "ControlAreaID": "code",
    },
    County: {
        "CountyID": "id",
        "CountyName": "name",
    },
    GeoLookup: {
        "TemproZoneID": "zone_id",
        "ZoneName": "zone_name",
        "RegionID": "region_id",
        "AuthorityID": "authority_id",
        "CountyID": "county_id",
        "NTEM7ZoneCode": "zone_code",
    },
}

LOOKUP_TABLES: list[type[Base]] = [
    CarAvailabilityTypes,
    CarOwnershipTypes,
    PurposeTypes,
    TimePeriodTypes,
    TripType,
    #    Years,
    PlanningDataTypes,
    Region,
    Authority,
    County,
    GeoLookup,
]
