"""Defines the stuctures of the database tables."""

# Third Party
import pathlib
import sqlalchemy as sa
from sqlalchemy import orm


def connection_string(output_path: pathlib.Path) -> str:
    """Creates a connection string to the database."""
    return f"sqlite:///{output_path.resolve()}"


class Base(orm.DeclarativeBase):
    pass

class MetaData(Base):
    __tablename__ = "metadata"
    id:orm.Mapped[int] = orm.mapped_column(primary_key=True)
    share_type_id: orm.MappedColumn[int|None]
    version:orm.MappedColumn[str]
    scenario: orm.MappedColumn[str]

class PlanningDataTypes(Base):
    __tablename__ = "planning_data_types"
    id = orm.mapped_column( primary_key=True)
    name: orm.MappedColumn[str]

class CarOwnershipTypes(Base):
    __tablename__ = "car_ownership_types"
    id:orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    name: orm.MappedColumn[str]

class PurposeTypes(Base):
    __tablename__ = "purpose_types"
    id:orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    name: orm.MappedColumn[str]

class ModeTypes(Base):
    __tablename__ = "mode_types"
    id:orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    name: orm.MappedColumn[str]

class TimePeriodTypes(Base):
    __tablename__ = "time_period_types"
    id:orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    name: orm.MappedColumn[str]

class TripType(Base):
    __tablename__ = "trip_type"
    id:orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    name: orm.MappedColumn[str]

class Years(Base):
    __tablename__ = "years"
    id:orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    year: orm.MappedColumn[int]

class TripEndDataByCarAvailability(Base):
    __tablename__ = "trip_end_data_by_car_availability"
    metadata_id: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("metadata.id"))
    zone_id: orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    zone_type_id: orm.MappedColumn[int]
    purpose: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("purpose_types.id"))
    mode: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("mode_types.id"))
    car_ownership_type: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("car_ownership_types.id"))
    year: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("years.id"))
    value: orm.MappedColumn[float]

class TripEndDataByDirection(Base):
    __tablename__ = "trip_end_data_by_direction"
    metadata_id: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("metadata.id"))
    zone_id: orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    zone_type_id: orm.MappedColumn[int]
    purpose: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("purpose_types.id"))
    mode: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("mode_types.id"))
    time_period: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("time_period_types.id"))
    trip_type: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("trip_type.id"))
    year: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("years.id"))
    value: orm.MappedColumn[float]

class CarOwnership(Base):
    __tablename__ = "car_owner  ship"
    metadata_id: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("metadata.id"))
    zone_id: orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    zone_type_id: orm.MappedColumn[int]
    car_ownership_type: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("car_ownership_types.id"))
    year: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("years.id"))
    value: orm.MappedColumn[float]

class Planning(Base):
    __tablename__ = "planning"
    metadata_id: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("metadata.id"))
    zone_id: orm.MappedColumn[int] = orm.mapped_column(primary_key=True)
    zone_type_id: orm.MappedColumn[int]
    planning_data_type: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("planning_data_types.id"))
    year: orm.MappedColumn[int] = orm.mapped_column(sa.ForeignKey("years.id"))
    value: orm.MappedColumn[float]