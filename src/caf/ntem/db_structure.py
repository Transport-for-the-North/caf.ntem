"""Defines the stuctures of the database tables."""

# Third Party
import sqlalchemy as sa
from sqlalchemy import orm


class Base(orm.DeclarativeBase):
    pass


