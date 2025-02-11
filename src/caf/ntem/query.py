import pathlib
import pandas as pd
import pydantic
import sqlalchemy
from sqlalchemy import orm

from caf.ntem import structure, ntem_constants


class QueryArgs(ntem_constants.InputBase):

    output_path: pathlib.Path = pydantic.Field(description="Path to the output directory.")
    """Path to directory to output the processed NTEM data."""

    db_path: pydantic.FilePath = pydantic.Field(description="Path to NTEM database.")
    """Path to NTEM database, which has been outputted by the build module."""

    @property
    def logging_path(self) -> pathlib.Path:
        return self.output_path / "caf_ntem.log"

    def run(self) -> None:
        perform_query(self)


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

def perform_query(query_args: QueryArgs):

    engine = sqlalchemy.create_engine(structure.connection_string(query_args.db_path))
    with sqlalchemy.Connection(engine) as connection:

        zones = pd.read_sql(
            _zone_subset(["Cheshire West and Chester", "Cheshire East"], 2), connection
        )
        print("STAP")
