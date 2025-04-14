"""Stores and processes constants and global variables and classes for the package."""

from __future__ import annotations

# Built-Ins
import abc
import enum
import os
import pathlib
from typing import Any

# Third Party
import caf.toolkit as ctk
import numpy as np

# We have to set the default to str despite converting back to avoid pylint whinging
_NTEM_ZONE_SYSTEM_ID: int = int(os.getenv("NTEM_ZONE_SYSTEM_ID", "1"))
_AUTHORITY_SYSTEM_ID: int = int(os.getenv("AUTHORITY_ZONE_SYSTEM_ID", "2"))
_COUNTY_SYSTEM_ID: int = int(os.getenv("COUNTY_ZONE_SYSTEM_ID", "3"))
_REGION_SYSTEM_ID: int = int(os.getenv("REGION_ZONE_SYSTEM_ID", "4"))

_NTEM_LOW_YEAR: int = int(os.getenv("NTEM_LOW_YEAR", "2011"))
_NTEM_HIGH_YEAR: int = int(os.getenv("NTEM_HIGH_YEAR", "2061"))
_NTEM_YEAR_STEP: int = int(os.getenv("NTEM_YEAR_STEP", "5"))

NTEM_YEARS: np.ndarray = np.array(
    range(
        _NTEM_LOW_YEAR,
        _NTEM_HIGH_YEAR + 1,  # +1 as last value in range is N-1
        _NTEM_YEAR_STEP,
    )
)


class CaseInsensitiveEnum(str, enum.Enum):
    """Enum that allows for case insensitivity. All attribute values should be lowercase."""

    @classmethod
    def _missing_(cls, value: Any):
        if isinstance(value, str):
            value = value.lower()

            for member in cls:
                if member.lower() == value:
                    return member
        return None


class InputBase(ctk.BaseConfig, abc.ABC):
    """Base class for input parameters."""

    @abc.abstractmethod
    def run(self):
        """Run the relevant function."""

    @property
    @abc.abstractmethod
    def logging_path(self) -> pathlib.Path:
        """Logging path for the sub command."""


class BuildColumnNames(enum.Enum):
    """Column Names Needed for Building the NTEM database."""

    METADATA_ID = "metadata_id"
    """Column name for the metadata_id column."""
    ZONE_SYSTEM_ID = "zone_type_id"
    """Column name for the zone system id column."""
    ZONE_ID = "zone_id"
    """Column name for the zone id column."""


class Purpose(enum.IntEnum):
    """NTEM purposes, values are IDs."""

    HB_WORK = 1
    HB_EB = 2
    HB_EDUCATION = 3
    HB_SHOPPING = 4
    HB_PERSONAL = 5
    HB_SOCIAL = 6
    HB_VISITING = 7
    HB_HOLIDAY = 8
    NHB_WORK = 11
    NHB_EB = 12
    NHB_EDUCATION = 13
    NHB_SHOPPING = 14
    NHB_PERSONAL = 15
    NHB_SOCIAL = 16
    NHB_HOLIDAY = 18


class Mode(CaseInsensitiveEnum):
    """NTEM modes."""

    WALK = "walk"
    CYCLE = "cycle"
    CAR_DRIVER = "car_driver"
    CAR_PASSENGER = "car_passenger"
    BUS = "bus"
    RAIL = "rail"

    def id(self) -> int:
        """Database ID of the mode."""
        lookup = {
            Mode.WALK: 1,
            Mode.CYCLE: 2,
            Mode.CAR_DRIVER: 3,
            Mode.CAR_PASSENGER: 4,
            Mode.BUS: 5,
            Mode.RAIL: 6,
        }
        return lookup[self]


class TimePeriod(CaseInsensitiveEnum):
    """NTEM time periods."""

    AM = "am"
    IP = "ip"
    PM = "pm"
    OP = "op"
    SAT = "saturday"
    SUN = "sunday"
    AVG_WKDAY = "average_weekday"
    AVG_DAY = "average_day"

    def id(self) -> int:
        """Database ID of the time period."""
        lookup = {
            TimePeriod.AM: 1,
            TimePeriod.IP: 2,
            TimePeriod.PM: 3,
            TimePeriod.OP: 4,
            TimePeriod.SAT: 5,
            TimePeriod.SUN: 6,
            TimePeriod.AVG_WKDAY: 7,
            TimePeriod.AVG_DAY: 8,
        }
        return lookup[self]


class TripType(CaseInsensitiveEnum):
    """NTEM Trip Type."""

    PA = "pa"
    OD = "od"

    def id(self) -> list[int]:
        """Database ID of the trip type."""
        lookup = {
            TripType.PA: [1, 2],
            TripType.OD: [3, 4],
        }
        return lookup[self]


class ZoningSystems(CaseInsensitiveEnum):
    """NTEM zoning systems."""

    NTEM_ZONE = "ntem_zone"
    AUTHORITY = "authority"
    COUNTY = "county"
    REGION = "region"

    @property
    def id(self) -> int:
        """Database ID of the zoning system."""
        id_lookup: dict[str, int] = {
            ZoningSystems.NTEM_ZONE: _NTEM_ZONE_SYSTEM_ID,
            ZoningSystems.AUTHORITY: _AUTHORITY_SYSTEM_ID,
            ZoningSystems.COUNTY: _COUNTY_SYSTEM_ID,
            ZoningSystems.REGION: _REGION_SYSTEM_ID,
        }

        return id_lookup[self]


class Scenarios(CaseInsensitiveEnum):
    """Defined valid NTEM scenarios."""

    CORE = "core"
    HIGH = "high"
    LOW = "low"
    REGIONAL = "regional"
    BEHAVIOURAL = "behavioural"
    TECHNOLOGY = "technology"

    def id(self, version: Versions) -> int:
        """Database metadata ID of the scenario and version.

        Parameters
        ----------
        version : Versions
            Version to retrieve the ID for.

        Returns
        -------
        int
            ID of version and scenario
        """
        if version != Versions.EIGHT:
            raise NotImplementedError(
                f"Code base is not currently set up for versions other than {str(Versions.EIGHT.value)}"
            )

        id_lookup: dict[str, int] = {
            Scenarios.CORE: 5,
            Scenarios.HIGH: 1,
            Scenarios.LOW: 2,
            Scenarios.REGIONAL: 3,
            Scenarios.BEHAVIOURAL: 6,
            Scenarios.TECHNOLOGY: 4,
        }

        return id_lookup[self]


class Versions(enum.Enum):
    """NTEM versions."""

    EIGHT = "8.0"
