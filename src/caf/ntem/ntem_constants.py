from __future__ import annotations

# Built-Ins
import abc
import dataclasses
import enum
import os
import pathlib
from typing import Any

# Third Party
import caf.toolkit as ctk
import numpy as np
import pydantic

_NTEM_ZONE_SYSTEM_ID: int = int(os.getenv("NTEM_ZONE_SYSTEM_ID", 1))
_AUTHORITY_SYSTEM_ID: int = int(os.getenv("AUTHORITY_ZONE_SYSTEM_ID", 2))
_COUNTY_SYSTEM_ID: int = int(os.getenv("COUNTY_ZONE_SYSTEM_ID", 3))
_REGION_SYSTEM_ID: int = int(os.getenv("REGION_ZONE_SYSTEM_ID", 4))

_NTEM_LOW_YEAR: int = int(os.getenv("NTEM_LOW_YEAR", 2011))
_NTEM_HIGH_YEAR: int = int(os.getenv("NTEM_HIGH_YEAR", 2061))
_NTEM_YEAR_STEP: int = int(os.getenv("NTEM_YEAR_STEP", 5))

NTEM_YEARS: np.ndarray = np.array(
    range(
        _NTEM_LOW_YEAR,
        _NTEM_HIGH_YEAR + 1,  # +1 as last value in range is N-1
        _NTEM_YEAR_STEP,
    )
)


class CaseInsensitiveEnum(str, enum.Enum):
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
        """Run the relavent function."""

    @property
    @abc.abstractmethod
    def logging_path(self) -> pathlib.Path:
        """Logging path for the sub command."""


class ZoningSystems(CaseInsensitiveEnum):
    NTEM_ZONE = "ntem_zone"
    AUTHORITY = "authority"
    COUNTY = "county"
    REGION = "region"

    @property
    def id(self) -> int:

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
    EIGHT = 8.0


class AccessTables(enum.Enum):
    """Defines the names of the access data tables."""

    PLANNING = "Planning"
    CAR_OWNERSHIP = "CarOwnership"
    TE_CAR_AVAILABILITY = "TripEndDataByCarAvailability"
    TE_DIRECTION = "TripEndDataByDirection"
