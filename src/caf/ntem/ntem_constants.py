# Built-Ins
import abc
import dataclasses
import enum
import pathlib

# Third Party
import caf.toolkit as ctk
import pydantic


class InputBase(ctk.BaseConfig, abc.ABC):
    """Base class for input parameters."""

    output_path: pathlib.Path = pydantic.Field(description="Path to the output directory.")

    @abc.abstractmethod
    def run(self):
        """Run the relavent function."""


class Scenarios(enum.Enum):
    """Defined valid NTEM scenarios."""

    CORE: str = "Core"
    HIGH: str = "High"
    LOW: str = "Low"
    REGIONAL: str = "Regional"
    BEHAVIOURAL: str = "Behavioural"
    TECHNOLOGY: str = "Technology"


class AccessTables(enum.Enum):
    """Defines the names of the access data tables."""

    PLANNING: str = "Planning"
    CAR_OWNERSHIP: str = "CarOwnership"
    TE_CAR_AVAILABILITY: str = "TripEndDataByCarAvailability"
    TE_DIRECTION: str = "TripEndDataByDirection"
