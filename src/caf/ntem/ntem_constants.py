
import dataclasses
import pathlib


import caf.toolkit as ctk

@dataclasses.dataclass
class Scenarios:
    core: str= "Core"
    high: str= "High"
    low: str= "Low"
    regional: str= "Regional"
    behavioural: str= "Behavioural"
    technology: str= "Technology"

class NtemConstants(ctk.BaseConfig):
    # The directory where the data is stored
    scenarios: Scenarios = Scenarios()


def get_constants(path: pathlib.Path|None) -> NtemConstants:
    """Get constants for the caf.ntem module.

    Parameters
    ----------
    path : pathlib.Path | None
        if None then return the default constants, otherwise load the constants from the path.

    Returns
    -------
    NtemConstants
        Constants for the caf.ntem module.
    """    
    if path is None:
        return NtemConstants()
    
    return NtemConstants.load_yaml(path)