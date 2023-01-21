from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from types import NoneType
from typing import Protocol

import numpy as np
import pandas as pd

RECIPES: dict[tuple, dict] = {
    ("pandas", "Timestamp", None): {
        "method": "as_str",
        "attributes": None,
        "keep": True,
        "constructor": ("pandas", "Timestamp", None),
    },
    ("numpy", "datetime64", None): {
        "method": "as_str",
        "attributes": None,
        "keep": True,
        "constructor": ("numpy", "datetime64", None),
    },
    ("pudl.workspace.datastore", "Datastore", None): {
        "method": None,
        "attributes": None,
        "keep": False,
        "constructor": ("pudl.workspace.datastore", "Datastore", None),
    },
    ("sqlalchemy.engine.base", "Engine", None): {
        "method": "by_attribute",
        "attributes": (("url", str),),
        "keep": True,
        "constructor": ("sqlalchemy", None, "create_engine"),
    },
}


class DZableObj(Protocol):
    """Protocol for an object that can be serialized with :class:`DataZip`."""

    def __getstate__(self) -> dict:
        ...

    def __setstate__(self, state: dict) -> None:
        ...


DZable = (
    complex
    | float
    | frozenset
    | int
    | set
    | str
    | Mapping
    | Sequence
    | DZableObj
    | datetime
    | Path
    | np.ndarray
    | pd.DataFrame
    | pd.Series
    | None
)

STD_TYPES = (
    complex,
    dict,
    frozenset,
    float,
    int,
    list,
    tuple,
    set,
    bool,
    NoneType,
    datetime,
    str,
    Path,
)
