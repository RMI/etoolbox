from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Protocol

import numpy as np
import pandas as pd


class DZableObj(Protocol):
    """Protocol for an object that can be serialized with :class:`DataZip`."""

    def __getstate__(self) -> dict | tuple[dict | None, dict]: ...

    def __setstate__(self, state: dict | tuple[dict | None, dict]) -> None: ...


JSONABLE = float | int | dict | list | str | bool | None


DZable = (
    complex
    | frozenset
    | set
    | JSONABLE
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
