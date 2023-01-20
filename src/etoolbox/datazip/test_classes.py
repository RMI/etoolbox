"""Mocks and other objects for testing :class:`.IOWrapper`."""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Literal, NamedTuple

import numpy as np
import pandas as pd
import sqlalchemy as sa


class ObjMeta(NamedTuple):
    """NamedTuple for testing."""

    module: str
    qualname: str
    constructor: str | None = None


class KlassSlots:
    """Generic class with slots for testing."""

    __slots__ = ("foo", "_dfs", "tup", "lis")

    def __init__(self, **kwargs):
        """Init."""
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __setstate__(self, state):
        for k, v in state.items():
            if k in self.__slots__:
                setattr(self, k, v)

    def __getstate__(self):
        return {k: getattr(self, k) for k in self.__slots__}

    def __eq__(self, other):
        return all(
            (
                isinstance(other, self.__class__),
                all(
                    getattr(self, k) == getattr(other, k) for k in ("foo", "tup", "lis")
                ),
                all(
                    a.compare(b).empty
                    for a, b in zip(self._dfs.values(), other._dfs.values())
                ),
            )
        )


class TestKlass:
    """Generic class w/o slots for testing."""

    def __init__(self, **kwargs):
        """Init."""
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __setstate__(self, state):
        self.__dict__ = state

    def __getstate__(self):
        return self.__dict__

    def __repr__(self):
        return (
            self.__class__.__qualname__
            + f"({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"
        )

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        def _comp(v0, v1):
            try:
                return bool(v0 == v1)
            except ValueError:
                return np.all(np.all(v0 == v1))

        r = []
        for v0, v1 in zip(self.__dict__.values(), other.__dict__.values()):
            if isinstance(v0, dict):
                for v01, v11 in zip(v0.values(), v1.values()):
                    r.append(_comp(v01, v11))
            else:
                r.append(_comp(v0, v1))

        return all(r)


class MockPudlTabl:
    """A fake PudlTabl."""

    def __init__(
        self,
        freq: Literal["AS", "MS", None] = None,
        fill_fuel_cost: bool = False,
        roll_fuel_cost: bool = False,
        fill_net_gen: bool = False,
        fill_tech_desc: bool = True,
        unit_ids: bool = False,
    ):
        """Initialize the PUDL output object."""
        # Validating ds is deferred to the etl_eia861 & etl_ferc714 methods
        # because those are the only places a datastore is required.
        self.ds = TestKlass(local_cache_path=str(Path.home() / "pudl-work/data"))

        self.pudl_engine = sa.create_engine(
            "sqlite:////Users/aengel/pudl-work/sqlite/pudl.sqlite"
        )

        if freq not in (None, "AS", "MS"):
            raise ValueError(
                f"freq must be one of None, 'MS', or 'AS', but we got {freq}."
            )
        self.freq: Literal["AS", "MS", None] = freq

        self.start_date = pd.Timestamp("2005-01-01")
        self.end_date = pd.Timestamp("2022-01-01")

        self.roll_fuel_cost: bool = roll_fuel_cost
        self.fill_fuel_cost: bool = fill_fuel_cost
        self.fill_net_gen: bool = fill_net_gen
        self.fill_tech_desc = fill_tech_desc  # only for eia860 table.
        self.unit_ids = unit_ids

        # Used to persist the output tables. Returns None if they don't exist.
        self._dfs = defaultdict(lambda: None)

    def utils_eia860(self, update=False):
        """Add a df to ``self._dfs``."""
        if update or self._dfs["utils_eia860"] is None:
            self._dfs["utils_eia860"] = pd.DataFrame([(1.4, 3.2, 5.4), (2.4, 6.2, 9.4)])
        return self._dfs["utils_eia860"]

    def bga_eia860(self, update=False):
        """Add a df to ``self._dfs``."""
        if update or self._dfs["bga_eia860"] is None:
            self._dfs["bga_eia860"] = pd.DataFrame([(1.4, 3.2, 5.4), (2.4, 6.2, 9.4)])
        return self._dfs["bga_eia860"]
