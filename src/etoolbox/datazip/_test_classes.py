"""Mocks and other objects for testing :class:`.DataZip`."""
from __future__ import annotations

from typing import NamedTuple

import pandas as pd


class ObjMeta(NamedTuple):
    """NamedTuple for testing."""

    module: str
    qualname: str
    constructor: str | None = None


def _eq_func(self, other):
    if not isinstance(other, self.__class__):
        return False

    def _comp(v0, v1):
        if isinstance(v0, dict):
            for v01, v11 in zip(v0.values(), v1.values()):  # noqa: B905
                return _comp(v01, v11)
        if isinstance(v0, list | tuple):
            for v01, v11 in zip(v0, v1):  # noqa: B905
                return _comp(v01, v11)
        if isinstance(v0, pd.DataFrame | pd.Series):
            return v0.compare(v1).empty
        return bool(v0 == v1)

    r = []

    if hasattr(self, "__dict__"):
        for v0, v1 in zip(  # noqa: B905
            self.__dict__.values(), other.__dict__.values()
        ):
            r.append(_comp(v0, v1))

    if hasattr(self, "__slots__"):
        for k in self.__slots__:
            if hasattr(self, k) and hasattr(other, k):
                v0, v1 = getattr(self, k), getattr(other, k)
                r.append(_comp(v0, v1))
            if hasattr(self, k) and not hasattr(other, k):
                r.append(False)

    return all(r)


class _TestKlassSlotsCore:
    """Test class with slots w/o get/set."""

    __slots__ = ("foo", "_dfs", "tup", "lis")

    def __init__(self, **kwargs):
        """Init."""
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        attrs = ", ".join(
            f"{k}={getattr(self, k)}" for k in self.__slots__ if hasattr(self, k)
        )
        return self.__class__.__qualname__ + f"({attrs})"

    def __eq__(self, other):
        return _eq_func(self, other)


class _KlassSlots(_TestKlassSlotsCore):
    """Generic class with slots and get/set."""

    __slots__ = ("foo", "_dfs", "tup", "lis")

    def __init__(self, **kwargs):
        """Init."""
        super().__init__(**kwargs)

    def __setstate__(self, state):
        _, state = state
        for k, v in state.items():
            if k in self.__slots__:
                setattr(self, k, v)

    def __getstate__(self):
        return None, {k: getattr(self, k) for k in self.__slots__}


class _TestKlassSlotsDict:
    """Test class with slots and __dict__ w/o get/set."""

    __slots__ = (
        "foo",
        "__dict__",
    )

    def __init__(self, **kwargs):
        """Init."""
        for k, v in kwargs.items():
            setattr(self, k, v)

    def add_to_dict(self, k, v):
        setattr(self, k, v)
        return self

    def __eq__(self, other):
        return _eq_func(self, other)

    def __repr__(self):
        attrs = ", ".join(
            f"{k}={getattr(self, k)}" for k in self.__slots__ if hasattr(self, k)
        )
        return self.__class__.__qualname__ + f"({attrs})"


class _TestKlassCore:
    """Generic class w/o slots w/o get/set."""

    def __init__(self, **kwargs):
        """Init."""
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return (
            self.__class__.__qualname__
            + f"({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"
        )

    def __eq__(self, other):
        return _eq_func(self, other)


class _TestKlass(_TestKlassCore):
    """Generic class w/o slots with get/set."""

    def __init__(self, **kwargs):
        """Init."""
        super().__init__(**kwargs)

    def __setstate__(self, state):
        self.__dict__ = state

    def __getstate__(self):
        return self.__dict__
