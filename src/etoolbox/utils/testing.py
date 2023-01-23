"""Helpers for tests."""
import numpy as np
import pandas as pd


def idfn(val):
    """ID function for pytest parameterization."""
    if isinstance(val, float):
        return None
    return str(val)


def assert_equal(left, right) -> None:
    """Recursively check that left and right objects are equal."""
    if isinstance(right, pd.Series):
        pd.testing.assert_series_equal(left, right)
    elif isinstance(right, pd.DataFrame):
        pd.testing.assert_frame_equal(left, right)
    elif isinstance(right, (list, tuple)):
        for v0, v1 in zip(left, right):
            assert_equal(v0, v1)
    elif isinstance(right, dict):
        for v0, v1 in zip(left.values(), right.values()):
            assert_equal(v0, v1)
    else:
        assert np.all(left == right)
