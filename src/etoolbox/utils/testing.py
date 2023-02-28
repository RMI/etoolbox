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
    if type(left) is not type(right):
        raise AssertionError(f"{type(left)=} is not {type(right)=}")
    if isinstance(right, pd.Series):
        pd.testing.assert_series_equal(left, right)
    elif isinstance(right, pd.DataFrame):
        pd.testing.assert_frame_equal(left, right)
    elif isinstance(right, (list, tuple)):
        assert len(left) == len(right)
        for v0, v1 in zip(left, right):
            assert_equal(v0, v1)
    elif isinstance(right, dict):
        assert len(left) == len(right)
        for k0v0, k1v1 in zip(left.items(), right.items()):
            assert_equal(k0v0, k1v1)
    else:
        assert np.all(left == right)


def capture(func, *args, **kwargs):
    """Return a tuple with the returned exception and value.

    Allows you to test that two implementations of func return
    the same result or the same error.

    Examples
    --------
    Function with args returns result

    >>> capture(sum, (1, 2, 3))
    (None, 6)

    Function with args produces an error

    >>> capture(sum, (1, "3", 3))
    (<class 'TypeError'>, None)

    Different implementations produce the same error

    >>> import math
    >>> capture(sum, (1, "3", 3)) == capture(math.fsum, (1, "3", 3))
    True


    """
    try:
        return None, func(*args, **kwargs)
    except Exception as exc:
        return type(exc), None
