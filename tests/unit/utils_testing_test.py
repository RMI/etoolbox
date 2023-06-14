"""Tests for testing utils."""
import math

import numpy as np
import pandas as pd
import polars as pl
import pytest
from etoolbox.utils.testing import assert_equal, capture, idfn


@pytest.mark.parametrize(
    "right, left, exception",
    [
        (pd.Series([1, 2, 3]), pd.Series([1, 2, 3]), None),
        (pd.Series([1, 2, 3]), pd.Series([1, 45, 3]), AssertionError),
        (pl.Series([1, 2, 3]), pl.Series([1, 2, 3]), None),
        (pl.Series([1, 2, 3]), pl.Series([1, 45, 3]), AssertionError),
        ([1, pd.Series([1, 2, 3]), 3], [1, pd.Series([1, 2, 3]), 3], None),
        ([1, pd.Series([1, 2, 3]), 3], [1, pd.Series([1, 45, 3]), 3], AssertionError),
        ((1, 2, 3), (1, 2, 3), None),
        ((1, 2, 3), (1, 22, 3), AssertionError),
        ((1, 2, 3, 5), (1, 22, 3), AssertionError),
        ((1, 2, 3, 5), (1, 2, 3), ValueError),
        ({1: 5}, {1: 5, 2: 4}, ValueError),
        (
            {
                0: 2,
                55: pd.DataFrame(
                    {(0, "a"): [0.1, 2.1, 2.0], (0, "b"): [0.1, 3.1, 2.0]}
                ),
            },
            {
                0: 2,
                55: pd.DataFrame(
                    {(0, "a"): [0.1, 2.1, 2.0], (0, "b"): [0.1, 3.1, 2.0]}
                ),
            },
            None,
        ),
        (
            {
                0: 2,
                55: pd.DataFrame(
                    {(0, "a"): [0.1, 66.2, 2.0], (0, "b"): [0.1, 3.1, 2.0]}
                ),
            },
            {
                0: 2,
                55: pd.DataFrame(
                    {(0, "a"): [0.1, 2.1, 2.0], (0, "b"): [0.1, 3.1, 2.0]}
                ),
            },
            AssertionError,
        ),
        (
            pl.DataFrame({"a": [0.1, 2.1, 2.0], "b": [0.1, 3.1, 2.0]}),
            pl.DataFrame({"a": [0.1, 2.1, 2.0], "b": [0.1, 3.1, 2.0]}),
            None,
        ),
        (
            pl.DataFrame({"a": [0.1, 66.2, 2.0], "b": [0.1, 3.1, 2.0]}),
            pl.DataFrame({"a": [0.1, 2.1, 2.0], "b": [0.1, 3.1, 2.0]}),
            AssertionError,
        ),
        (
            pl.LazyFrame({"a": [0.1, 2.1, 2.0], "b": [0.1, 3.1, 2.0]}),
            pl.LazyFrame({"a": [0.1, 2.1, 2.0], "b": [0.1, 3.1, 2.0]}),
            None,
        ),
        (
            pl.LazyFrame({"a": [0.1, 66.2, 2.0], "b": [0.1, 3.1, 2.0]}),
            pl.LazyFrame({"a": [0.1, 2.1, 2.0], "b": [0.1, 3.1, 2.0]}),
            AssertionError,
        ),
    ],
    ids=idfn,
)
def test_assert_equal(left, right, exception):
    """Test assert_equal."""
    if exception is None:
        assert_equal(left, right)
    else:
        with pytest.raises(exception):
            assert_equal(left, right)


@pytest.mark.parametrize(
    "funca, funcb, args",
    [
        (sum, math.fsum, (1, 2, 3)),
        (sum, math.fsum, ("a", 2, 3)),
        (sum, math.fsum, (1, "2", None)),
        pytest.param(
            sum, math.fsum, np.arange(0.0, 15.0, 0.01), marks=pytest.mark.xfail
        ),
    ],
    ids=idfn,
)
def test_capture(funca, funcb, args):
    """Capture test."""
    assert capture(funca, args) == capture(funcb, args)
