"""Test for :mod:`etoolbox.utils.arrays`."""

import numpy as np
import pandas as pd
from etoolbox.utils.arrays import compare_dfs, isclose


def test_isclose_pd():
    """Test isclose with :class:`pandas.Series`."""
    a = pd.Series([1, 2.0, 3, "foo", "bar"])
    b = pd.Series([3, 2.00000001, 3, "foob", "bar"])
    assert np.all(isclose(a, b) == pd.Series([False, True, True, False, True]))


def test_compare_dfs():
    """Test `compare_dfs`."""
    df0 = pd.DataFrame(
        {
            "a": ["1", "2", "3", "4", "5"],
            "b": ["1", "2", "7", "4", "5"],
            "c": [1, 2, 7, 4, 5],
        }
    )
    df1 = pd.DataFrame(
        {
            "a": ["1", "2", "3", "4", "5", "7"],
            "b": ["1", "2", "78", "4", "5", "8"],
            "c": [1, 9, 7, 4, 5, 12],
        }
    )
    c = compare_dfs(df0, df1, align_col=["a", "b"])
    expected = pd.DataFrame(
        {
            ("a", "other"): [np.nan, np.nan, "3", "7"],
            ("a", "self"): [np.nan, "3", np.nan, np.nan],
            ("b", "other"): [np.nan, np.nan, "78", "8"],
            ("b", "self"): [np.nan, "7", np.nan, np.nan],
            ("c", "other"): [9.0, np.nan, 7.0, 12.0],
            ("c", "self"): [2.0, 7.0, np.nan, np.nan],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("both", "2", "2"),
                ("self_only", "3", "7"),
                ("other_only", "3", "78"),
                ("other_only", "7", "8"),
            ],
            names=["merge", "a", "b"],
        ),
    )
    pd.testing.assert_frame_equal(c, expected)
