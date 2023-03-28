"""Test for :mod:`etoolbox.utils.arrays`."""
import numpy as np
import pandas as pd
from etoolbox.utils.arrays import isclose


def test_isclose_pd():
    """Test isclose with :class:`pandas.Series`."""
    a = pd.Series([1, 2.0, 3, "foo", "bar"])
    b = pd.Series([3, 2.00000001, 3, "foob", "bar"])
    assert np.all(isclose(a, b) == pd.Series([False, True, True, False, True]))
