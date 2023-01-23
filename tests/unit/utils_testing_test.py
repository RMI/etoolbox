"""Tests for testing utils."""
import pandas as pd
import pytest

from etoolbox.utils.testing import assert_equal, idfn


@pytest.mark.parametrize(
    "right, left, exception",
    [
        (pd.Series([1, 2, 3]), pd.Series([1, 2, 3]), None),
        (pd.Series([1, 2, 3]), pd.Series([1, 45, 3]), AssertionError),
        ([1, pd.Series([1, 2, 3]), 3], [1, pd.Series([1, 2, 3]), 3], None),
        ([1, pd.Series([1, 2, 3]), 3], [1, pd.Series([1, 45, 3]), 3], AssertionError),
        ((1, 2, 3), (1, 2, 3), None),
        ((1, 2, 3), (1, 22, 3), AssertionError),
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
