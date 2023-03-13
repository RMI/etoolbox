"""PyTest configuration module.

Defines useful fixtures, command line args.
"""
import logging
import shutil
from pathlib import Path

import pandas as pd
import pytest

from etoolbox.datazip._test_classes import _KlassSlots, _TestKlass

logger = logging.getLogger(__name__)


@pytest.fixture
def df_dict() -> dict:
    """Dictionary of dfs."""
    return {
        "a": pd.DataFrame(
            [[0, 1], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        ),
        "b": pd.Series([1, 2, 3, 4]),
    }


@pytest.fixture
def klass_w_slot(df_dict):
    """Generic class that uses slots."""
    obj = _KlassSlots()
    obj.foo = df_dict["a"]
    obj.tup = (1, 2)
    obj.lis = (3, 4)
    obj._dfs = df_dict
    return obj


@pytest.fixture
def klass_wo_slot(df_dict):
    """Generic class that does not use slots."""
    obj = _TestKlass()
    obj.foo = df_dict["a"]
    obj._dfs = df_dict
    return obj


@pytest.fixture(scope="session")
def test_dir() -> Path:
    """Return the path to the top-level directory containing the tests.

    This might be useful if there's test data stored under the tests
    directory that
    you need to be able to access from elsewhere within the tests.

    Mostly this is meant as an example of a fixture.
    """
    return Path(__file__).parent


@pytest.fixture(scope="session")
def temp_dir(test_dir) -> Path:
    """Return the path to a temp directory that gets deleted on teardown."""
    out = test_dir / "temp"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(exist_ok=True)
    yield out
    shutil.rmtree(out)


@pytest.fixture(
    scope="class",
    params=[
        "pandas",
        pytest.param(
            "pyarrow",
            marks=pytest.mark.skipif(
                pd.__version__ < "2.0.0",
                reason="`pandas.options.mode.dtype_backend` option not available before 2.0.0",
            ),
        ),
    ],
)
def pd_backend(test_dir, request) -> str:
    """Use to run test with both pandas backends."""
    if pd.__version__ > "2.0.0":
        pd.set_option("mode.dtype_backend", request.param)
    return request.param
