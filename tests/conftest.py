"""PyTest configuration module.

Defines useful fixtures, command line args.
"""

import gzip
import logging
import shutil
from pathlib import Path

import pandas as pd
import pytest
import yaml
from etoolbox.datazip._test_classes import _KlassSlots, _TestKlass
from etoolbox.utils.pudl import setup_access_key_for_ci

logger = logging.getLogger(__name__)


@pytest.fixture()
def df_dict() -> dict:
    """Dictionary of dfs."""
    return {
        "a": pd.DataFrame(
            [[0, 1], [2, 3]],
            columns=pd.MultiIndex.from_tuples([(0, "a"), (1, "b")]),
        ),
        "b": pd.Series([1, 2, 3, 4]),
    }


@pytest.fixture()
def klass_w_slot(df_dict):
    """Generic class that uses slots."""
    obj = _KlassSlots()
    obj.foo = df_dict["a"]
    obj.tup = (1, 2)
    obj.lis = (3, 4)
    obj._dfs = df_dict
    return obj


@pytest.fixture()
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
                pd.__version__ < "4.0.0",
                reason="`pandas.options.mode.dtype_backend` "
                "option not available before 2.0.0",
            ),
        ),
    ],
)
def pd_backend(test_dir, request) -> str:
    """Use to run test with both pandas backends."""
    if pd.__version__ > "4.0.0":
        pd.set_option("mode.dtype_backend", request.param)
    return request.param


@pytest.fixture(scope="session")
def pudl_config(temp_dir) -> str:
    """Use to run test with both pandas backends."""
    file = temp_dir / ".pudl5.yml"
    with open(file, "w") as f:
        yaml.safe_dump({"pudl_out": "/Users/pytest"}, f)
    yield file
    file.unlink()
    assert not file.exists()


# @pytest.fixture(scope="session")
# def pudl_zip_path(temp_dir):
#     """Path to save pudl.zip."""
#     pudl = pytest.importorskip("pudl")
#
#     return temp_dir / "pudl.zip"


# def get_pudl_loc(temp_dir):
#     """Download pudl sqlite for testing if we don't have a local one."""
#     try:
#         pudl_sql_path = Path(get_pudl_sql_url().replace("sqlite:///", ""))
#         if not pudl_sql_path.exists():
#             raise FileNotFoundError
#     except FileNotFoundError:
#         pudl_temp_sqlite_path = temp_dir / "pudl.sqlite"
#         if not pudl_temp_sqlite_path.exists():
#             zip_path = pudl_temp_sqlite_path.with_suffix(".sqlite.gz")
#             download(
#                 "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl.sqlite.gz",
#                 zip_path,
#             )
#             ungzip(zip_path, pudl_temp_sqlite_path)
#         return str(pudl_temp_sqlite_path.parent)
#     else:
#         return str(pudl_sql_path.parent)


@pytest.fixture(scope="session")
def pudl_access_key_setup():  # noqa: PT004
    """Set up PUDL access key for testing."""
    setup_access_key_for_ci()


@pytest.fixture(scope="session")
def gzip_test_data(temp_dir):
    """Download pudl sqlite for testing if we don't have a local one."""
    gzip_path = temp_dir / "test_file.txt.gz"
    content = b"What hath G-d wrought"
    with gzip.open(gzip_path, "wb") as f:
        f.write(content)
    return gzip_path, content
