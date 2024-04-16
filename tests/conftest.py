"""PyTest configuration module.

Defines useful fixtures, command line args.
"""

import gzip
import logging
import os
import shutil
from pathlib import Path

import pandas as pd
import pytest
import yaml
from etoolbox.datazip._test_classes import _KlassSlots, _TestKlass
from etoolbox.utils.logging import setup_logging
from etoolbox.utils.pudl import TOKEN_PATH, rmi_pudl_init

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


@pytest.fixture(scope="class")
def pudl_access_key_setup():
    """Set up PUDL access key for testing."""
    written = rmi_pudl_init(os.environ.get("PUDL_ACCESS_KEY"))
    yield None
    if written:
        TOKEN_PATH.unlink()


@pytest.fixture(scope="session")
def pudl_test_cache(temp_dir):  # noqa: PT004
    """Change PUDL cache path for testing."""
    import etoolbox.utils.pudl as pudl

    pudl.CACHE_PATH = temp_dir / "pudl_cache"
    pudl.CACHE_PATH.mkdir(exist_ok=True)


@pytest.fixture(scope="session")
def gzip_test_data(temp_dir):
    """Download pudl sqlite for testing if we don't have a local one."""
    gzip_path = temp_dir / "test_file.txt.gz"
    content = b"What hath G-d wrought"
    with gzip.open(gzip_path, "wb") as f:
        f.write(content)
    return gzip_path, content


@pytest.fixture(scope="session")
def test_logger(temp_dir):
    """Set up logger for testing."""
    log_file = temp_dir / "logs/log.jsonl"
    setup_logging(filename=log_file)

    logger = logging.getLogger("etb_test")
    return logger, log_file
