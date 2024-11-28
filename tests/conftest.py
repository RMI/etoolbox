"""PyTest configuration module.

Defines useful fixtures, command line args.
"""

import gzip
import logging
import shutil
from pathlib import Path

import pandas as pd
import pytest

from etoolbox.datazip._test_classes import _KlassSlots, _TestKlass
from etoolbox.utils.logging_utils import setup_logging

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
    import yaml

    file = temp_dir / ".pudl5.yml"
    with open(file, "w") as f:
        yaml.safe_dump({"pudl_out": "/Users/pytest"}, f)
    yield file
    file.unlink()
    assert not file.exists()


@pytest.fixture(scope="session")
def pudl_test_cache(temp_dir):
    """Change PUDL cache path for testing."""
    import etoolbox.utils.pudl as pudl

    pudl.CACHE_PATH = temp_dir / "pudl_cache"


@pytest.fixture
def pudl_test_cache_for_ep(temp_dir):
    """Setup dummy pudl cache and config directories for testing."""
    import etoolbox.utils.pudl as pudl

    pudl.CACHE_PATH = temp_dir / "rmi.pudl.cache/aws"
    pudl.TOKEN_PATH = temp_dir / "rmi.pudl/.pudl-access-key.json"

    pudl.CACHE_PATH.mkdir(exist_ok=True, parents=True)
    pudl.TOKEN_PATH.parent.mkdir(exist_ok=True)
    pudl.TOKEN_PATH.touch(exist_ok=True)
    (pudl.CACHE_PATH / "cache").touch()
    (pudl.CACHE_PATH.parent / "cache").touch()


@pytest.fixture
def cloud_test_cache(temp_dir):
    """Setup dummy cloud cache and config directories for testing."""
    import etoolbox.utils.cloud as cloud

    original_paths = (cloud.CONFIG_PATH, cloud.RMICFEZIL_TOKEN_PATH)

    cloud.AZURE_CACHE_PATH = temp_dir / "rmi.cloud.cache"
    cloud.CONFIG_PATH = temp_dir / "rmi.cloud"
    cloud.RMICFEZIL_TOKEN_PATH = cloud.CONFIG_PATH / "rmicfezil_token.txt"

    cloud.AZURE_CACHE_PATH.mkdir(exist_ok=True, parents=True)
    cloud.CONFIG_PATH.mkdir(exist_ok=True, parents=True)
    yield

    shutil.rmtree(temp_dir / "rmi.cloud.cache", ignore_errors=True)
    # shutil.rmtree(temp_dir / "rmi.cloud", ignore_errors=True)
    cloud.CONFIG_PATH, cloud.RMICFEZIL_TOKEN_PATH = original_paths


@pytest.fixture
def cloud_test_cache_w_files(cloud_test_cache):
    """Setup dummy cloud cache and config files for testing."""
    import etoolbox.utils.cloud as cloud

    with open(cloud.RMICFEZIL_TOKEN_PATH, "w") as f:
        f.write("123")
    (cloud.AZURE_CACHE_PATH / "cache").touch()


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
