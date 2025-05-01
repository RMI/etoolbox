from datetime import datetime

import pandas as pd
import pytest

from etoolbox.utils.cloud import (
    _cache_info,
    get,
    put,
    read_patio_resource_results,
    write_patio_econ_results,
)
from etoolbox.utils.testing import idfn


@pytest.mark.parametrize(
    "destination", ["test", pytest.param("test5", marks=pytest.mark.xfail)], ids=idfn
)
def test_copy_to_cloud(temp_dir, destination):
    """Test uploading to RMI's Azure cloud storage."""
    test_path = (temp_dir / datetime.now().strftime("%Y%m%d%H%M")).with_suffix(".txt")
    test_path.touch()
    put(test_path, destination, azcopy_path="foo")


@pytest.mark.parametrize(
    "source",
    ["test_data.parquet", pytest.param("test_dir", marks=pytest.mark.xfail)],
    ids=idfn,
)
def test_get_from_cloud(temp_dir, source):
    """Test downloading from RMI's Azure cloud storage."""
    get("raw-data/" + source, temp_dir)
    assert (temp_dir / source).exists()


def test_read_patio_results(temp_dir):
    """Test downloading from RMI's Azure cloud storage."""
    out_dict = read_patio_resource_results("202504262322")
    assert "full" in out_dict


def test_write_patio_econ_results(temp_dir):
    """Test writing to RMI's Azure cloud storage."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    write_patio_econ_results(df, "202504262322", "test_results.parquet")


def test_cache_info__():
    """Test cache info."""
    _cache_info("")
