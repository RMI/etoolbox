"""Test pretend PudlTabl."""
import os
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from etoolbox.utils.pudl import (
    conform_pudl_dtypes,
    get_pudl_sql_url,
    pd_read_pudl,
    pl_read_pudl,
    pl_scan_pudl,
)


class TestPudlLoc:
    @mock.patch.dict(os.environ, {"PUDL_OUTPUT": "/Users/pytest/output"})
    def test_get_pudl_sql_url_env(self):
        """Test pudl.sqlite url from env variable."""
        assert get_pudl_sql_url() == "sqlite:////Users/pytest/output/pudl.sqlite"

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_get_pudl_sql_url_config_good(self, pudl_config):
        """Test pudl.sqlite url from config."""
        assert (
            get_pudl_sql_url(pudl_config)
            == "sqlite:////Users/pytest/output/pudl.sqlite"
        )

    @pytest.mark.skip(reason="added hard to test fallback")
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_get_pudl_sql_url_config_bad(self, temp_dir):
        """Test pudl.sqlite url from config failure."""
        with pytest.raises(FileNotFoundError):
            get_pudl_sql_url(temp_dir / ".foo.yml")


def test_fix_types():
    """Test fix_types function."""
    df = pd.DataFrame(
        {
            "plant_id_eia": [1.0, 2.0, np.nan],
            "generator_id": [1, 2, 3],
            "foobar": ["a", "b", "c"],
        }
    )
    assert (
        df.pipe(conform_pudl_dtypes)
        .dtypes.astype(str)
        .compare(
            pd.Series(
                {"plant_id_eia": "Int64", "generator_id": "string", "foobar": "object"}
            )
        )
        .empty
    )


@pytest.mark.usefixtures("pudl_access_key_setup")
class TestGCSPudl:
    def test_pl_read_pudl_table(self):
        """Test reading table from GCS as :func:`polars.DataFrame."""
        df = pl_read_pudl("core_eia__codes_balancing_authorities")
        assert not df.is_empty()

    def test_pl_scan_pudl_table(self):
        """Test reading table from GCS as :func:`polars.LazyFrame."""
        df = pl_scan_pudl("core_eia__codes_balancing_authorities")
        assert not df.collect().is_empty()

    def test_pd_read_pudl_table(self):
        """Test reading table from GCS as :func:`pandas.DataFrame."""
        df = pd_read_pudl("core_eia__codes_balancing_authorities")
        assert not df.empty
