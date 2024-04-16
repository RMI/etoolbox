"""Test pretend PudlTabl."""

import os
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from etoolbox.datazip import DataZip
from etoolbox.utils.pudl import (
    PUDL_DTYPES,
    PretendPudlTabl,
    conform_pudl_dtypes,
    get_pudl_sql_url,
    pd_read_pudl,
    pl_read_pudl,
    pl_scan_pudl,
)
from etoolbox.utils.testing import idfn


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


def test_pudl_dtypes_getitem():
    """Test that PUDL_DTYPES item access fails."""
    with pytest.raises(DeprecationWarning):
        _ = PUDL_DTYPES["foo"]


def test_pudl_dtypes_get():
    """Test that PUDL_DTYPES item access fails."""
    with pytest.raises(DeprecationWarning):
        _ = PUDL_DTYPES.get("foo")


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


class TestPretendPudlTabl:
    """Tests for PretendPudlTabl."""

    def test_type(self, test_dir, temp_dir):
        """Test with a sample PudlTabl."""
        pt = DataZip.load(test_dir / "pudltabl.zip", PretendPudlTabl)
        assert type(pt) is PretendPudlTabl

    def test_load(self, test_dir, temp_dir):
        """Test with a sample PudlTabl."""
        pt = DataZip.load(test_dir / "pudltabl.zip", PretendPudlTabl)
        df = pt.epacamd_eia()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_load_error(self, test_dir, temp_dir):
        """Test with a sample PudlTabl."""
        pt = DataZip.load(test_dir / "pudltabl.zip", PretendPudlTabl)
        with pytest.raises(KeyError):
            _ = pt.foo()


@pytest.mark.usefixtures("pudl_access_key_setup")
@pytest.mark.usefixtures("pudl_test_cache")
class TestGCSPudl:
    @pytest.mark.parametrize("use_polars", [False, True], ids=idfn)
    def test_pl_read_pudl_table(self, use_polars):
        """Test reading table from GCS as :class:`polars.DataFrame`."""
        df = pl_read_pudl(
            "core_eia__codes_balancing_authorities", use_polars=use_polars
        )
        assert not df.is_empty()

    @pytest.mark.parametrize("use_polars", [False, True], ids=idfn)
    def test_pl_scan_pudl_table(self, use_polars):
        """Test reading table from GCS as :class:`polars.LazyFrame`."""
        df = pl_scan_pudl(
            "core_eia__codes_balancing_authorities", use_polars=use_polars
        )
        assert not df.collect().is_empty()

    def test_pd_read_pudl_table(self):
        """Test reading table from GCS as :class:`pandas.DataFrame`."""
        df = pd_read_pudl("core_eia__codes_balancing_authorities")
        assert not df.empty


@pytest.mark.disable_socket()
@pytest.mark.usefixtures("pudl_access_key_setup")
@pytest.mark.usefixtures("pudl_test_cache")
class TestGCSPudlNoInternet:
    @pytest.mark.parametrize("use_polars", [False, True], ids=idfn)
    def test_pl_read_pudl_table(self, use_polars):
        """Test reading table from GCS as :class:`polars.DataFrame`."""
        if use_polars:
            with pytest.raises(FileNotFoundError):
                _ = pl_read_pudl(
                    "core_eia__codes_balancing_authorities", use_polars=use_polars
                )
        else:
            df = pl_read_pudl(
                "core_eia__codes_balancing_authorities", use_polars=use_polars
            )
            assert not df.is_empty()

    @pytest.mark.parametrize("use_polars", [False, True], ids=idfn)
    def test_pl_scan_pudl_table(self, use_polars):
        """Test reading table from GCS as :class:`polars.LazyFrame`."""
        if use_polars:
            with pytest.raises(FileNotFoundError):
                _ = pl_scan_pudl(
                    "core_eia__codes_balancing_authorities", use_polars=use_polars
                )
        else:
            df = pl_scan_pudl(
                "core_eia__codes_balancing_authorities", use_polars=use_polars
            )
            assert not df.collect().is_empty()

    def test_pd_read_pudl_table(self):
        """Test reading table from GCS as :class:`pandas.DataFrame`."""
        df = pd_read_pudl("core_eia__codes_balancing_authorities")
        assert not df.empty


@pytest.mark.script_launch_mode("inprocess")
def test_rmi_pudl_init_entry_point(script_runner):
    """Test the rmi-pudl-init entry point."""
    script_runner.run(
        ["rmi-pudl-init", os.environ.get("PUDL_ACCESS_KEY")], print_result=True
    )
    df = pl_scan_pudl("core_eia__codes_balancing_authorities")
    assert not df.collect().is_empty()
