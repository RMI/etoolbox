"""Test pretend PudlTabl."""
import os
import sys
from importlib.util import find_spec
from unittest import mock

import pandas as pd
import pytest
from etoolbox.datazip.core import DataZip
from etoolbox.utils.pudl import (
    PretendPudlTabl,
    _Faker,
    get_pudl_sql_url,
    make_pudl_tabl,
    read_pudl_table,
)
from etoolbox.utils.testing import idfn

from tests.conftest import get_pudl_loc


def test_faker():
    """Test _Faker."""
    fake = _Faker(5)
    assert fake() == 5


class TestPretendPudlTabl:
    """Tests for PretendPudlTabl."""

    @pytest.mark.skip(reason="we have better tests for this now")
    def test_load_actual(self, test_dir):
        """Test with a fresh PudlTabl."""
        pudl = pytest.importorskip("pudl")

        pt = DataZip.load(test_dir / "pudltabl.zip", PretendPudlTabl)
        df = pt.plants_eia860()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

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

    @pytest.mark.skipif(
        find_spec("pudl") is not None,
        reason="This test is for when PUDL is not installed",
    )
    def test_load_error(self, test_dir, temp_dir):
        """Test with a sample PudlTabl."""
        pt = DataZip.load(test_dir / "pudltabl.zip", PretendPudlTabl)
        with pytest.raises(ModuleNotFoundError):
            import pudl  # noqa: F401
        with pytest.raises(ModuleNotFoundError):
            df = pt.plants_eia860()


@pytest.mark.parametrize(
    "table, expected",
    [
        ("epacamd_eia", "in_pt"),
        pytest.param(
            "plants_eia860",
            ModuleNotFoundError,
            marks=pytest.mark.skipif(
                find_spec("pudl") is not None,
                reason="This test is for when PUDL is not installed",
            ),
        ),
        ("__slots__", None),
        ("foobar", None),
        ("unit_ids", _Faker(thing=False)),
    ],
    ids=idfn,
)
def test_make_pudl_tabl(test_dir, table, expected):
    """PudlTabl maker from test file."""
    pt = make_pudl_tabl(test_dir / "pudltabl.zip")
    if expected == "in_pt":
        assert not getattr(pt, table)().empty
    elif expected is None:
        assert getattr(pt, table)() is expected
    elif isinstance(expected(), Exception):
        with pytest.raises(expected):
            getattr(pt, table)
    else:
        assert getattr(pt, table) == expected()


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

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_get_pudl_sql_url_config_bad(self, temp_dir):
        """Test pudl.sqlite url from config failure."""
        with pytest.raises(FileNotFoundError):
            get_pudl_sql_url(temp_dir / ".foo.yml")


@pytest.mark.skipif(
    find_spec("pudl") is None,
    reason="This test is for when PUDL is not installed",
)
class TestRealPudl:
    """Test for PUDL related functionality."""

    def test_pu_ferc1_in_fresh(self, pudltabl):
        """Test that PudlTabl has expected table."""
        assert pudltabl._dfs["pu_ferc1"] is not None

    def test_pu_ferc1(self, pudltabl):
        """Test that PudlTabl has expected table."""
        df = pudltabl._dfs["pu_ferc1"]
        assert not df.empty

    def test_utils_eia860_not_in_pudltabl(self, pudltabl):
        """Test that PudlTabl does not have but can create table."""
        default = pudltabl._dfs["utils_eia860"]
        assert default is None

    def test_utils_eia860(self, pudltabl):
        """Test that PudlTabl does not have but can create table."""
        df = pudltabl.utils_eia860()
        assert not df.empty

    def test_pu_ferc1_in_zip(self, pudl_zip_path):
        """Test that PudlTabl from zip has expected table."""
        pudl_tabl = make_pudl_tabl(pudl_zip_path)
        df = pudl_tabl._dfs["pu_ferc1"]
        assert df is not None

    def test_pu_ferc1_df_in_zip(self, pudl_zip_path):
        """Test that PudlTabl from zip has expected table."""
        pudl_tabl = make_pudl_tabl(pudl_zip_path)
        df = pudl_tabl._dfs["pu_ferc1"]
        assert not df.empty

    def test_utils_eia860_from_zip(self, pudl_zip_path):
        """Test that PudlTabl from zip does not have table."""
        pudl_tabl = make_pudl_tabl(pudl_zip_path)
        default = pudl_tabl._dfs.get("utils_eia860", None)
        assert default is None

    def test_sales_eia861_not_in_zip(self, pudl_zip_path):
        """Test that PudlTabl from zip does not have table."""
        pudl_tabl = make_pudl_tabl(pudl_zip_path)
        default = pudl_tabl._dfs.get("sales_eia861", None)
        assert default is None

    def test_sales_eia861_from_zip(self, pudl_zip_path):
        """Test that PudlTabl from zip does not have table."""
        pudl_tabl = make_pudl_tabl(pudl_zip_path)
        df = pudl_tabl.sales_eia861()
        assert df is not None
        assert not df.empty


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="autodownload sometimes fails on windows",
)
@pytest.mark.parametrize(
    "table, expected",
    [
        ("plants_eia860", None),
        ("foobar", KeyError),
    ],
    ids=idfn,
)
def test_read_pudl_table(temp_dir, table, expected):
    """Test function to get tables from ``pudl.sqlite``."""
    os.environ["PUDL_OUTPUT"] = get_pudl_loc(temp_dir)
    if expected is None:
        df = read_pudl_table(table_name=table)
        assert not df.empty
    else:
        with pytest.raises(expected):
            _ = read_pudl_table(table_name=table)
