"""Test pretend PudlTabl."""
import os
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
from etoolbox.datazip.core import DataZip
from etoolbox.utils.pudl import (
    PretendPudlTabl,
    _Faker,
    get_pudl_sql_url,
    make_pudl_tabl,
)
from etoolbox.utils.testing import idfn


def test_faker():
    """Test _Faker."""
    fake = _Faker(5)
    assert fake() == 5


class TestPretendPudlTabl:
    """Tests for PretendPudlTabl."""

    def test_load_actual(self, test_dir):
        """Test with a fresh PudlTabl."""
        sa = pytest.importorskip("sqlalchemy")
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
        ("plants_eia860", ModuleNotFoundError),
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


@mock.patch.dict(os.environ, {"PUDL_OUTPUT": "/Users/pytest/output"})
def test_get_pudl_sql_url_env():
    """Test pudl.sqlite url from env variable."""
    assert get_pudl_sql_url() == "sqlite:////Users/pytest/output/pudl.sqlite"


def test_get_pudl_sql_url_config_good(pudl_config):
    """Test pudl.sqlite url from config."""
    assert get_pudl_sql_url(pudl_config) == "sqlite:////Users/pytest/output/pudl.sqlite"


def test_get_pudl_sql_url_config_bad():
    """Test pudl.sqlite url from config failure."""
    with pytest.raises(FileNotFoundError):
        assert (
            get_pudl_sql_url(Path.home() / ".foo.yml")
            == "sqlite:////Users/pytest/output/pudl.sqlite"
        )
