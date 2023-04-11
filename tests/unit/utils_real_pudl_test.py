"""Test pudl utilities with PUDL installed."""
from importlib.util import find_spec

import pytest
from etoolbox.utils.pudl import make_pudl_tabl


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
