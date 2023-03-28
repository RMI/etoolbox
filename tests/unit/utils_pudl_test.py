"""Test pretend PudlTabl."""
import pandas as pd
import pytest
from etoolbox.datazip.core import DataZip
from etoolbox.utils.pudl import PretendPudlTabl, _Faker


def test_faker():
    """Test _Faker."""
    fake = _Faker(5)
    assert fake() == 5


class TestPretendPudlTabl:
    """Tests for PretendPudlTabl."""

    @pytest.mark.skip
    def test_load_actual(self, test_dir):
        """Test with a fresh PudlTabl."""
        pudl = pytest.importorskip("pudl")
        sa = pytest.importorskip("sqlalchemy")

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
        with pytest.raises(RuntimeError):
            df = pt.plants_eia860()
