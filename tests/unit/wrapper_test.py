"""Tests for :class:`.IOWrapper`."""
import pytest

from etoolbox.datazip import IOWrapper
from etoolbox.datazip.test_classes import MockPudlTabl


def test_wrapper(temp_dir):
    """Test wrapping a mocked PudlTabl."""
    r = {
        ("etoolbox.datazip.test_classes", "KlassWOSlots", None): {
            "keep": False,
            "constructor": (
                "etoolbox.datazip.test_classes",
                "KlassWOSlots",
                None,
            ),
        }
    }
    a = IOWrapper(MockPudlTabl(), recipes=r)
    df = a.utils_eia860()
    a.to_file(temp_dir / "test_wrapper")
    b = IOWrapper.from_file(temp_dir / "test_wrapper")
    assert df.compare(b.utils_eia860()).empty
    assert not b.bga_eia860().empty


def test_wrapper_on_pudl(temp_dir):
    """Test :class:`.IOWrapper` on :class:`pudl.PudlTabl`."""
    pudl = pytest.importorskip("pudl")
    sqlalchemy = pytest.importorskip("sqlalchemy")

    file = temp_dir / "test_wrapper_on_pudl"
    file.with_suffix(".zip").unlink(missing_ok=True)

    pt = IOWrapper(
        pudl.output.pudltabl.PudlTabl(
            sqlalchemy.create_engine(pudl.workspace.setup.get_defaults()["pudl_db"]),
            freq="AS",
            # start_date="2019-01-01",
            # end_date="2020-12-31",
            fill_fuel_cost=True,
            roll_fuel_cost=True,
            fill_net_gen=True,
        )
    )
    pt.plants_eia860()
    pt.gens_eia860()
    pt.to_file(temp_dir / "test_wrapper_on_pudl")

    new = IOWrapper.from_file(file)
    assert new._dfs.keys() == pt._dfs.keys()
    for df_name, df in new._dfs.items():
        assert df.shape == pt._dfs[df_name].shape
