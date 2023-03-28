"""Tests for :class:`.IOMixin`."""
import pytest
from etoolbox.datazip import IOMixin


def test_mixin_on_pudl(temp_dir):
    """Test :class:`.IOMixin` on :class:`pudl.PudlTabl`."""
    pudl = pytest.importorskip("pudl")
    sqlalchemy = pytest.importorskip("sqlalchemy")

    file = temp_dir / "test_mixin_on_pudl"
    file.with_suffix(".zip").unlink(missing_ok=True)

    class PTM(pudl.output.pudltabl.PudlTabl, IOMixin):
        recipes = {
            ("sqlalchemy.engine.base", "Engine", None): {
                "keep": False,
            },
        }

        def __init__(self, *args, **kwargs):
            super().__init__(
                sqlalchemy.create_engine(
                    pudl.workspace.setup.get_defaults()["pudl_db"]
                ),
                **kwargs,
            )

    pt = PTM(
        freq="AS",
        start_date="2019-01-01",
        end_date="2020-12-31",
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )
    pt.plants_eia860()
    pt.gens_eia860()

    pt.to_file(temp_dir / "test_mixin_on_pudl")

    new = PTM.from_file(file)
    assert new._dfs.keys() == pt._dfs.keys()
    for df_name, df in new._dfs.items():
        assert df.shape == pt._dfs[df_name].shape
