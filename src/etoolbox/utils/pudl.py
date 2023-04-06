"""Functions and objects for creating and ~mocking PudlTabl."""
import logging
import os
from pathlib import Path

from etoolbox.utils.lazy_import import lazy_import

PUDL_CONFIG = Path.home() / ".pudl.yml"
logger = logging.getLogger(__name__)


def get_pudl_sql_url(file=PUDL_CONFIG) -> str:
    """Get the URL for the pudl.sqlite DB."""
    try:
        return f"sqlite:///{os.environ['PUDL_OUTPUT']}/pudl.sqlite"
    except KeyError:
        if not file.exists():
            raise FileNotFoundError(
                ".pudl.yml is missing and 'PUDL_OUTPUT' environment variable is "
                "missing. For more info see: "
                "https://catalystcoop-pudl.readthedocs.io/en/dev/dev/dev_setup.html"
            ) from None

        import yaml

        with open(file, "r") as f:  # noqa: UP015
            return f"sqlite:///{yaml.safe_load(f)['pudl_out']}/output/pudl.sqlite"


def _make_pudl_tabl():
    pudltabl = lazy_import("pudl.output.pudltabl", wait_for_signal=False)
    sa = lazy_import("sqlalchemy", wait_for_signal=False)

    pudl_out = pudltabl.PudlTabl(
        sa.create_engine(get_pudl_sql_url()),
    )
    return pudl_out


def make_pudl_tabl(
    pudl_path: Path | str,
    tables: tuple
    | list = (
        "gf_eia923",
        "gen_original_eia923",
        "bf_eia923",
        "gens_eia860",
        "plants_eia860",
        "epacamd_eia",
    ),
    *,
    clobber=False,
):
    """Load a PudlTabl from cache or create a new one.

    Args:
        pudl_path: path to the existing DataZip containing a PudlTabl, or, if one does
            not exist yet, the path to where the DataZip of the PudlTabl will be stored.
        tables: tables that will be preloaded when creating a new PudlTabl.
        clobber: create a new PudlTabl cache even if the DataZip exists.

    Returns: A PudlTabl or a PretendPudlTabl

    """
    from etoolbox.datazip.core import DataZip

    if isinstance(pudl_path, Path):
        pudl_path = Path(pudl_path)

    if not pudl_path.with_suffix(".zip").exists() or clobber:
        if not pudl_path.parent.exists():
            pudl_path.parent.mkdir(parents=True)

        pudl_path.with_suffix(".zip").unlink(missing_ok=True)
        logger.info("Rebuilding PudlTabl")

        pudl_out = _make_pudl_tabl()
        for table in tables:
            try:
                getattr(pudl_out, table)()
            except AttributeError as exc:
                logger.error("Unable to load %s. %r", table, exc)
        DataZip.dump(pudl_out, pudl_path)
        return pudl_out
    try:
        pudl_out = DataZip.load(pudl_path, PretendPudlTabl)
    except Exception as exc:
        logger.error("Loading PudlTabl from file failed %r", exc)
        return make_pudl_tabl(pudl_path=pudl_path, clobber=True)
    else:
        logger.info("Loading PudlTabl from file")
        return pudl_out


class _Faker:
    """Return a thing when called.

    >>> fake = _Faker(5)
    >>> fake()
    5

    """

    def __init__(self, thing):
        self.thing = thing

    def __call__(self, *args, **kwargs):
        return self.thing


class PretendPudlTabl:
    """A DataZip of a PudlTabl can be recreated with this to avoid importing PUDL.

    Examples
    --------
    .. code-block:: python

        from pathlib import Path
        from etoolbox.datazip.core import DataZip
        from etoolbox.utils.pudl import PretendPudlTabl

        pudl_tabl = DataZip.load(Path("path_to_zip"), PretendPudlTabl)
        df = pudl_tabl.epacamd_eia()

    """

    _name_map = {"gen_original_eia923": "gen_og_eia923"}

    def __setstate__(self, state):
        self.__dict__ = state
        self._real_pt = None

    def __getattr__(self, item):
        if (n_item := self._name_map.get(item, item)) in self.__dict__["_dfs"]:
            return _Faker(self.__dict__["_dfs"][n_item])
        if self._real_pt is not None:
            return getattr(self._real_pt, item)
        if not any(("ferc" in item, "eia" in item, "epa" in item)):
            return _Faker(None)
        else:
            self._make_it_real()
            return getattr(self._real_pt, item)

    def _make_it_real(self):
        from collections import defaultdict

        logger.warning("We're making a real PudlTabl")

        try:
            pt = _make_pudl_tabl()
            pt._dfs = defaultdict(lambda: None, self.__dict__["_dfs"])
        except Exception as exc:
            raise ModuleNotFoundError(
                "I am only a pretend PudlTabl. I tried to load a real one "
                "but wasn't able to because PUDL is not installed."
            ) from exc
        else:
            self._real_pt = pt
