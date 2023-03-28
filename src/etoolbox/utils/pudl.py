"""A pretend PudlTabl."""


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

    def __setstate__(self, state):
        self.__dict__ = state
        self._real_pt = []

    def __getattr__(self, item):
        if item in self.__dict__["_dfs"]:
            return _Faker(self.__dict__["_dfs"][item])
        if not isinstance(self._real_pt, list):
            return getattr(self._real_pt, item)
        else:
            return self._make_it_real(item)

    def _make_it_real(self, item):
        from collections import defaultdict
        from importlib import import_module

        try:
            sa = import_module("sqlalchemy")
            pudl = import_module("pudl")
            pt = pudl.output.pudltabl.PudlTabl(
                sa.create_engine(pudl.workspace.setup.get_defaults()["pudl_db"])
            )
            pt._dfs = defaultdict(lambda: None, self.__dict__["_dfs"])
        except Exception as exc:
            raise RuntimeError(
                f"I am only a pretend PudlTabl. I tried to load a real one to get "
                f"'{item}' but wasn't able to."
            ) from exc
        else:
            self._real_pt = pt
            return getattr(self._real_pt, item)
