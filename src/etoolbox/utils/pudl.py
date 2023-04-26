"""Functions and objects for creating and ~mocking PudlTabl."""
import logging
import os
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import sqlalchemy as sa

from etoolbox.utils.lazy_import import lazy_import

PUDL_CONFIG = Path.home() / ".pudl.yml"
TABLE_NAME_MAP = {
    "gen_original_eia923": "gen_og_eia923",
    "gen_fuel_by_generator_energy_source_eia923": "gen_fuel_by_genid_esc_eia923",
    "gen_fuel_by_generator_eia923": "gen_fuel_allocated_eia923",
    "gen_fuel_by_generator_energy_source_owner_eia923": "gen_fuel_by_genid_esc_own",
}
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


def read_pudl_table(
    table_name,
    *,
    schema: str | None = None,
    index_col: str | list[str] | None = None,
    coerce_float: bool = True,
    parse_dates: list[str] | dict[str, str] | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Retrieve a table (or query) from ``pudl.sqlite``.

    Essentially a partial of :func:`pandas.read_sql_table`, docstring is mostly lifted
    from there.

    Args:
        table_name: Name of SQL table in database.
        schema: Name of SQL schema in database to query (if database flavor
            supports this). Uses default schema if None (default).
        index_col: str or list of str, optional, default: None
            Column(s) to set as index(MultiIndex).
        coerce_float: Attempts to convert values of non-string, non-numeric objects
            (like decimal.Decimal) to floating point. Can result in loss of Precision.
        parse_dates:
            - List of column names to parse as dates.
            - Dict of ``{column_name: format string}`` where format string is
              strftime compatible in case of parsing string times or is one of
              (D, s, ns, ms, us) in case of parsing integer timestamps.
            - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
              to the keyword arguments of :func:`pandas.to_datetime`
              Especially useful with databases without native Datetime support,
              such as SQLite.
        columns: List of column names to select from SQL table.

    Returns: a table from Pudl as a df.

    """
    con = sa.create_engine(get_pudl_sql_url())
    try:
        con.connect()
    except sa.exc.OperationalError as exc:
        raise FileNotFoundError(f"Unable to connect to database at {con.url}") from exc
    else:
        if table_name not in sa.inspect(con).get_table_names():
            raise KeyError(f"{table_name} is not in {con.url}")
        return pd.read_sql_table(
            table_name=table_name,
            con=con,
            schema=schema,
            index_col=index_col,
            coerce_float=coerce_float,
            parse_dates=parse_dates,
            columns=columns,
        )


def _make_pudl_tabl(**kwargs):
    pudltabl = lazy_import("pudl.output.pudltabl", wait_for_signal=False)

    class PudlTabl(pudltabl.PudlTabl):
        def __getstate__(self) -> dict:
            """Get current object state for serializing (pickling).

            This method is run as part of pickling the object. It needs to return the
            object's current state with any un-serializable objects converted to a form
            that can be serialized. See :meth:`object.__getstate__` for further details
            on the expected behavior of this method.
            """
            return self.__dict__.copy() | {
                # defaultdict may be serializable but lambdas are not, so it must go
                "_dfs": dict(self.__dict__["_dfs"]),
                # sqlalchemy engines are also a problem here, saving the URL should
                # provide enough of what is needed to recreate it, though that means the
                # pickle is not portable, but any fix to that will happen when the
                # object is restored
                "pudl_engine": str(self.__dict__["pudl_engine"].url),
            }

        def __setstate__(self, state: dict) -> None:
            """Restore the object's state from a dictionary.

            This method is run when the object is restored from a pickle. Anything
            that was changed in :meth:`pudl.output.pudltabl.PudlTabl.__getstate__` must
            be undone here. Another important detail is that ``__init__`` is not run
            when an object is de-serialized, so any setup there that alters external
            state might need to happen here as well.

            Args:
                state: the object state to restore. This is effectively the output
                    of :meth:`pudl.output.pudltabl.PudlTabl.__getstate__`.
            """
            try:
                pudl_engine = sa.create_engine(state["pudl_engine"])
                # make sure that the URL for the engine from ``state`` is usable now
                pudl_engine.connect()
            except sa.exc.OperationalError:
                # if the URL from ``state`` is not valid, e.g. because it is for a local
                # DB on a different computer, create the engine from PUDL defaults
                pudl_uri = get_pudl_sql_url()
                logger.warning(
                    "Unable to connect to the restored pudl_db URL %s. "
                    "Will use the default pudl_db %s instead.",
                    state["pudl_engine"],
                    pudl_uri,
                )
                pudl_engine = sa.create_engine(pudl_uri)

            self.__dict__ = state | {
                # recreate the defaultdict from the vanilla one from ``state``
                "_dfs": defaultdict(lambda: None, state["_dfs"]),
                "pudl_engine": pudl_engine,
            }

    pudl_out = PudlTabl(sa.create_engine(get_pudl_sql_url()), **kwargs)
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
    freq: Literal["AS", "MS", None] = None,
    start_date: str | date | datetime | pd.Timestamp = None,
    end_date: str | date | datetime | pd.Timestamp = None,
    fill_fuel_cost: bool = False,
    roll_fuel_cost: bool = False,
    fill_net_gen: bool = False,
    fill_tech_desc: bool = True,
    unit_ids: bool = False,
):
    """Load a PudlTabl from cache or create a new one.

    Args:
        pudl_path: path to the existing DataZip containing a PudlTabl, or, if one does
            not exist yet, the path to where the DataZip of the PudlTabl will be stored.
        tables: tables that will be preloaded when creating a new PudlTabl.
        clobber: create a new PudlTabl cache even if the DataZip exists.
        freq: A string indicating the time frequency at which to aggregate
            reported data. ``MS`` is monththly and ``AS`` is annually. If
            None, the data will not be aggregated.
        start_date: Beginning date for data to pull from the PUDL DB. If
            a string, it should use the ISO 8601 ``YYYY-MM-DD`` format.
        end_date: End date for data to pull from the PUDL DB. If a string,
            it should use the ISO 8601 ``YYYY-MM-DD`` format.
        fill_fuel_cost: if True, fill in missing ``frc_eia923()`` fuel cost
            data with state-fuel averages from EIA's bulk electricity data.
        roll_fuel_cost: if True, apply a rolling average to a subset of
            output table's columns (currently only ``fuel_cost_per_mmbtu``
            for the ``fuel_receipts_costs_eia923`` table.)
        fill_net_gen: if True, use the net generation from the
            generation_fuel_eia923 - which is reported at the
            plant/fuel/prime mover level and  re-allocated to generators in
            ``mcoe()``, ``capacity_factor()`` and ``heat_rate_by_unit()``.
        fill_tech_desc: If True, fill the technology_description
            field to years earlier than 2013 based on plant and
            energy_source_code_1 and fill in technologies with only one matching
            code.
        unit_ids: If True, use several heuristics to assign
            individual generators to functional units. EXPERIMENTAL.
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

        pudl_out = _make_pudl_tabl(
            freq=freq,
            start_date=start_date,
            end_date=end_date,
            fill_fuel_cost=fill_fuel_cost,
            roll_fuel_cost=roll_fuel_cost,
            fill_net_gen=fill_net_gen,
            fill_tech_desc=fill_tech_desc,
            unit_ids=unit_ids,
        )
        for table in tables:
            try:
                internal_table = TABLE_NAME_MAP.get(table, table)
                df = getattr(pudl_out, table)()  # noqa: PD901
                if pudl_out._dfs[internal_table] is None:
                    pudl_out._dfs[internal_table] = df

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
        if args or kwargs:
            logger.warning("all arguments to _Faker are ignored.")
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
        self._real_pt = None

    def __getattr__(self, item):
        if (n_item := TABLE_NAME_MAP.get(item, item)) in self.__dict__["_dfs"]:
            return _Faker(self.__dict__["_dfs"][n_item])
        if self._real_pt is not None:
            return self._get_from_real_pt(item)
        if not any(("ferc" in item, "eia" in item, "epa" in item)):
            return _Faker(None)
        self._make_it_real()
        return self._get_from_real_pt(item)

    def _get_from_real_pt(self, item):
        df = getattr(self._real_pt, item)()  # noqa: PD901
        self.__dict__["_dfs"][TABLE_NAME_MAP.get(item, item)] = df
        return _Faker(df)

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
