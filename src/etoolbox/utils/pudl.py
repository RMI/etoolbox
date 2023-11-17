"""Functions and objects for creating and ~mocking PudlTabl."""
import logging
import os
import warnings
from collections import defaultdict
from collections.abc import Sequence
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Literal

import pandas as pd
import sqlalchemy as sa

from etoolbox.utils.lazy_import import lazy_import

logger = logging.getLogger(__name__)
PUDL_CONFIG = Path.home() / ".pudl.yml"
TABLE_NAME_MAP = {
    "gen_original_eia923": "gen_og_eia923",
    "gen_fuel_by_generator_energy_source_eia923": "gen_fuel_by_genid_esc_eia923",
    "gen_fuel_by_generator_eia923": "gen_fuel_allocated_eia923",
    "gen_fuel_by_generator_energy_source_owner_eia923": "gen_fuel_by_genid_esc_own",
}
DTYPES = pd.Series(
    {
        "address_2": "string",
        "air_flow_100pct_load_cubic_feet_per_minute": "float64",
        "ash_content_pct": "float64",
        "ash_impoundment": "boolean",
        "ash_impoundment_lined": "boolean",
        "ash_impoundment_status": "string",
        "associated_combined_heat_power": "boolean",
        "attention_line": "string",
        "balancing_authority_code_eia": "string",
        "balancing_authority_code_eia_consistent_rate": "float64",
        "balancing_authority_name_eia": "string",
        "bga_source": "string",
        "boiler_fuel_code_1": "string",
        "boiler_fuel_code_2": "string",
        "boiler_fuel_code_3": "string",
        "boiler_fuel_code_4": "string",
        "boiler_generator_assn_type_code": "object",
        "boiler_id": "string",
        "boiler_manufacturer": "string",
        "boiler_manufacturer_code": "string",
        "boiler_operating_date": "datetime64[ns]",
        "boiler_retirement_date": "datetime64[ns]",
        "boiler_status": "string",
        "boiler_type": "string",
        "bypass_heat_recovery": "boolean",
        "capacity_mw": "float64",
        "carbon_capture": "boolean",
        "chlorine_content_ppm": "float64",
        "city": "string",
        "coalmine_county_id_fips": "string",
        "cofire_fuels": "boolean",
        "compliance_year_mercury": "Int64",
        "compliance_year_nox": "Int64",
        "compliance_year_particulate": "Int64",
        "compliance_year_so2": "Int64",
        "contact_firstname": "string",
        "contact_firstname_2": "string",
        "contact_lastname": "string",
        "contact_lastname_2": "string",
        "contact_title": "string",
        "contact_title_2": "string",
        "contract_expiration_date": "datetime64[ns]",
        "contract_type_code": "string",
        "county": "string",
        "current_planned_generator_operating_date": "datetime64[ns]",
        "data_maturity": "string",
        "datum": "string",
        "deliver_power_transgrid": "boolean",
        "distributed_generation": "boolean",
        "duct_burners": "boolean",
        "efficiency_100pct_load": "float64",
        "efficiency_50pct_load": "float64",
        "emissions_unit_id_epa": "string",
        "energy_source_1_transport_1": "string",
        "energy_source_1_transport_2": "string",
        "energy_source_1_transport_3": "string",
        "energy_source_2_transport_1": "string",
        "energy_source_2_transport_2": "string",
        "energy_source_2_transport_3": "string",
        "energy_source_code": "string",
        "energy_source_code_1": "string",
        "energy_source_code_2": "string",
        "energy_source_code_3": "string",
        "energy_source_code_4": "string",
        "energy_source_code_5": "string",
        "energy_source_code_6": "string",
        "energy_storage": "boolean",
        "energy_storage_capacity_mwh": "float64",
        "entity_type": "string",
        "ferc_cogen_docket_no": "string",
        "ferc_cogen_status": "boolean",
        "ferc_exempt_wholesale_generator": "boolean",
        "ferc_exempt_wholesale_generator_docket_no": "string",
        "ferc_qualifying_facility": "boolean",
        "ferc_qualifying_facility_docket_no": "string",
        "ferc_small_power_producer": "boolean",
        "ferc_small_power_producer_docket_no": "string",
        "firing_rate_using_coal_tons_per_hour": "float64",
        "firing_rate_using_gas_mcf_per_hour": "float64",
        "firing_rate_using_oil_bbls_per_hour": "float64",
        "firing_rate_using_other_fuels": "float64",
        "firing_type_1": "string",
        "firing_type_2": "string",
        "firing_type_3": "string",
        "fluidized_bed_tech": "boolean",
        "fly_ash_reinjection": "boolean",
        "fraction_owned": "float64",
        "fuel_consumed_for_electricity_mmbtu": "float64",
        "fuel_consumed_for_electricity_units": "float64",
        "fuel_consumed_mmbtu": "float64",
        "fuel_consumed_units": "float64",
        "fuel_cost_from_eiaapi": "bool",
        "fuel_cost_per_mmbtu": "float64",
        "fuel_group_code": "string",
        "fuel_mmbtu_per_unit": "float64",
        "fuel_received_units": "float64",
        "fuel_type_code_aer": "string",
        "fuel_type_code_pudl": "string",
        "fuel_type_count": "int64",
        "generator_id": "string",
        "generator_id_epa": "object",
        "generator_operating_date": "datetime64[ns]",
        "generator_retirement_date": "datetime64[ns]",
        "grid_voltage_1_kv": "float64",
        "grid_voltage_2_kv": "float64",
        "grid_voltage_3_kv": "float64",
        "hrsg": "boolean",
        "iso_rto_code": "string",
        "latitude": "float64",
        "liquefied_natural_gas_storage": "boolean",
        "longitude": "float64",
        "max_steam_flow_1000_lbs_per_hour": "float64",
        "mercury_content_ppm": "float64",
        "mercury_control_existing_strategy_1": "string",
        "mercury_control_existing_strategy_2": "string",
        "mercury_control_existing_strategy_3": "string",
        "mercury_control_existing_strategy_4": "string",
        "mercury_control_existing_strategy_5": "string",
        "mercury_control_existing_strategy_6": "string",
        "mercury_control_proposed_strategy_1": "string",
        "mercury_control_proposed_strategy_2": "string",
        "mercury_control_proposed_strategy_3": "string",
        "mine_id_msha": "Int64",
        "mine_name": "string",
        "mine_state": "object",
        "mine_type_code": "string",
        "minimum_load_mw": "float64",
        "moisture_content_pct": "float64",
        "multiple_fuels": "boolean",
        "nameplate_power_factor": "float64",
        "natural_gas_delivery_contract_type_code": "string",
        "natural_gas_local_distribution_company": "string",
        "natural_gas_pipeline_name_1": "string",
        "natural_gas_pipeline_name_2": "string",
        "natural_gas_pipeline_name_3": "string",
        "natural_gas_storage": "boolean",
        "natural_gas_transport_code": "string",
        "nerc_region": "string",
        "net_capacity_mwdc": "float64",
        "net_generation_mwh": "float64",
        "net_metering": "boolean",
        "new_source_review": "boolean",
        "new_source_review_date": "datetime64[ns]",
        "new_source_review_permit": "string",
        "nox_control_existing_caaa_compliance_strategy_1": "string",
        "nox_control_existing_caaa_compliance_strategy_2": "string",
        "nox_control_existing_caaa_compliance_strategy_3": "string",
        "nox_control_existing_strategy_1": "string",
        "nox_control_existing_strategy_2": "string",
        "nox_control_existing_strategy_3": "string",
        "nox_control_manufacturer": "string",
        "nox_control_manufacturer_code": "string",
        "nox_control_out_of_compliance_strategy_1": "string",
        "nox_control_out_of_compliance_strategy_2": "string",
        "nox_control_out_of_compliance_strategy_3": "string",
        "nox_control_planned_caaa_compliance_strategy_1": "string",
        "nox_control_planned_caaa_compliance_strategy_2": "string",
        "nox_control_planned_caaa_compliance_strategy_3": "string",
        "nox_control_proposed_strategy_1": "string",
        "nox_control_proposed_strategy_2": "string",
        "nox_control_proposed_strategy_3": "string",
        "nox_control_status_code": "string",
        "nuclear_unit_id": "string",
        "operating_switch": "string",
        "operational_status": "string",
        "operational_status_code": "string",
        "original_planned_generator_operating_date": "datetime64[ns]",
        "other_combustion_tech": "boolean",
        "other_modifications_date": "datetime64[ns]",
        "other_planned_modifications": "boolean",
        "owned_by_non_utility": "boolean",
        "owner_city": "string",
        "owner_country": "string",
        "owner_name": "string",
        "owner_state": "string",
        "owner_street_address": "string",
        "owner_utility_id_eia": "Int64",
        "owner_zip_code": "string",
        "ownership_code": "string",
        "particulate_control_out_of_compliance_strategy_1": "string",
        "particulate_control_out_of_compliance_strategy_2": "string",
        "particulate_control_out_of_compliance_strategy_3": "string",
        "phone_extension": "string",
        "phone_extension_2": "string",
        "phone_number": "string",
        "phone_number_2": "string",
        "pipeline_notes": "string",
        "planned_derate_date": "datetime64[ns]",
        "planned_energy_source_code_1": "string",
        "planned_generator_retirement_date": "datetime64[ns]",
        "planned_modifications": "boolean",
        "planned_net_summer_capacity_derate_mw": "float64",
        "planned_net_summer_capacity_uprate_mw": "float64",
        "planned_net_winter_capacity_derate_mw": "float64",
        "planned_net_winter_capacity_uprate_mw": "float64",
        "planned_new_capacity_mw": "float64",
        "planned_new_prime_mover_code": "string",
        "planned_repower_date": "datetime64[ns]",
        "planned_uprate_date": "datetime64[ns]",
        "plant_id_eia": "Int64",
        "plant_id_epa": "Int64",
        "plant_id_pudl": "Int64",
        "plant_name_eia": "string",
        "plants_reported_asset_manager": "boolean",
        "plants_reported_operator": "boolean",
        "plants_reported_other_relationship": "boolean",
        "plants_reported_owner": "boolean",
        "previously_canceled": "boolean",
        "primary_purpose_id_naics": "Int64",
        "primary_transportation_mode_code": "string",
        "prime_mover_code": "string",
        "pulverized_coal_tech": "boolean",
        "reactive_power_output_mvar": "float64",
        "regulation_mercury": "string",
        "regulation_nox": "string",
        "regulation_particulate": "string",
        "regulation_so2": "string",
        "regulatory_status_code": "string",
        "report_date": "datetime64[ns]",
        "reporting_frequency_code": "string",
        "rto_iso_lmp_node_id": "string",
        "rto_iso_location_wholesale_reporting_id": "string",
        "secondary_transportation_mode_code": "string",
        "sector_id_eia": "Int64",
        "sector_name_eia": "string",
        "service_area": "string",
        "so2_control_existing_caaa_compliance_strategy_1": "string",
        "so2_control_existing_caaa_compliance_strategy_2": "string",
        "so2_control_existing_caaa_compliance_strategy_3": "string",
        "so2_control_existing_strategy_1": "string",
        "so2_control_existing_strategy_2": "string",
        "so2_control_existing_strategy_3": "string",
        "so2_control_out_of_compliance_strategy_1": "string",
        "so2_control_out_of_compliance_strategy_2": "string",
        "so2_control_out_of_compliance_strategy_3": "string",
        "so2_control_planned_caaa_compliance_strategy_1": "string",
        "so2_control_planned_caaa_compliance_strategy_2": "string",
        "so2_control_planned_caaa_compliance_strategy_3": "string",
        "so2_control_proposed_strategy_1": "string",
        "so2_control_proposed_strategy_2": "string",
        "so2_control_proposed_strategy_3": "string",
        "solid_fuel_gasification": "boolean",
        "standard_nox_rate": "float64",
        "standard_particulate_rate": "float64",
        "standard_so2_percent_scrubbed": "float64",
        "standard_so2_rate": "float64",
        "startup_source_code_1": "string",
        "startup_source_code_2": "string",
        "startup_source_code_3": "string",
        "startup_source_code_4": "string",
        "state": "string",
        "steam_plant_type_code": "float64",
        "stoker_tech": "boolean",
        "street_address": "string",
        "subcritical_tech": "boolean",
        "subplant_id": "Int64",
        "sulfur_content_pct": "float64",
        "summer_capacity_estimate": "boolean",
        "summer_capacity_mw": "float64",
        "summer_estimated_capability_mw": "float64",
        "supercritical_tech": "boolean",
        "supplier_name": "string",
        "switch_oil_gas": "boolean",
        "syncronized_transmission_grid": "boolean",
        "technology_description": "string",
        "time_cold_shutdown_full_load_code": "string",
        "timezone": "string",
        "topping_bottoming_code": "string",
        "total_fuel_cost": "float64",
        "transmission_distribution_owner_id": "Int64",
        "transmission_distribution_owner_name": "string",
        "transmission_distribution_owner_state": "string",
        "turbines_inverters_hydrokinetics": "Int64",
        "turbines_num": "Int64",
        "turndown_ratio": "float64",
        "ultrasupercritical_tech": "boolean",
        "unit_id_eia": "object",
        "unit_id_pudl": "Int64",
        "unit_nox": "string",
        "unit_particulate": "string",
        "unit_so2": "string",
        "uprate_derate_completed_date": "datetime64[ns]",
        "uprate_derate_during_year": "boolean",
        "utility_id_eia": "Int64",
        "utility_id_pudl": "Int64",
        "utility_name_eia": "string",
        "waste_heat_input_mmbtu_per_hour": "float64",
        "water_source": "string",
        "wet_dry_bottom": "string",
        "winter_capacity_estimate": "boolean",
        "winter_capacity_mw": "float64",
        "winter_estimated_capability_mw": "float64",
        "zip_code": "string",
        "zip_code_4": "string",
    }
)
SUGGESTION = """
    import pandas as pd
    import sqlalchemy as sa

    from etoolbox.utils.pudl import get_pudl_sql_url, conform_pudl_dtypes

    pd.read_sql_table(table_name, sa.create_engine(get_pudl_sql_url())).pipe(
        conform_pudl_dtypes
    )

    import polars as pl

    pl.read_database("SELECT * FROM table_name", get_pudl_sql_url())

    Current and new table names can be found here:
    https://docs.google.com/spreadsheets/d/1RBuKl_xKzRSLgRM7GIZbc5zUYieWFE20cXumWuv5njo/edit#gid=1126117325
    """
WARNING_TEXT = (
    "This function will soon be removed. Read tables directly:\n" + SUGGESTION
)


class _WarnDict(dict):
    def __getitem__(self, item):
        raise DeprecationWarning(
            "Use `conform_pudl_dtypes(df)` or `df.pipe(conform_pudl_dtypes)`"
        )

    def get(self, item, default=None):
        return self[item]


PUDL_DTYPES = _WarnDict()


def conform_pudl_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Conform types of PUDL columns to those in PudlTabl.

    Args:
        df: a dataframe with columns from PUDL

    Returns: the pudl table with standardized dtypes

    Examples
    --------
    .. code-block:: python

        import pandas as pd
        import sqlalchemy as sa

        from etoolbox.utils.pudl import get_pudl_sql_url, conform_pudl_dtypes

        pd.read_sql_table(table_name, sa.create_engine(get_pudl_sql_url())).pipe(
            conform_pudl_dtypes
        )

    """
    return df.astype(DTYPES.loc[df.columns.intersection(DTYPES.index)].to_dict())


def get_pudl_sql_url(file=PUDL_CONFIG) -> str:
    """Get the URL for the pudl.sqlite DB."""
    try:
        pudl_path = f"{os.environ['PUDL_OUTPUT']}/pudl.sqlite"
    except KeyError:
        if file.exists():
            import yaml

            with open(file, "r") as f:  # noqa: UP015
                pudl_path = f"{yaml.safe_load(f)['pudl_out']}/output/pudl.sqlite"
        else:
            pudl_path = Path.home() / "pudl-work/output/pudl.sqlite"
            if not pudl_path.exists():
                raise FileNotFoundError(
                    f"~/.pudl.yml is missing, 'PUDL_OUTPUT' environment variable is "
                    f"not set, and pudl.sqlite is not at {pudl_path}. Please move your "
                    f"pudl.sqlite to {pudl_path}. The sqlite file can be downloaded "
                    f"from https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/dev/pudl.sqlite"
                ) from None
    return f"sqlite:///{pudl_path}"


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
        try:
            out = pd.read_sql_table(
                table_name=table_name,
                con=con,
                schema=schema,
                index_col=index_col,
                coerce_float=coerce_float,
                parse_dates=parse_dates,
                columns=columns,
            )
        except AttributeError:
            out = pd.read_sql_table(
                table_name=table_name,
                con=con.connect(),
                schema=schema,
                index_col=index_col,
                coerce_float=coerce_float,
                parse_dates=parse_dates,
                columns=columns,
            )
        return out.pipe(conform_pudl_dtypes)


def get_pudl_tables_as_dz(tables: Sequence[str]) -> BytesIO:
    """Grab set of pudl tables and put them in a DataZip.

    Args:
        tables: tables in `PUDL.sqlite`

    Returns: DataZip

    """
    from etoolbox.datazip import DataZip

    with DataZip(buffer := BytesIO()) as dz:
        for table in tables:
            dz[table] = read_pudl_table(table).pipe(conform_pudl_dtypes)
    return buffer


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
    tables: tuple | list = (
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
    start_date: str | date | datetime | pd.Timestamp | None = None,
    end_date: str | date | datetime | pd.Timestamp | None = None,
    fill_fuel_cost: bool = False,
    roll_fuel_cost: bool = False,
    fill_net_gen: bool = False,
    fill_tech_desc: bool = True,
    unit_ids: bool = False,
):
    """Load a PudlTabl from cache or create a new one.

    .. admonition:: DeprecationWarning
       :class: warning

       ``make_pudl_tabl`` will be removed in a future version, read tables directly
       from the sqlite.

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
    warnings.warn(WARNING_TEXT, DeprecationWarning, stacklevel=2)

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


class PretendPudlTablCore:
    """A DataZip of a PudlTabl can be recreated with this to avoid importing PUDL.

    .. admonition:: DeprecationWarning
       :class: warning

       ``PretendPudlTablCore`` will be removed in a future version, read tables directly
       from the sqlite.

    """

    def __setstate__(self, state):
        warnings.warn(WARNING_TEXT, DeprecationWarning, stacklevel=2)

        self.__dict__ = state
        self._real_pt = None

    def __getattr__(self, item):
        warnings.warn(WARNING_TEXT, DeprecationWarning, stacklevel=2)

        if (n_item := TABLE_NAME_MAP.get(item, item)) in self.__dict__["_dfs"]:
            return _Faker(self.__dict__["_dfs"][n_item])
        if self._real_pt is not None:
            return self._get_from_real_pt(item)
        if not any(("ferc" in item, "eia" in item, "epa" in item)):
            return _Faker(None)
        self._make_it_real()
        return self._get_from_real_pt(item)

    def _get_from_real_pt(self, item):
        raise KeyError(
            f"{item} not found, available tables: {tuple(self.__dict__['_dfs'])}"
        )

    def _make_it_real(self):
        return None


class PretendPudlTabl(PretendPudlTablCore):
    """A DataZip of a PudlTabl can be recreated with this to avoid importing PUDL.

    .. admonition:: DeprecationWarning
       :class: warning

       ``PretendPudlTabl`` will be removed in a future version, read tables directly
       from the sqlite.

    """

    def _get_from_real_pt(self, item):
        df = getattr(self._real_pt, item)()  # noqa: PD901
        self.__dict__["_dfs"][TABLE_NAME_MAP.get(item, item)] = df
        return _Faker(df)

    def _make_it_real(self):
        raise RuntimeError(
            "Silently creating a real PudlTabl is no longer supported. "
            "Read the table directly: \n" + SUGGESTION
        )
        # from collections import defaultdict
        #
        # logger.warning("We're making a real PudlTabl")
        #
        # try:
        #     pt = _make_pudl_tabl()
        #     pt._dfs = defaultdict(lambda: None, self.__dict__["_dfs"])
        # except Exception as exc:
        #     raise ModuleNotFoundError(
        #         "I am only a pretend PudlTabl. I tried to load a real one "
        #         "but wasn't able to because PUDL is not installed."
        #     ) from exc
        # else:
        #     self._real_pt = pt
