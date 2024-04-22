"""Functions and objects for accessing PUDL data."""

import logging
import os
import warnings
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import ClassVar

import orjson as json
import pandas as pd
import polars as pl
from platformdirs import __version__ as platformdirs_version
from platformdirs import user_cache_path, user_config_path

from etoolbox.utils.misc import have_internet

logger = logging.getLogger("etoolbox")
TOKEN_PATH = user_config_path("rmi.pudl") / ".pudl-access-key.json"
CACHE_PATH = user_cache_path("rmi.pudl")


def rmi_pudl_init(b64_encoded: str | None = None) -> bool:
    """Setup rmi.pudl to provide access to PUDL tables from GCS with local caching.

    Args:
        b64_encoded: base64 encoding of the service_account json for PUDL GCS access.

    Returns: a boolean indicating if the token was written to disk.

    """
    if not CACHE_PATH.exists():
        CACHE_PATH.mkdir(parents=True)

    if TOKEN_PATH.exists():
        print(f"Using existing token at {TOKEN_PATH}")
        return False
    elif not TOKEN_PATH.parent.exists():
        TOKEN_PATH.parent.mkdir(parents=True)

    if b64_encoded is None:
        from argparse import ArgumentParser

        parser = ArgumentParser(
            description="Setup rmi.pudl to provide access to PUDL tables from GCS with "
            "local caching. You must provide a base64 encoding (as a string) of a "
            "service_account json."
        )
        parser.add_argument(
            "b64_encoded",
            help="base64 encoding of the service_account json for PUDL GCS access. "
            "This can be pasted directly into the terminal.",
        )
        b64_encoded = parser.parse_args().b64_encoded
        if b64_encoded in (None, "None"):
            raise RuntimeError(
                "b64_encoded must be provided if TOKEN_PATH does not exist. This can "
                "happen if you have not run `rmi-pudl-init` yet."
            )

    from base64 import b64decode

    try:
        as_json = json.loads(b64decode(b64_encoded))
    except Exception as exc:
        raise RuntimeError("Failed to decode json from base64") from exc
    else:
        with open(TOKEN_PATH, "wb") as f:
            f.write(json.dumps(as_json, option=json.OPT_INDENT_2))
        print(f"Wrote decoded json to {TOKEN_PATH}")
    return True


def _gcs_token_path(token: str | None) -> Path:
    if token is None:
        token = TOKEN_PATH
        if not token.exists():
            if platformdirs_version < "3.0.0":
                raise RuntimeError(
                    f"{platformdirs_version=} < 3.0.0 required for PUDL GCS access."
                )
            raise RuntimeError(
                "Provide a token for PUDL GCS access or run 'rmi-pudl-init' first"
            )
    return token


@lru_cache
def _gcs_token_dict(token: str | None) -> dict[str, str]:
    """Token for PUDL GCS access as a dict."""
    return json.loads(_gcs_token_path(token).read_text())


def _gcs_filecache_filesystem(token):
    """Create a fsspec/GCS filesystem with a filecache.

    Args:
        token: token or path to token for PUDL GCS access

    """
    from fsspec import filesystem

    return filesystem(
        "filecache",
        target_protocol="gcs",
        target_options={"token": _gcs_token_dict(token)},
        cache_storage=str(CACHE_PATH),
        cache_timeout=None,
        check_files=True,
    )


def _cache_path(table_name: str, release: str = "nightly") -> Path | None:
    """Determine the local path for a cached PUDL table if it exists.

    Args:
        table_name: name of pudl table
        release: version of pudl table, i.e. 'nightly'

    Returns: Path of cached PUDL table

    """
    if have_internet():
        fs = _gcs_filecache_filesystem(None)
        if table_cache_data := fs._check_file(
            f"parquet.catalyst.coop/{release}/{table_name}.parquet"
        ):
            return Path(table_cache_data[1])
        return None
    cache_info_path = CACHE_PATH / "cache"
    if cache_info_path.exists():
        with open(cache_info_path) as f:
            cache_data = json.loads(f.read())
        if table_cache_data := cache_data.get(
            f"parquet.catalyst.coop/{release}/{table_name}.parquet"
        ):
            warnings.warn(
                f"Unable to validate cache for Table {release}/{table_name} using "
                f"table as of {datetime.fromtimestamp(table_cache_data['time'])}",
                UserWarning,
                stacklevel=2,
            )
            return CACHE_PATH / table_cache_data["fn"]
    return None


def pudl_list(
    release: str = "nightly", token: str | None = None, *, detail: bool = False
) -> list[str | dict[str, str | int]]:
    """List PUDL tables in GCS using ``ls`` command.

    Args:
        release: ``nightly``, ``stable`` or versioned, pass ``None`` to list all
        token: token or path to token for PUDL GCS access
        detail: if True, return details of each table, otherwise just names

    """
    fs = _gcs_filecache_filesystem(token)
    ls = (
        fs.ls(f"parquet.catalyst.coop/{release}")
        if release
        else fs.ls("parquet.catalyst.coop")
    )
    if detail:
        return ls
    return [i["name"] for i in fs.ls(f"parquet.catalyst.coop/{release}")]


def pd_read_pudl(
    table_name: str, release: str = "nightly", token: str | None = None, **kwargs
) -> pd.DataFrame:
    """Read PUDL table from GCS as :class:`pandas.DataFrame`.

    Args:
        table_name: name of table in PUDL sqlite database
        release: ``nightly`` or ``stable``
        token: token or path to token for PUDL GCS access
        kwargs: passed to :func:`pandas.read_parquet`

    """
    if have_internet():
        return pd.read_parquet(
            f"gs://parquet.catalyst.coop/{release}/{table_name}.parquet",
            filesystem=_gcs_filecache_filesystem(token),
            **kwargs,
        )
    return pd.read_parquet(
        _cache_path(table_name=table_name, release=release), **kwargs
    )


def pl_scan_pudl(
    table_name: str,
    release: str = "nightly",
    token: str | None = None,
    *,
    use_polars=False,
    **kwargs,
) -> pl.LazyFrame:
    """Read PUDL table from GCS as :class:`polars.LazyFrame`.

    .. note::

       Accessing PUDL tables directly from GCS using polars requires version 0.20
       or higher.

    Args:
        table_name: name of table in PUDL sqlite database
        release: ``nightly`` or ``stable``
        token: path to token for PUDL GCS access
        use_polars: If ``True``, use polars GCP client, this does not
            work with local caching. If ``False``, use
            :class:`fsspec.implementations.cached.WholeFileCacheFileSystem`
            for file access and caching.
        kwargs: passed to :func:`polars.scan_parquet`
    """
    if use_polars:
        if pl.__version__ < "0.20.0":
            raise RuntimeError(
                f"Accessing PUDL tables directly from GCS using polars requires "
                f"version 0.20 or higher, current version: {pl.__version__}"
            )
        if have_internet():
            return pl.scan_parquet(
                f"gs://parquet.catalyst.coop/{release}/{table_name}.parquet",
                storage_options={
                    "google_service_account_path": str(_gcs_token_path(token))
                },
                **kwargs,
            )
        raise FileNotFoundError(f"Unable to load {release}/{table_name} from GCS.")
    if cached_path := _cache_path(table_name=table_name, release=release):
        return pl.scan_parquet(cached_path)
    if have_internet():
        fs = _gcs_filecache_filesystem(token)
        return pl.scan_parquet(
            fs.open(f"gs://parquet.catalyst.coop/{release}/{table_name}.parquet").name
        )
    raise FileNotFoundError(
        f"Unable to load {release}/{table_name} from GCS or local cache."
    )


def pl_read_pudl(
    table_name: str,
    release: str = "nightly",
    token: str | None = None,
    *,
    use_polars=False,
    **kwargs,
) -> pl.DataFrame:
    """Read PUDL table from GCS as :class:`polars.DataFrame`.

    .. note::

       Accessing PUDL tables directly from GCS using polars requires version 0.20
       or higher.

    Args:
        table_name: name of table in PUDL sqlite database
        release: ``nightly`` or ``stable``
        token: path to token for PUDL GCS access
        use_polars: use polars GCP client rather than GCSFs, this does not
            work with local caching
        kwargs: passed to :func:`polars.scan_parquet`


    """
    return pl_scan_pudl(
        table_name=table_name,
        release=release,
        token=token,
        use_polars=use_polars,
        **kwargs,
    ).collect()


"""
====================================== DEPRECATED ======================================
These are here for backwards compatibility and will eventually be removed.
"""
PUDL_CONFIG = Path.home() / ".pudl.yml"
SUGGESTION = """
    from etoolbox.utils.pudl import pd_read_pudl

    pd_read_pudl(table_name)

    --- OR ---

    import pandas as pd
    import sqlalchemy as sa

    from etoolbox.utils.pudl import get_pudl_sql_url, conform_pudl_dtypes

    pd.read_sql_table(table_name, sa.create_engine(get_pudl_sql_url())).pipe(
        conform_pudl_dtypes
    )

    --- OR ---

    import polars as pl

    pl.read_database("SELECT * FROM table_name", get_pudl_sql_url())

    Current and new table names can be found here:
    https://docs.google.com/spreadsheets/d/1RBuKl_xKzRSLgRM7GIZbc5zUYieWFE20cXumWuv5njo/edit#gid=1126117325
    """
WARNING_TEXT = (
    "This function will soon be removed. Read tables directly:\n" + SUGGESTION
)
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


class _WarnDict(dict):
    def __getitem__(self, item):
        raise DeprecationWarning(
            "Use `conform_pudl_dtypes(df)` or `df.pipe(conform_pudl_dtypes)`"
        )

    def get(self, item, default=None):
        return self[item]


PUDL_DTYPES = _WarnDict()


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
                    f"from https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl.sqlite.gz"
                ) from None
    return f"sqlite:///{pudl_path}"


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

    table_name_map: ClassVar[dict[str, str]] = {
        "gen_original_eia923": "gen_og_eia923",
        "gen_fuel_by_generator_energy_source_eia923": "gen_fuel_by_genid_esc_eia923",
        "gen_fuel_by_generator_eia923": "gen_fuel_allocated_eia923",
        "gen_fuel_by_generator_energy_source_owner_eia923": "gen_fuel_by_genid_esc_own",
    }

    def __setstate__(self, state):
        warnings.warn(WARNING_TEXT, DeprecationWarning, stacklevel=2)

        self.__dict__ = state
        self._real_pt = None

    def __getattr__(self, item):
        warnings.warn(WARNING_TEXT, DeprecationWarning, stacklevel=2)

        if (n_item := self.table_name_map.get(item, item)) in self.__dict__["_dfs"]:
            return _Faker(self.__dict__["_dfs"][n_item])
        raise KeyError(
            f"{item} not found, available tables: {tuple(self.__dict__['_dfs'])}"
        )


PretendPudlTabl = PretendPudlTablCore
