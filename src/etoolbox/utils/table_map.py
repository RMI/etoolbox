"""Script to rename PUDL tables in a set of files."""

from pathlib import Path

PUDL_TABLE_MAP = {
    "clean_balancing_authority_eia861": "_core_eia861__balancing_authority",
    "clean_boiler_cooling_assn_eia860": "_core_eia860__boiler_cooling_assn",
    "clean_boiler_emissions_control_equipment_assn_eia860": "_core_eia860__boiler_emissions_control_equipment_assn",
    "clean_boiler_fuel_eia923": "_core_eia923__boiler_fuel",
    "clean_boiler_generator_assn_eia860": "_core_eia860__boiler_generator_assn",
    "clean_boiler_stack_flue_assn_eia860": "_core_eia860__boiler_stack_flue_assn",
    "clean_boilers_eia860": "_core_eia860__boilers",
    "clean_coalmine_eia923": "_core_eia923__coalmine",
    "clean_emissions_control_equipment_eia860": "_core_eia860__emissions_control_equipment",
    "clean_fuel_receipts_costs_eia923": "_core_eia923__fuel_receipts_costs",
    "clean_generation_eia923": "_core_eia923__generation",
    "clean_generation_fuel_eia923": "_core_eia923__generation_fuel",
    "clean_generation_fuel_nuclear_eia923": "_core_eia923__generation_fuel_nuclear",
    "clean_generators_eia860": "_core_eia860__generators",
    "clean_ownership_eia860": "_core_eia860__ownership",
    "clean_plants_eia860": "_core_eia860__plants",
    "clean_utilities_eia860": "_core_eia860__utilities",
    "emissions_unit_ids_epacems": "_core_epacems__emissions_unit_ids",
    "epacamd_eia_unique": "_core_epa__assn_eia_epacamd_unique",
    "advanced_metering_infrastructure_eia861": "core_eia861__yearly_advanced_metering_infrastructure",
    "averaging_periods_eia": "core_eia__codes_averaging_periods",
    "balance_sheet_assets_ferc1": "core_ferc1__yearly_balance_sheet_assets_sched110",
    "balance_sheet_liabilities_ferc1": "core_ferc1__yearly_balance_sheet_liabilities_sched110",
    "balancing_authorities_eia": "core_eia__codes_balancing_authorities",
    "balancing_authority_assn_eia861": "core_eia861__assn_balancing_authority",
    "balancing_authority_eia861": "core_eia861__yearly_balancing_authority",
    "boiler_cooling_assn_eia860": "core_eia860__assn_boiler_cooling",
    "boiler_emissions_control_equipment_assn_eia860": "core_eia860__assn_yearly_boiler_emissions_control_equipment",
    "boiler_fuel_eia923": "core_eia923__monthly_boiler_fuel",
    "boiler_generator_assn_eia860": "core_eia860__assn_boiler_generator",
    "boiler_generator_assn_types_eia": "core_eia__codes_boiler_generator_assn_types",
    "boiler_stack_flue_assn_eia860": "core_eia860__assn_boiler_stack_flue",
    "boiler_status_eia": "core_eia__codes_boiler_status",
    "boiler_types_eia": "core_eia__codes_boiler_types",
    "boilers_eia860": "core_eia860__scd_boilers",
    "boilers_entity_eia": "core_eia__entity_boilers",
    "cash_flow_ferc1": "core_ferc1__yearly_cash_flows_sched120",
    "coalmine_eia923": "core_eia923__entity_coalmine",
    "coalmine_types_eia": "core_eia__codes_coalmine_types",
    "contract_types_eia": "core_eia__codes_contract_types",
    "county_censusdp1": "core_censusdp1__entity_county",
    "data_maturities": "core_pudl__codes_data_maturities",
    "datasources": "core_pudl__codes_datasources",
    "demand_hourly_pa_ferc714": "core_ferc714__hourly_demand_pa",
    "demand_response_eia861": "core_eia861__yearly_demand_response",
    "demand_response_water_heater_eia861": "core_eia861__yearly_demand_response_water_heater",
    "demand_side_management_ee_dr_eia861": "core_eia861__yearly_demand_side_management_ee_dr",
    "demand_side_management_misc_eia861": "core_eia861__yearly_demand_side_management_misc",
    "demand_side_management_sales_eia861": "core_eia861__yearly_demand_side_management_sales",
    "depreciation_amortization_summary_ferc1": "core_ferc1__yearly_depreciation_summary_sched336",
    "distributed_generation_fuel_eia861": "core_eia861__yearly_distributed_generation_fuel",
    "distributed_generation_misc_eia861": "core_eia861__yearly_distributed_generation_misc",
    "distributed_generation_tech_eia861": "core_eia861__yearly_distributed_generation_tech",
    "distribution_systems_eia861": "core_eia861__yearly_distribution_systems",
    "dynamic_pricing_eia861": "core_eia861__yearly_dynamic_pricing",
    "electric_energy_dispositions_ferc1": "core_ferc1__yearly_energy_dispositions_sched401",
    "electric_energy_sources_ferc1": "core_ferc1__yearly_energy_sources_sched401",
    "electric_operating_expenses_ferc1": "core_ferc1__yearly_operating_expenses_sched320",
    "electric_operating_revenues_ferc1": "core_ferc1__yearly_operating_revenues_sched300",
    "electric_plant_depreciation_changes_ferc1": "core_ferc1__yearly_depreciation_changes_sched219",
    "electric_plant_depreciation_functional_ferc1": "core_ferc1__yearly_depreciation_by_function_sched219",
    "electricity_sales_by_rate_schedule_ferc1": "core_ferc1__yearly_sales_by_rate_schedules_sched304",
    "emission_control_equipment_types_eia": "core_eia__codes_emission_control_equipment_types",
    "emissions_control_equipment_eia860": "core_eia860__scd_emissions_control_equipment",
    "energy_efficiency_eia861": "core_eia861__yearly_energy_efficiency",
    "energy_sources_eia": "core_eia__codes_energy_sources",
    "environmental_equipment_manufacturers_eia": "core_eia__codes_environmental_equipment_manufacturers",
    "epacamd_eia": "core_epa__assn_eia_epacamd",
    "epacamd_eia_subplant_ids": "core_epa__assn_eia_epacamd_subplant_ids",
    "ferc_accounts": "core_ferc__codes_accounts",
    "firing_types_eia": "core_eia__codes_firing_types",
    "fuel_ferc1": "core_ferc1__yearly_steam_plants_fuel_sched402",
    "fuel_receipts_costs_aggs_eia": "core_eia__yearly_fuel_receipts_costs_aggs",
    "fuel_receipts_costs_eia923": "core_eia923__monthly_fuel_receipts_costs",
    "fuel_transportation_modes_eia": "core_eia__codes_fuel_transportation_modes",
    "fuel_types_aer_eia": "core_eia__codes_fuel_types_aer",
    "generation_eia923": "core_eia923__monthly_generation",
    "generation_fuel_eia923": "core_eia923__monthly_generation_fuel",
    "generation_fuel_nuclear_eia923": "core_eia923__monthly_generation_fuel_nuclear",
    "generators_eia860": "core_eia860__scd_generators",
    "generators_entity_eia": "core_eia__entity_generators",
    "green_pricing_eia861": "core_eia861__yearly_green_pricing",
    "hourly_emissions_epacems": "core_epacems__hourly_emissions",
    "income_statement_ferc1": "core_ferc1__yearly_income_statements_sched114",
    "mercury_compliance_strategies_eia": "core_eia__codes_mercury_compliance_strategies",
    "mergers_eia861": "core_eia861__yearly_mergers",
    "momentary_interruptions_eia": "core_eia__codes_momentary_interruptions",
    "net_metering_customer_fuel_class_eia861": "core_eia861__yearly_net_metering_customer_fuel_class",
    "net_metering_misc_eia861": "core_eia861__yearly_net_metering_misc",
    "non_net_metering_customer_fuel_class_eia861": "core_eia861__yearly_non_net_metering_customer_fuel_class",
    "non_net_metering_misc_eia861": "core_eia861__yearly_non_net_metering_misc",
    "nox_compliance_strategies_eia": "core_eia__codes_nox_compliance_strategies",
    "nox_control_status_eia": "core_eia__codes_nox_control_status",
    "nox_units_eia": "core_eia__codes_nox_units",
    "operational_data_misc_eia861": "core_eia861__yearly_operational_data_misc",
    "operational_data_revenue_eia861": "core_eia861__yearly_operational_data_revenue",
    "operational_status_eia": "core_eia__codes_operational_status",
    "other_regulatory_liabilities_ferc1": "core_ferc1__yearly_other_regulatory_liabilities_sched278",
    "ownership_eia860": "core_eia860__scd_ownership",
    "particulate_compliance_strategies_eia": "core_eia__codes_particulate_compliance_strategies",
    "particulate_units_eia": "core_eia__codes_particulate_units",
    "plant_in_service_ferc1": "core_ferc1__yearly_plant_in_service_sched204",
    "plants_eia": "core_pudl__assn_eia_pudl_plants",
    "plants_eia860": "core_eia860__scd_plants",
    "plants_entity_eia": "core_eia__entity_plants",
    "plants_ferc1": "core_pudl__assn_ferc1_pudl_plants",
    "plants_hydro_ferc1": "core_ferc1__yearly_hydroelectric_plants_sched406",
    "plants_pudl": "core_pudl__entity_plants_pudl",
    "plants_pumped_storage_ferc1": "core_ferc1__yearly_pumped_storage_plants_sched408",
    "plants_small_ferc1": "core_ferc1__yearly_small_plants_sched410",
    "plants_steam_ferc1": "core_ferc1__yearly_steam_plants_sched402",
    "political_subdivisions": "core_pudl__codes_subdivisions",
    "power_purchase_types_ferc1": "core_ferc1__codes_power_purchase_types",
    "prime_movers_eia": "core_eia__codes_prime_movers",
    "purchased_power_ferc1": "core_ferc1__yearly_purchased_power_and_exchanges_sched326",
    "regulations_eia": "core_eia__codes_regulations",
    "reliability_eia861": "core_eia861__yearly_reliability",
    "reporting_frequencies_eia": "core_eia__codes_reporting_frequencies",
    "respondent_id_ferc714": "core_ferc714__respondent_id",
    "retained_earnings_ferc1": "core_ferc1__yearly_retained_earnings_sched118",
    "sales_eia861": "core_eia861__yearly_sales",
    "sector_consolidated_eia": "core_eia__codes_sector_consolidated",
    "service_territory_eia861": "core_eia861__yearly_service_territory",
    "so2_compliance_strategies_eia": "core_eia__codes_so2_compliance_strategies",
    "so2_units_eia": "core_eia__codes_so2_units",
    "state_censusdp1": "core_censusdp1__entity_state",
    "steam_plant_types_eia": "core_eia__codes_sector_consolidated",
    "tract_censusdp1": "core_censusdp1__entity_tract",
    "transmission_statistics_ferc1": "core_ferc1__yearly_transmission_lines_sched422",
    "utilities_eia": "core_pudl__assn_eia_pudl_utilities",
    "utilities_eia860": "core_eia860__scd_utilities",
    "utilities_entity_eia": "core_eia__entity_utilities",
    "utilities_ferc1": "core_pudl__assn_ferc1_pudl_utilities",
    "utilities_ferc1_dbf": "core_pudl__assn_ferc1_dbf_pudl_utilities",
    "utilities_ferc1_xbrl": "core_pudl__assn_ferc1_xbrl_pudl_utilities",
    "utilities_pudl": "core_pudl__entity_utilities_pudl",
    "utility_assn_eia861": "core_eia861__assn_utility",
    "utility_data_misc_eia861": "core_eia861__yearly_utility_data_misc",
    "utility_data_nerc_eia861": "core_eia861__yearly_utility_data_nerc",
    "utility_data_rto_eia861": "core_eia861__yearly_utility_data_rto",
    "utility_plant_assn": "core_pudl__assn_utilities_plants",
    "utility_plant_summary_ferc1": "core_ferc1__yearly_utility_plant_summary_sched200",
    "wet_dry_bottom_eia": "core_eia__codes_wet_dry_bottom",
    "annualized_respondents_ferc714": "_out_ferc714__annualized_respondents",
    "capacity_factor_by_generator_monthly": "_out_eia__monthly_capacity_factor_by_generator",
    "capacity_factor_by_generator_yearly": "_out_eia__yearly_capacity_factor_by_generator",
    "categorized_respondents_ferc714": "_out_ferc714__categorized_respondents",
    "clean_hourly_demand_matrix_ferc714": "_out_ferc714__hourly_demand_matrix",
    "compiled_geometry_balancing_authority_eia861": "out_eia861__compiled_geometry_balancing_authorities",
    "compiled_geometry_utility_eia861": "out_eia861__compiled_geometry_utilities",
    "denorm_boiler_fuel_eia923": "out_eia923__boiler_fuel",
    "denorm_boiler_fuel_monthly_eia923": "out_eia923__monthly_boiler_fuel",
    "denorm_boiler_fuel_yearly_eia923": "out_eia923__yearly_boiler_fuel",
    "denorm_boilers_eia": "out_eia__yearly_boilers",
    "denorm_emissions_control_equipment_eia860": "out_eia860__yearly_emissions_control_equipment",
    "denorm_fuel_receipts_costs_eia923": "out_eia923__fuel_receipts_costs",
    "denorm_fuel_receipts_costs_monthly_eia923": "out_eia923__monthly_fuel_receipts_costs",
    "denorm_fuel_receipts_costs_yearly_eia923": "out_eia923__yearly_fuel_receipts_costs",
    "denorm_generation_eia923": "out_eia923__generation",
    "denorm_generation_fuel_combined_eia923": "out_eia923__generation_fuel_combined",
    "denorm_generation_fuel_combined_monthly_eia923": "out_eia923__monthly_generation_fuel_combined",
    "denorm_generation_fuel_combined_yearly_eia923": "out_eia923__yearly_generation_fuel_combined",
    "denorm_generation_monthly_eia923": "out_eia923__monthly_generation",
    "denorm_generation_yearly_eia923": "out_eia923__yearly_generation",
    "denorm_generators_eia": "_out_eia__yearly_generators",
    "denorm_ownership_eia860": "out_eia860__yearly_ownership",
    "denorm_plants_eia": "out_eia__yearly_plants",
    "denorm_plants_utilities_eia": "_out_eia__plants_utilities",
    "denorm_utilities_eia": "out_eia__yearly_utilities",
    "denorm_balance_sheet_assets_ferc1": "out_ferc1__yearly_balance_sheet_assets_sched110",
    "denorm_balance_sheet_liabilities_ferc1": "out_ferc1__yearly_balance_sheet_liabilities_sched110",
    "denorm_cash_flow_ferc1": "out_ferc1__yearly_cash_flows_sched120",
    "denorm_depreciation_amortization_summary_ferc1": "out_ferc1__yearly_depreciation_summary_sched336",
    "denorm_electric_energy_dispositions_ferc1": "out_ferc1__yearly_energy_dispositions_sched401",
    "denorm_electric_energy_sources_ferc1": "out_ferc1__yearly_energy_sources_sched401",
    "denorm_electric_operating_expenses_ferc1": "out_ferc1__yearly_operating_expenses_sched320",
    "denorm_electric_operating_revenues_ferc1": "out_ferc1__yearly_operating_revenues_sched300",
    "denorm_electric_plant_depreciation_changes_ferc1": "out_ferc1__yearly_depreciation_changes_sched219",
    "denorm_electric_plant_depreciation_functional_ferc1": "out_ferc1__yearly_depreciation_by_function_sched219",
    "denorm_electricity_sales_by_rate_schedule_ferc1": "out_ferc1__yearly_sales_by_rate_schedules_sched304",
    "denorm_fuel_by_plant_ferc1": "out_ferc1__yearly_steam_plants_fuel_by_plant_sched402",
    "denorm_fuel_ferc1": "out_ferc1__yearly_steam_plants_fuel_sched402",
    "denorm_income_statement_ferc1": "out_ferc1__yearly_income_statements_sched114",
    "denorm_other_regulatory_liabilities_ferc1": "out_ferc1__yearly_other_regulatory_liabilities_sched278",
    "denorm_plant_in_service_ferc1": "out_ferc1__yearly_plant_in_service_sched204",
    "denorm_plants_all_ferc1": "out_ferc1__yearly_all_plants",
    "denorm_plants_hydro_ferc1": "_out_ferc1__yearly_hydroelectric_plants_sched406",
    "denorm_plants_pumped_storage_ferc1": "_out_ferc1__yearly_pumped_storage_plants_sched408",
    "denorm_plants_small_ferc1": "_out_ferc1__yearly_small_plants_sched410",
    "denorm_plants_steam_ferc1": "_out_ferc1__yearly_steam_plants_sched402",
    "denorm_plants_utilities_ferc1": "_out_ferc1__yearly_plants_utilities",
    "denorm_purchased_power_ferc1": "out_ferc1__yearly_purchased_power_and_exchanges_sched326",
    "denorm_retained_earnings_ferc1": "out_ferc1__yearly_retained_earnings_sched118",
    "denorm_transmission_statistics_ferc1": "out_ferc1__yearly_transmission_lines_sched422",
    "denorm_utility_plant_summary_ferc1": "out_ferc1__yearly_utility_plant_summary_sched200",
    "fipsified_respondents_ferc714": "out_ferc714__fipsified_respondents",
    "fuel_cost_by_generator_monthly": "_out_eia__monthly_fuel_cost_by_generator",
    "fuel_cost_by_generator_yearly": "_out_eia__yearly_fuel_cost_by_generator",
    "generation_fuel_by_generator_energy_source_monthly_eia923": "out_eia923__monthly_generation_fuel_by_generator_energy_source",
    "generation_fuel_by_generator_energy_source_owner_yearly_eia923": "out_eia923__yearly_generation_fuel_by_generator_energy_source_owner",
    "generation_fuel_by_generator_energy_source_yearly_eia923": "out_eia923__yearly_generation_fuel_by_generator_energy_source",
    "generation_fuel_by_generator_monthly_eia923": "out_eia923__monthly_generation_fuel_by_generator",
    "generation_fuel_by_generator_yearly_eia923": "out_eia923__yearly_generation_fuel_by_generator",
    "georeferenced_counties_ferc714": "_out_ferc714__georeferenced_counties",
    "georeferenced_respondents_ferc714": "_out_ferc714__georeferenced_respondents",
    "heat_rate_by_generator_monthly": "_out_eia__monthly_heat_rate_by_generator",
    "heat_rate_by_generator_yearly": "_out_eia__yearly_heat_rate_by_generator",
    "heat_rate_by_unit_monthly": "_out_eia__monthly_heat_rate_by_unit",
    "heat_rate_by_unit_yearly": "_out_eia__yearly_heat_rate_by_unit",
    "imputed_hourly_demand_ferc714": "_out_ferc714__hourly_imputed_demand",
    "mcoe_generators_monthly": "out_eia__monthly_generators",
    "mcoe_generators_yearly": "out_eia__yearly_generators",
    "mcoe_monthly": "_out_eia__monthly_derived_generator_attributes",
    "mcoe_yearly": "_out_eia__yearly_derived_generator_attributes",
    "predicted_state_hourly_demand": "out_ferc714__hourly_predicted_state_demand",
    "raw_hourly_demand_matrix_ferc714": "_out_ferc714__hourly_pivoted_demand_matrix",
    "summarized_demand_ferc714": "out_ferc714__summarized_demand",
    "utc_offset_ferc714": "_out_ferc714__utc_offset",
    "accumulated_provision_for_depreciation_of_electric_utility_plant_changes_section_a_219_duration": "raw_ferc1_xbrl__accumulated_provision_for_depreciation_of_electric_utility_plant_changes_section_a_219_duration",
    "accumulated_provision_for_depreciation_of_electric_utility_plant_changes_section_a_219_instant": "raw_ferc1_xbrl__accumulated_provision_for_depreciation_of_electric_utility_plant_changes_section_a_219_instant",
    "accumulated_provision_for_depreciation_of_electric_utility_plant_functional_classification_section_b_219_duration": "raw_ferc1_xbrl__accumulated_provision_for_depreciation_of_electric_utility_plant_functional_classification_section_b_219_duration",
    "accumulated_provision_for_depreciation_of_electric_utility_plant_functional_classification_section_b_219_instant": "raw_ferc1_xbrl__accumulated_provision_for_depreciation_of_electric_utility_plant_functional_classification_section_b_219_instant",
    "comparative_balance_sheet_assets_and_other_debits_110_duration": "raw_ferc1_xbrl__comparative_balance_sheet_assets_and_other_debits_110_duration",
    "comparative_balance_sheet_assets_and_other_debits_110_instant": "raw_ferc1_xbrl__comparative_balance_sheet_assets_and_other_debits_110_instant",
    "comparative_balance_sheet_liabilities_and_other_credits_110_duration": "raw_ferc1_xbrl__comparative_balance_sheet_liabilities_and_other_credits_110_duration",
    "comparative_balance_sheet_liabilities_and_other_credits_110_instant": "raw_ferc1_xbrl__comparative_balance_sheet_liabilities_and_other_credits_110_instant",
    "electric_energy_account_401a_duration": "raw_ferc1_xbrl__electric_energy_account_401a_duration",
    "electric_energy_account_401a_instant": "raw_ferc1_xbrl__electric_energy_account_401a_instant",
    "electric_operating_revenues_300_duration": "raw_ferc1_xbrl__electric_operating_revenues_300_duration",
    "electric_operating_revenues_300_instant": "raw_ferc1_xbrl__electric_operating_revenues_300_instant",
    "electric_operations_and_maintenance_expenses_320_duration": "raw_ferc1_xbrl__electric_operations_and_maintenance_expenses_320_duration",
    "electric_operations_and_maintenance_expenses_320_instant": "raw_ferc1_xbrl__electric_operations_and_maintenance_expenses_320_instant",
    "electric_plant_in_service_204_duration": "raw_ferc1_xbrl__electric_plant_in_service_204_duration",
    "electric_plant_in_service_204_instant": "raw_ferc1_xbrl__electric_plant_in_service_204_instant",
    "f1_accumdepr_prvsn": "raw_ferc1_dbf__f1_accumdepr_prvsn",
    "f1_bal_sheet_cr": "raw_ferc1_dbf__f1_bal_sheet_cr",
    "f1_cash_flow": "raw_ferc1_dbf__f1_cash_flow",
    "f1_comp_balance_db": "raw_ferc1_dbf__f1_comp_balance_db",
    "f1_dacs_epda": "raw_ferc1_dbf__f1_dacs_epda",
    "f1_elc_op_mnt_expn": "raw_ferc1_dbf__f1_elc_op_mnt_expn",
    "f1_elctrc_erg_acct": "raw_ferc1_dbf__f1_elctrc_erg_acct",
    "f1_elctrc_oper_rev": "raw_ferc1_dbf__f1_elctrc_oper_rev",
    "f1_fuel": "raw_ferc1_dbf__f1_fuel",
    "f1_gnrt_plant": "raw_ferc1_dbf__f1_gnrt_plant",
    "f1_hydro": "raw_ferc1_dbf__f1_hydro",
    "f1_incm_stmnt_2": "raw_ferc1_dbf__f1_incm_stmnt_2",
    "f1_income_stmnt": "raw_ferc1_dbf__f1_income_stmnt",
    "f1_othr_reg_liab": "raw_ferc1_dbf__f1_othr_reg_liab",
    "f1_plant_in_srvce": "raw_ferc1_dbf__f1_plant_in_srvce",
    "f1_pumped_storage": "raw_ferc1_dbf__f1_pumped_storage",
    "f1_purchased_pwr": "raw_ferc1_dbf__f1_purchased_pwr",
    "f1_retained_erng": "raw_ferc1_dbf__f1_retained_erng",
    "f1_sales_by_sched": "raw_ferc1_dbf__f1_sales_by_sched",
    "f1_steam": "raw_ferc1_dbf__f1_steam",
    "f1_utltyplnt_smmry": "raw_ferc1_dbf__f1_utltyplnt_smmry",
    "f1_xmssn_line": "raw_ferc1_dbf__f1_xmssn_line",
    "generating_plant_statistics_410_duration": "raw_ferc1_xbrl__generating_plant_statistics_410_duration",
    "generating_plant_statistics_410_instant": "raw_ferc1_xbrl__generating_plant_statistics_410_instant",
    "hydroelectric_generating_plant_statistics_large_plants_406_duration": "raw_ferc1_xbrl__hydroelectric_generating_plant_statistics_large_plants_406_duration",
    "hydroelectric_generating_plant_statistics_large_plants_406_instant": "raw_ferc1_xbrl__hydroelectric_generating_plant_statistics_large_plants_406_instant",
    "other_regulatory_liabilities_account_254_278_duration": "raw_ferc1_xbrl__other_regulatory_liabilities_account_254_278_duration",
    "other_regulatory_liabilities_account_254_278_instant": "raw_ferc1_xbrl__other_regulatory_liabilities_account_254_278_instant",
    "pumped_storage_generating_plant_statistics_large_plants_408_duration": "raw_ferc1_xbrl__pumped_storage_generating_plant_statistics_large_plants_408_duration",
    "pumped_storage_generating_plant_statistics_large_plants_408_instant": "raw_ferc1_xbrl__pumped_storage_generating_plant_statistics_large_plants_408_instant",
    "purchased_power_326_duration": "raw_ferc1_xbrl__purchased_power_326_duration",
    "purchased_power_326_instant": "raw_ferc1_xbrl__purchased_power_326_instant",
    "raw_adjacency_ba_ferc714": "raw_ferc714__adjacency_ba",
    "raw_advanced_metering_infrastructure_eia861": "raw_eia861__advanced_metering_infrastructure",
    "raw_balancing_authority_eia861": "raw_eia861__balancing_authority",
    "raw_boiler_cooling_eia860": "raw_eia860__boiler_cooling",
    "raw_boiler_fuel_eia923": "raw_eia923__boiler_fuel",
    "raw_boiler_generator_assn_eia860": "raw_eia860__boiler_generator_assn",
    "raw_boiler_info_eia860": "raw_eia860__boiler_info",
    "raw_boiler_mercury_eia860": "raw_eia860__boiler_mercury",
    "raw_boiler_nox_eia860": "raw_eia860__boiler_nox",
    "raw_boiler_particulate_eia860": "raw_eia860__boiler_particulate",
    "raw_boiler_so2_eia860": "raw_eia860__boiler_so2",
    "raw_boiler_stack_flue_eia860": "raw_eia860__boiler_stack_flue",
    "raw_cooling_equipment_eia860": "raw_eia860__cooling_equipment",
    "raw_delivery_companies_eia861": "raw_eia861__delivery_companies",
    "raw_demand_forecast_pa_ferc714": "raw_ferc714__demand_forecast_pa",
    "raw_demand_hourly_pa_ferc714": "raw_ferc714__demand_hourly_pa",
    "raw_demand_monthly_ba_ferc714": "raw_ferc714__demand_monthly_ba",
    "raw_demand_response_eia861": "raw_eia861__demand_response",
    "raw_demand_side_management_eia861": "raw_eia861__demand_side_management",
    "raw_description_pa_ferc714": "raw_ferc714__description_pa",
    "raw_distributed_generation_eia861": "raw_eia861__distributed_generation",
    "raw_distribution_systems_eia861": "raw_eia861__distribution_systems",
    "raw_dynamic_pricing_eia861": "raw_eia861__dynamic_pricing",
    "raw_emission_control_strategies_eia860": "raw_eia860__emission_control_strategies",
    "raw_emissions_control_equipment_eia860": "raw_eia860__emissions_control_equipment",
    "raw_energy_efficiency_eia861": "raw_eia861__energy_efficiency",
    "raw_epacamd_eia": "raw_pudl__assn_eia_epacamd",
    "raw_fgd_equipment_eia860": "raw_eia860__fgd_equipment",
    "raw_fgp_equipment_eia860": "raw_eia860__fgp_equipment",
    "raw_frame_eia861": "raw_eia861__frame",
    "raw_fuel_receipts_costs_eia923": "raw_eia923__fuel_receipts_costs",
    "raw_gen_plants_ba_ferc714": "raw_ferc714__gen_plants_ba",
    "raw_generation_fuel_eia923": "raw_eia923__generation_fuel",
    "raw_generator_eia860": "raw_eia860__generator",
    "raw_generator_eia923": "raw_eia923__generator",
    "raw_generator_existing_eia860": "raw_eia860__generator_existing",
    "raw_generator_proposed_eia860": "raw_eia860__generator_proposed",
    "raw_generator_retired_eia860": "raw_eia860__generator_retired",
    "raw_green_pricing_eia861": "raw_eia861__green_pricing",
    "raw_id_certification_ferc714": "raw_ferc714__id_certification",
    "raw_interchange_ba_ferc714": "raw_ferc714__interchange_ba",
    "raw_lambda_description_ferc714": "raw_ferc714__lambda_description",
    "raw_lambda_hourly_ba_ferc714": "raw_ferc714__lambda_hourly_ba",
    "raw_mergers_eia861": "raw_eia861__mergers",
    "raw_multifuel_existing_eia860": "raw_eia860__multifuel_existing",
    "raw_multifuel_retired_eia860": "raw_eia860__multifuel_retired",
    "raw_net_energy_load_ba_ferc714": "raw_ferc714__net_energy_load_ba",
    "raw_net_metering_eia861": "raw_eia861__net_metering",
    "raw_non_net_metering_eia861": "raw_eia861__non_net_metering",
    "raw_operational_data_eia861": "raw_eia861__operational_data",
    "raw_ownership_eia860": "raw_eia860__ownership",
    "raw_plant_eia860": "raw_eia860__plant",
    "raw_reliability_eia861": "raw_eia861__reliability",
    "raw_respondent_id_ferc714": "raw_ferc714__respondent_id",
    "raw_sales_eia861": "raw_eia861__sales",
    "raw_service_territory_eia861": "raw_eia861__service_territory",
    "raw_short_form_eia861": "raw_eia861__short_form",
    "raw_stack_flue_equipment_eia860": "raw_eia860__stack_flue_equipment",
    "raw_stocks_eia923": "raw_eia923__stocks",
    "raw_utility_data_eia861": "raw_eia861__utility_data",
    "raw_utility_eia860": "raw_eia860__utility",
    "raw_xbrl_metadata_json": "raw_xbrl_metadata_json",
    "retained_earnings_118_duration": "raw_ferc1_xbrl__retained_earnings_118_duration",
    "retained_earnings_118_instant": "raw_ferc1_xbrl__retained_earnings_118_instant",
    "retained_earnings_appropriations_118_duration": "raw_ferc1_xbrl__retained_earnings_appropriations_118_duration",
    "retained_earnings_appropriations_118_instant": "raw_ferc1_xbrl__retained_earnings_appropriations_118_instant",
    "sales_of_electricity_by_rate_schedules_account_440_residential_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_440_residential_304_duration",
    "sales_of_electricity_by_rate_schedules_account_440_residential_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_440_residential_304_instant",
    "sales_of_electricity_by_rate_schedules_account_442_commercial_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_442_commercial_304_duration",
    "sales_of_electricity_by_rate_schedules_account_442_commercial_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_442_commercial_304_instant",
    "sales_of_electricity_by_rate_schedules_account_442_industrial_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_442_industrial_304_duration",
    "sales_of_electricity_by_rate_schedules_account_442_industrial_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_442_industrial_304_instant",
    "sales_of_electricity_by_rate_schedules_account_444_public_street_and_highway_lighting_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_444_public_street_and_highway_lighting_304_duration",
    "sales_of_electricity_by_rate_schedules_account_444_public_street_and_highway_lighting_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_444_public_street_and_highway_lighting_304_instant",
    "sales_of_electricity_by_rate_schedules_account_445_other_sales_to_public_authorities_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_445_other_sales_to_public_authorities_304_duration",
    "sales_of_electricity_by_rate_schedules_account_445_other_sales_to_public_authorities_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_445_other_sales_to_public_authorities_304_instant",
    "sales_of_electricity_by_rate_schedules_account_446_sales_to_railroads_and_railways_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_446_sales_to_railroads_and_railways_304_duration",
    "sales_of_electricity_by_rate_schedules_account_446_sales_to_railroads_and_railways_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_446_sales_to_railroads_and_railways_304_instant",
    "sales_of_electricity_by_rate_schedules_account_448_interdepartmental_sales_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_448_interdepartmental_sales_304_duration",
    "sales_of_electricity_by_rate_schedules_account_448_interdepartmental_sales_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_448_interdepartmental_sales_304_instant",
    "sales_of_electricity_by_rate_schedules_account_4491_provision_for_rate_refunds_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_4491_provision_for_rate_refunds_304_duration",
    "sales_of_electricity_by_rate_schedules_account_4491_provision_for_rate_refunds_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_4491_provision_for_rate_refunds_304_instant",
    "sales_of_electricity_by_rate_schedules_account_totals_304_duration": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_totals_304_duration",
    "sales_of_electricity_by_rate_schedules_account_totals_304_instant": "raw_ferc1_xbrl__sales_of_electricity_by_rate_schedules_account_totals_304_instant",
    "statement_of_cash_flows_120_duration": "raw_ferc1_xbrl__statement_of_cash_flows_120_duration",
    "statement_of_cash_flows_120_instant": "raw_ferc1_xbrl__statement_of_cash_flows_120_instant",
    "statement_of_income_114_duration": "raw_ferc1_xbrl__statement_of_income_114_duration",
    "statement_of_income_114_instant": "raw_ferc1_xbrl__statement_of_income_114_instant",
    "steam_electric_generating_plant_statistics_large_plants_402_duration": "raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_402_duration",
    "steam_electric_generating_plant_statistics_large_plants_402_instant": "raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_402_instant",
    "steam_electric_generating_plant_statistics_large_plants_fuel_statistics_402_duration": "raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_fuel_statistics_402_duration",
    "steam_electric_generating_plant_statistics_large_plants_fuel_statistics_402_instant": "raw_ferc1_xbrl__steam_electric_generating_plant_statistics_large_plants_fuel_statistics_402_instant",
    "summary_of_depreciation_and_amortization_charges_section_a_336_duration": "raw_ferc1_xbrl__summary_of_depreciation_and_amortization_charges_section_a_336_duration",
    "summary_of_depreciation_and_amortization_charges_section_a_336_instant": "raw_ferc1_xbrl__summary_of_depreciation_and_amortization_charges_section_a_336_instant",
    "summary_of_utility_plant_and_accumulated_provisions_for_depreciation_amortization_and_depletion_200_duration": "raw_ferc1_xbrl__summary_of_utility_plant_and_accumulated_provisions_for_depreciation_amortization_and_depletion_200_duration",
    "summary_of_utility_plant_and_accumulated_provisions_for_depreciation_amortization_and_depletion_200_instant": "raw_ferc1_xbrl__summary_of_utility_plant_and_accumulated_provisions_for_depreciation_amortization_and_depletion_200_instant",
    "transmission_line_statistics_422_duration": "raw_ferc1_xbrl__transmission_line_statistics_422_duration",
    "transmission_line_statistics_422_instant": "raw_ferc1_xbrl__transmission_line_statistics_422_instant",
    "state_average_fuel_costs_eia": "out_eia__state_average_fuel_costs",
    "mega_generators_eia": "out_eia__yearly_generators_by_ownership",
    "plant_parts_eia": "out_eia__yearly_plant_parts",
    "out__yearly_plants_all_ferc1_plant_parts_eia": "out_pudl__yearly_assn_eia_ferc1_plant_parts",
}


def renamer(args=None, pattern=None, *, dry=False, yes=False):
    if args is not None:
        pattern = args.pattern
        dry = args.dry
        yes = args.yes
    paths = [p for p in Path.cwd().glob(pattern) if p.is_file()]
    path_str = "\n  ".join(str(p) for p in paths)

    print(f"The following files will have their tables renamed:\n  {path_str}")
    if dry:
        return

    if not yes:
        prompt = input("Are you sure you want to rename tables in these files? [y/n]")
        if prompt.casefold() != "y":
            return

    for path in paths:
        # Read the content of the original text file
        with open(path) as file:
            file_content = file.read()

        # Perform the replacements using the dictionary
        modified_content = file_content

        # Iterate through the dictionary to replace old names with new ones
        for old_name, new_name in PUDL_TABLE_MAP.items():
            modified_content = modified_content.replace(
                f'"{old_name}"', f'"{new_name}"'
            ).replace(f"'{old_name}'", f"'{new_name}'")

        if modified_content != file_content:
            new_path = path.parent / (path.stem + "_new" + path.suffix)
            # Write the modified content to the output file
            with open(new_path, "w") as file:
                file.write(modified_content)

            # Replace the old file with the new one
            new_path.rename(path)


def main():
    from argparse import ArgumentParser

    parser = ArgumentParser(
        description="Rename PUDL tables in files that match a provided pattern."
    )
    parser.add_argument(
        type=str,
        help="Pattern for globbing files.",
        dest="pattern",
    )
    parser.add_argument(
        "-d, --dry-run",
        action="store_true",
        help="Only display what would have been done.",
        dest="dry",
    )
    parser.add_argument(
        "-y, --yes",
        action="store_true",
        help="Sets any confirmation values to 'yes' automatically. Users will not be "
        " asked to confirm before tables are renamed.",
        dest="yes",
    )
    return renamer(parser.parse_args())

    # paths = [p for p in Path.cwd().glob(args.pattern) if p.is_file()]
    # path_str = "\n  ".join(str(p) for p in paths)
    #
    # print(f"The following files will have their tables renamed:\n  {path_str}")
    # if args.dry_run:
    #     return
    #
    # if not args.yes:
    #     prompt = input("Are you sure you want to rename tables in these files? [y/n]")
    #     if prompt.casefold() != "y":
    #         return
    #
    # for path in paths:
    #     # Read the content of the original text file
    #     with open(path) as file:
    #         file_content = file.read()
    #
    #     # Perform the replacements using the dictionary
    #     modified_content = file_content
    #
    #     # Iterate through the dictionary to replace old names with new ones
    #     for old_name, new_name in PUDL_TABLE_MAP.items():
    #         modified_content = modified_content.replace(
    #             f'"{old_name}"', f'"{new_name}"'
    #         ).replace(f"'{old_name}'", f"'{new_name}'")
    #
    #     if modified_content != file_content:
    #         new_path = path.parent / (path.stem + "_new" + path.suffix)
    #         # Write the modified content to the output file
    #         with open(new_path, "w") as file:
    #             file.write(modified_content)
    #
    #         # Replace the old file with the new one
    #         new_path.rename(path)


if __name__ == "__main__":
    main()
