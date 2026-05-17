//! Output aggregation: column-shape planning, plan-driven row roll-up,
//! the partitioned-Parquet output writer, and the NONROAD-specific
//! post-processing summaries.

pub mod nonroad_postprocess;
pub mod output_aggregate;
pub mod output_processor;
pub mod plan;

pub use nonroad_postprocess::{
    emission_factors, inventory, mass_units_to_grams, population_by_sector_and_scc,
    EmissionFactorReport, EmissionFactorRow, InventoryReport, InventoryRow, NrSccLookup,
    PopulationRow,
};
pub use output_aggregate::{
    aggregate_activity, aggregate_emissions, TemporalScalingFactors, UnitScaling,
};
pub use output_processor::{OutputProcessor, NULL_PARTITION, PARQUET_CREATED_BY};
pub use plan::{
    activity_aggregation, base_rate_aggregation, emission_aggregation, AggregationColumn,
    AggregationInputs, AggregationPlan, AggregationTable, TemporalScaling,
};
