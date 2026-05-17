//! Output aggregation: column-shape planning, plan-driven row roll-up,
//! and the partitioned-Parquet output writer.

pub mod output_aggregate;
pub mod output_processor;
pub mod plan;

pub use output_aggregate::{
    aggregate_activity, aggregate_emissions, TemporalScalingFactors, UnitScaling,
};
pub use output_processor::{OutputProcessor, NULL_PARTITION, PARQUET_CREATED_BY};
pub use plan::{
    activity_aggregation, base_rate_aggregation, emission_aggregation, AggregationColumn,
    AggregationInputs, AggregationPlan, AggregationTable, TemporalScaling,
};
