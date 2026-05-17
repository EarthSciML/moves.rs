//! Execution-time view of a RunSpec, the execution-database schema the
//! calculators read, and the location iterator that expands geographic
//! selections into the ordered set the master loop walks.

pub mod execution_db;
pub mod execution_location_producer;
pub mod execution_runspec;

pub use execution_db::{
    ExecutionDatabaseSchema, ExecutionLocation, ExecutionTableSpec, ExecutionTables, ExecutionTime,
    IterationPosition, ScratchNamespace, TableSource,
};
pub use execution_location_producer::{
    CountyRow, ExecutionLocationProducer, GeographyTables, LinkRow, RoadTypeFilter,
    NONROAD_ROAD_TYPE_ID,
};
pub use execution_runspec::{ExecutionRunSpec, ModelCombination};
