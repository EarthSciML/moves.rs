//! Execution-time view of a RunSpec, the execution-database schema the
//! calculators read, the location iterator that expands geographic
//! selections into the ordered set the master loop walks, and the
//! [`MOVESEngine`] orchestration entry point with its bounded-concurrency
//! [`executor`].

pub mod engine;
pub mod execution_db;
pub mod execution_location_producer;
pub mod execution_runspec;
pub mod executor;

pub use engine::{EngineConfig, EngineOutcome, MOVESEngine};
pub use execution_db::{
    ExecutionDatabaseSchema, ExecutionLocation, ExecutionTableSpec, ExecutionTables, ExecutionTime,
    IterationPosition, ScratchNamespace, TableSource,
};
pub use execution_location_producer::{
    CountyRow, ExecutionLocationProducer, GeographyTables, LinkRow, RoadTypeFilter,
    NONROAD_ROAD_TYPE_ID,
};
pub use execution_runspec::{ExecutionRunSpec, ModelCombination};
pub use executor::{chunk_chains, BoundedExecutor, Chunk, Semaphore};
