//! Execution-time view of a RunSpec and the execution-database schema
//! the calculators read.

pub mod execution_db;
pub mod execution_runspec;

pub use execution_db::{
    ExecutionDatabaseSchema, ExecutionLocation, ExecutionTableSpec, ExecutionTables, ExecutionTime,
    IterationPosition, ScratchNamespace, TableSource,
};
pub use execution_runspec::{ExecutionRunSpec, ModelCombination};
