//! Input data manager: projects RunSpec selections into the
//! default-DB to execution-DB merge plan.

pub mod input_data_manager;

pub use input_data_manager::{
    default_tables, InputDataManager, MergePlan, MergeTableSpec, RunSpecFilters, TableMergePlan,
    WhereClause, WhereClauseBuilder,
};
