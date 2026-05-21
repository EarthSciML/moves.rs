//! AVFT internal control strategy — ports
//! `gov.epa.otaq.moves.master.implementation.ghg.internalcontrolstrategies.avft.AVFTStrategy`.
//!
//! # Role
//!
//! The AVFT control strategy takes a completed, gap-filled, projected AVFT
//! table and replaces the `AVFT` entry in the execution database so that
//! downstream emission calculators see the user-specified fleet-composition
//! fractions instead of the model defaults.
//!
//! It runs entirely in [`pre_run`](AvftControlStrategy::pre_run) — a
//! single call before the master loop begins. No per-iteration subscription
//! is needed because AVFT fractions are global (not location- or
//! time-varying at the granularity MOVES iterates).
//!
//! # Constructors
//!
//! * [`AvftControlStrategy::from_tool_inputs`] — builds the completed table
//!   from raw user AVFT + defaults + spec by running the gap-fill and
//!   projection logic (crate `moves-avft::tool`, Task 86) at construction
//!   time. Use this when the completed table is not cached on disk.
//! * [`AvftControlStrategy::from_completed`] — accepts an already-built
//!   table (e.g. loaded from a Parquet file via
//!   [`crate::parquet_io::read_parquet`]). Use this when the AVFT CLI has
//!   been run separately and its Parquet output is available.
//!
//! # Data-plane status (Task 50)
//!
//! The actual write of the completed table into the execution database is
//! deferred until `moves-framework`'s `ExecutionTables` gains a mutable
//! write API (Task 50 / `DataFrameStore`). The `modified_tables` declaration
//! already signals the engine which table will be replaced, so the hook-up
//! is a single `TODO` line once the data plane lands.

use moves_framework::{CalculatorContext, InternalControlStrategy};

use crate::error::Result;
use crate::model::AvftTable;
use crate::spec::ToolSpec;
use crate::tool::{run as run_tool, ToolInputs};

/// AVFT (Alternative Vehicle Fuel Technology) internal control strategy.
///
/// See the [module docs](self) for the full description.
#[derive(Debug)]
pub struct AvftControlStrategy {
    completed: AvftTable,
}

impl AvftControlStrategy {
    /// Build by running the AVFT Tool on raw user inputs.
    ///
    /// Runs the gap-fill and projection steps defined in `spec` over
    /// `input` (the user-authored AVFT table) and `default` (the
    /// model-default AVFT). The resulting completed table is stored and
    /// applied in [`pre_run`](Self::pre_run).
    ///
    /// `known_fractions` is consulted only when at least one source type
    /// uses [`crate::spec::ProjectionMethod::KnownFractions`]; pass
    /// `&AvftTable::new()` if no source type needs it.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::ToolSpec`] if `spec` fails validation, or
    /// [`crate::error::Error::ToolFailure`] if the tool cannot produce a valid output
    /// for any enabled source type.
    pub fn from_tool_inputs(
        spec: &ToolSpec,
        input: &AvftTable,
        default: &AvftTable,
        known_fractions: &AvftTable,
    ) -> Result<Self> {
        let inputs = ToolInputs {
            spec,
            input,
            default,
            known_fractions,
        };
        let report = run_tool(&inputs)?;
        Ok(Self {
            completed: report.output,
        })
    }

    /// Build from an already-completed AVFT table.
    ///
    /// Useful when the AVFT Tool has been run offline and its Parquet
    /// output loaded via [`crate::parquet_io::read_parquet`], or in tests
    /// that construct the table directly.
    pub fn from_completed(table: AvftTable) -> Self {
        Self { completed: table }
    }

    /// The completed AVFT table that will be applied in
    /// [`pre_run`](Self::pre_run).
    pub fn completed_table(&self) -> &AvftTable {
        &self.completed
    }
}

impl InternalControlStrategy for AvftControlStrategy {
    fn name(&self) -> &'static str {
        "AvftControlStrategy"
    }

    fn modified_tables(&self) -> &[&'static str] {
        &["AVFT"]
    }

    fn pre_run(
        &self,
        _ctx: &CalculatorContext,
    ) -> std::result::Result<(), moves_framework::Error> {
        // TODO(Task 50 / DataFrameStore): write `self.completed` into the
        // execution database as the `AVFT` table once `ExecutionTables`
        // exposes a mutable write API. The `modified_tables` declaration
        // above already signals the engine to invalidate and reload `AVFT`
        // after this hook returns.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AvftRecord;
    use crate::spec::{GapFillingMethod, MethodEntry, ProjectionMethod, ToolSpec};

    fn small_table(records: &[(i32, i32, i32, i32, f64)]) -> AvftTable {
        records
            .iter()
            .map(|&(st, my, fuel, eng, frac)| AvftRecord::new(st, my, fuel, eng, frac))
            .collect()
    }

    fn minimal_spec(source_type_id: i32) -> ToolSpec {
        ToolSpec {
            last_complete_model_year: 2020,
            analysis_year: 2020,
            methods: vec![MethodEntry {
                source_type_id,
                enabled: true,
                gap_filling: GapFillingMethod::Automatic,
                projection: ProjectionMethod::Constant,
            }],
        }
    }

    #[test]
    fn from_completed_preserves_table() {
        let t = small_table(&[(11, 2020, 1, 1, 0.7), (11, 2020, 2, 1, 0.3)]);
        let strategy = AvftControlStrategy::from_completed(t.clone());
        assert_eq!(
            strategy.completed_table().to_vec(),
            t.to_vec(),
            "from_completed must preserve the input table unchanged"
        );
    }

    #[test]
    fn name_is_stable() {
        let strategy = AvftControlStrategy::from_completed(AvftTable::new());
        assert_eq!(strategy.name(), "AvftControlStrategy");
    }

    #[test]
    fn modified_tables_contains_avft() {
        let strategy = AvftControlStrategy::from_completed(AvftTable::new());
        assert!(
            strategy.modified_tables().contains(&"AVFT"),
            "strategy must declare AVFT in modified_tables"
        );
    }

    #[test]
    fn pre_run_succeeds_with_empty_context() {
        let strategy = AvftControlStrategy::from_completed(AvftTable::new());
        let ctx = CalculatorContext::new();
        strategy.pre_run(&ctx).expect("pre_run must not fail");
    }

    #[test]
    fn from_tool_inputs_produces_complete_table() {
        // Single source type, single model year, defaults fully cover the gap.
        let default_t = small_table(&[
            (11, 2020, 1, 1, 0.8),
            (11, 2020, 2, 1, 0.2),
        ]);
        let user_t = small_table(&[]);
        let known = AvftTable::new();
        let spec = minimal_spec(11);

        let strategy = AvftControlStrategy::from_tool_inputs(&spec, &user_t, &default_t, &known)
            .expect("tool must succeed with valid inputs");

        let rows = strategy.completed_table().to_vec();
        assert!(!rows.is_empty(), "completed table must have rows for source type 11");
        assert_eq!(rows[0].source_type_id, 11);
    }

    #[test]
    fn strategy_is_trait_object_safe() {
        let strategy: Box<dyn InternalControlStrategy> =
            Box::new(AvftControlStrategy::from_completed(AvftTable::new()));
        assert_eq!(strategy.name(), "AvftControlStrategy");
    }
}
