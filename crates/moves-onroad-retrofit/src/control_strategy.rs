//! OnRoadRetrofit internal control strategy — ports
//! `gov.epa.otaq.moves.master.implementation.ghg.internalcontrolstrategies.onroadretrofit.OnRoadRetrofit`.
//!
//! # Role
//!
//! The OnRoadRetrofit control strategy applies emission-reduction adjustments
//! to account for retrofit programs: a fraction of the fleet has been
//! equipped with emission-control devices, reducing emissions by a specified
//! effectiveness factor.
//!
//! For each `(sourceType, modelYear, pollutant, process)` combination, the
//! combined emission adjustment factor is:
//!
//! ```text
//! factor = ∏ over active programs p of (1 - p.fraction * p.effectiveness)
//! ```
//!
//! where "active" means `p.retrofit_year_id ≤ analysis_year` and the model
//! year falls within `[p.start_model_year, p.end_model_year]`.
//!
//! # Lifecycle
//!
//! The strategy runs entirely in [`pre_run`](OnRoadRetrofitStrategy::pre_run).
//! Per-iteration subscriptions are not needed because the retrofit programs
//! are indexed by model year and retrofit year — they do not vary across
//! counties or months within a single MOVES run.
//!
//! # Data-plane status (Task 50)
//!
//! The computed adjustment factors would normally be written into the
//! `emissionRateAdjustment` table in the execution database so downstream
//! emission calculators consume them. That write is deferred until
//! `moves-framework`'s `ExecutionTables` gains a mutable write API (Task 50
//! / `DataFrameStore`). The `modified_tables` declaration already signals the
//! engine which table will be modified.

use moves_framework::{InMemoryStore, InternalControlStrategy};

use crate::model::RetrofitTable;

/// OnRoadRetrofit internal control strategy.
///
/// See the [module docs](self) for the full description.
#[derive(Debug)]
pub struct OnRoadRetrofitStrategy {
    programs: RetrofitTable,
}

impl OnRoadRetrofitStrategy {
    /// Build from a [`RetrofitTable`] already loaded from user input.
    pub fn new(programs: RetrofitTable) -> Self {
        Self { programs }
    }

    /// The retrofit programs this strategy will apply.
    pub fn programs(&self) -> &RetrofitTable {
        &self.programs
    }
}

impl InternalControlStrategy for OnRoadRetrofitStrategy {
    fn name(&self) -> &'static str {
        "OnRoadRetrofitStrategy"
    }

    fn modified_tables(&self) -> &[&'static str] {
        &["emissionRateAdjustment"]
    }

    fn pre_run(
        &self,
        _tables: &mut InMemoryStore,
    ) -> std::result::Result<(), moves_framework::Error> {
        // TODO: compute combined adjustment factors from `self.programs` and
        // write them into `_tables` as `"emissionRateAdjustment"`. Requires
        // iterating `(sourceType, modelYear, pollutant, process)` combinations
        // present in the execution database and calling
        // `self.programs.combined_factor(...)` for the run's analysis year.
        // Deferred to a follow-on bead; `modified_tables` already signals the engine.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RetrofitRecord;
    use moves_framework::InMemoryStore;

    #[allow(clippy::too_many_arguments)]
    fn make_record(
        source_type: i32,
        start_my: i32,
        end_my: i32,
        retrofit_year: i32,
        pollutant: u16,
        process: u16,
        fraction: f64,
        effectiveness: f64,
    ) -> RetrofitRecord {
        RetrofitRecord::new(
            source_type,
            start_my,
            end_my,
            retrofit_year,
            pollutant,
            process,
            fraction,
            effectiveness,
        )
    }

    #[test]
    fn name_is_stable() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        assert_eq!(s.name(), "OnRoadRetrofitStrategy");
    }

    #[test]
    fn modified_tables_contains_emission_rate_adjustment() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        assert!(
            s.modified_tables().contains(&"emissionRateAdjustment"),
            "strategy must declare emissionRateAdjustment in modified_tables"
        );
    }

    #[test]
    fn pre_run_succeeds_with_empty_context() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
    }

    #[test]
    fn pre_run_succeeds_with_populated_programs() {
        let programs: RetrofitTable = [make_record(11, 2005, 2015, 2020, 98, 1, 0.5, 0.8)]
            .into_iter()
            .collect();
        let s = OnRoadRetrofitStrategy::new(programs);
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
    }

    #[test]
    fn programs_accessor_returns_inserted_table() {
        let programs: RetrofitTable = [make_record(21, 2000, 2010, 2018, 3, 1, 0.4, 0.7)]
            .into_iter()
            .collect();
        let s = OnRoadRetrofitStrategy::new(programs);
        assert_eq!(s.programs().len(), 1);
    }

    #[test]
    fn no_subscriptions() {
        let s = OnRoadRetrofitStrategy::new(RetrofitTable::new());
        assert!(s.subscriptions().is_empty());
    }

    #[test]
    fn strategy_is_trait_object_safe() {
        let strategy: Box<dyn InternalControlStrategy> =
            Box::new(OnRoadRetrofitStrategy::new(RetrofitTable::new()));
        assert_eq!(strategy.name(), "OnRoadRetrofitStrategy");
    }
}
