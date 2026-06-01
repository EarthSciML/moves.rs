//! NonRoadRetrofit internal control strategy — wires NONROAD retrofit records
//! (ported from `clcrtrft.f` in) into the unified control-strategy
//! framework.
//!
//! # Role
//!
//! NONROAD's retrofit calculation reduces emissions for engine populations
//! that have been equipped with after-market emission-control devices. A
//! retrofit record specifies which engines are affected (by SCC, tech type,
//! HP band, and model-year range), the retrofit year window, which pollutant
//! is reduced, and how effective the device is.
//!
//! This strategy holds the set of [`RetrofitRecord`]s parsed from an `.RTR`
//! input file and makes them available to the framework. The per-SCC
//! reduction computation is performed by
//! [`moves_nonroad::emissions::retrofit::calculate_retrofit_reduction`] during
//! the nonroad geography loops; this adapter registers the strategy with the
//! framework so it participates in the unified lifecycle.
//!
//! # Lifecycle
//!
//! The strategy runs entirely in [`pre_run`](NonRoadRetrofitStrategy::pre_run).
//! Per-iteration subscriptions are not needed because the retrofit records are
//! indexed by model year and retrofit year and do not vary across counties or
//! months within a single run. The framework's post-`pre_run` table
//! invalidation is signalled via `modified_tables`.
//!
//! # Data-plane status
//!
//! Writing computed reduction fractions into the execution database is deferred
//! until `moves-framework`'s `ExecutionTables` gains a mutable write API
//!The `modified_tables` declaration already
//! signals the engine which tables will be modified.

use moves_framework::{InMemoryStore, InternalControlStrategy};
use moves_nonroad::population::retrofit::RetrofitRecord;

/// NonRoadRetrofit internal control strategy.
///
/// Holds the retrofit records parsed from a NONROAD `.RTR` input file and
/// exposes them to the unified control-strategy framework. The records are
/// consumed by the nonroad emission calculator
/// ([`moves_nonroad::emissions::retrofit::calculate_retrofit_reduction`])
/// during the per-SCC geography loop.
///
/// See the [module docs](self) for a full description.
#[derive(Debug)]
pub struct NonRoadRetrofitStrategy {
    records: Vec<RetrofitRecord>,
}

impl NonRoadRetrofitStrategy {
 /// Build from a list of retrofit records already parsed from an `.RTR`
 /// input file.
    pub fn new(records: Vec<RetrofitRecord>) -> Self {
        Self { records }
    }

 /// The retrofit records this strategy will apply.
    pub fn records(&self) -> &[RetrofitRecord] {
        &self.records
    }
}

impl InternalControlStrategy for NonRoadRetrofitStrategy {
    fn name(&self) -> &'static str {
        "NonRoadRetrofitStrategy"
    }

    fn modified_tables(&self) -> &[&'static str] {
 // NONROAD does not write into the onroad `emissionRateAdjustment`
 // table; instead the per-SCC reduction is applied inline during the
 // geography loop. There is no shared execution-DB table to invalidate.
 // When (DataFrameStore) lands, this will declare the NONROAD
 // output table that receives the adjusted emission totals.
        &[]
    }

    fn pre_run(
        &self,
        _tables: &mut InMemoryStore,
    ) -> std::result::Result<(), moves_framework::Error> {
 // Retrofit records are accessed via `records()` by the nonroad
 // geography loop directly — no execution-database write needed here.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_framework::InMemoryStore;

    fn make_record(id: i32) -> RetrofitRecord {
        RetrofitRecord {
            record_index: id as usize,
            id,
            year_retrofit_start: 2020,
            year_retrofit_end: 2025,
            year_model_start: 2000,
            year_model_end: 2010,
            scc: "ALL".to_string(),
            tech_type: "ALL".to_string(),
            hp_min: 0.0,
            hp_max: 100.0,
            annual_frac_or_count: 0.5,
            effectiveness: 0.5,
            pollutant: "HC".to_string(),
            pollutant_idx: 1,
        }
    }

    #[test]
    fn name_is_stable() {
        let s = NonRoadRetrofitStrategy::new(vec![]);
        assert_eq!(s.name(), "NonRoadRetrofitStrategy");
    }

    #[test]
    fn modified_tables_is_empty() {
        let s = NonRoadRetrofitStrategy::new(vec![]);
        assert!(s.modified_tables().is_empty());
    }

    #[test]
    fn pre_run_succeeds_with_empty_records() {
        let s = NonRoadRetrofitStrategy::new(vec![]);
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
    }

    #[test]
    fn pre_run_succeeds_with_populated_records() {
        let s = NonRoadRetrofitStrategy::new(vec![make_record(1), make_record(2)]);
        let mut store = InMemoryStore::new();
        s.pre_run(&mut store).expect("pre_run must not fail");
    }

    #[test]
    fn records_accessor_returns_inserted_slice() {
        let recs = vec![make_record(1), make_record(2)];
        let s = NonRoadRetrofitStrategy::new(recs);
        assert_eq!(s.records().len(), 2);
        assert_eq!(s.records()[0].id, 1);
        assert_eq!(s.records()[1].id, 2);
    }

    #[test]
    fn no_subscriptions() {
        let s = NonRoadRetrofitStrategy::new(vec![]);
        assert!(s.subscriptions().is_empty());
    }

    #[test]
    fn strategy_is_trait_object_safe() {
        let strategy: Box<dyn InternalControlStrategy> =
            Box::new(NonRoadRetrofitStrategy::new(vec![]));
        assert_eq!(strategy.name(), "NonRoadRetrofitStrategy");
    }

    #[test]
    fn strategy_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NonRoadRetrofitStrategy>();
    }
}
