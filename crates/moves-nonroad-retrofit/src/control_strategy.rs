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
//! input file. In canonical NONROAD (`clcrtrft.f`) these records are always
//! applied to the per-SCC emissions; the reduction computation is ported to
//! [`moves_nonroad::emissions::retrofit::calculate_retrofit_reduction`].
//!
//! # Data-plane status (IMPORTANT)
//!
//! The live nonroad emission path reads its retrofit records from
//! `moves_nonroad`'s `ReferenceData::retrofit_records` (populated by the
//! nonroad input loader from the `.RTR` file). It does **not** read this
//! strategy object. The framework has no mutable write API that would let
//! `pre_run` feed the records held here into `ReferenceData::retrofit_records`.
//!
//! Because of that gap, this adapter cannot yet apply retrofits through the
//! unified framework. To avoid silently dropping a reduction that canonical
//! NONROAD would apply, [`pre_run`](NonRoadRetrofitStrategy::pre_run) returns
//! an error when the strategy actually holds records — registering a non-empty
//! retrofit strategy must fail loudly rather than no-op. An empty strategy is
//! a harmless no-op (nothing to apply). Once `moves-framework` gains a way to
//! write into the nonroad reference data, `pre_run` should be wired to perform
//! that write and the guard removed.

use moves_framework::{InMemoryStore, InternalControlStrategy};
use moves_nonroad::population::retrofit::RetrofitRecord;

/// NonRoadRetrofit internal control strategy.
///
/// Holds the retrofit records parsed from a NONROAD `.RTR` input file.
///
/// The live nonroad calculator does **not** read these records from this
/// object — it reads `ReferenceData::retrofit_records`. Until the framework
/// gains a write API to feed the records held here into that reference data,
/// [`pre_run`](Self::pre_run) fails for any non-empty record set so retrofits
/// are never silently dropped. See the [module docs](self) for details.
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
        // The live nonroad calculator reads its retrofit records from
        // `ReferenceData::retrofit_records`, NOT from this strategy object, and
        // the framework has no write API to bridge the two. If we hold records
        // here, succeeding silently would drop a reduction that canonical
        // NONROAD (`clcrtrft.f`) always applies. Fail loudly instead so a
        // non-empty retrofit registration cannot become an accidental no-op.
        // An empty strategy has nothing to apply and is a harmless no-op.
        if !self.records.is_empty() {
            return Err(moves_framework::Error::Nonroad(format!(
                "NonRoadRetrofitStrategy holds {} retrofit record(s) but cannot \
                 apply them through the framework: the nonroad calculator reads \
                 ReferenceData::retrofit_records, and no framework write API \
                 exists to populate it from this strategy. Load retrofit records \
                 via moves_nonroad's .RTR input loader instead of registering \
                 this strategy, or wire pre_run once a write API is available.",
                self.records.len()
            )));
        }
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
    fn pre_run_fails_with_populated_records() {
        // A non-empty retrofit strategy cannot be applied through the framework
        // yet, so pre_run must fail rather than silently drop the reduction that
        // canonical NONROAD (`clcrtrft.f`) would apply.
        let s = NonRoadRetrofitStrategy::new(vec![make_record(1), make_record(2)]);
        let mut store = InMemoryStore::new();
        let err = s
            .pre_run(&mut store)
            .expect_err("pre_run must fail when records cannot be applied");
        assert!(matches!(err, moves_framework::Error::Nonroad(_)));
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
