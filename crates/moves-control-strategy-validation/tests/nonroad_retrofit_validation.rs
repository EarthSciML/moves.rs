//! NonRoadRetrofit control-strategy validation.
//!
//! Validates [`NonRoadRetrofitStrategy`], which wraps NONROAD's Fortran
//! `clcrtrft.f`-derived retrofit records in the unified control-strategy
//! framework. The actual per-SCC reduction computation is performed by
//! `moves_nonroad::emissions::retrofit::calculate_retrofit_reduction`; this
//! strategy is the framework adapter that holds the records.

use moves_framework::{CalculatorContext, InMemoryStore, InternalControlStrategy};
use moves_nonroad::population::retrofit::RetrofitRecord;
use moves_nonroad_retrofit::NonRoadRetrofitStrategy;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_record(id: i32, scc: &str, tech: &str, frac: f32, effectiveness: f32) -> RetrofitRecord {
    RetrofitRecord {
        record_index: id as usize,
        id,
        year_retrofit_start: 2020,
        year_retrofit_end: 2025,
        year_model_start: 2000,
        year_model_end: 2015,
        scc: scc.to_string(),
        tech_type: tech.to_string(),
        hp_min: 0.0_f32,
        hp_max: 100.0_f32,
        annual_frac_or_count: frac,
        effectiveness,
        pollutant: "HC".to_string(),
        pollutant_idx: 1,
    }
}

// ---------------------------------------------------------------------------
// Record storage and retrieval
// ---------------------------------------------------------------------------

#[test]
fn records_retained_in_order() {
    let recs = vec![
        make_record(1, "ALL", "ALL", 0.5, 0.6),
        make_record(2, "2265004010", "ALL", 0.3, 0.8),
        make_record(3, "ALL", "4ST", 0.4, 0.7),
    ];
    let strategy = NonRoadRetrofitStrategy::new(recs);
    assert_eq!(strategy.records().len(), 3);
    assert_eq!(strategy.records()[0].id, 1);
    assert_eq!(strategy.records()[1].id, 2);
    assert_eq!(strategy.records()[2].id, 3);
}

#[test]
fn records_fields_accessible() {
    let r = make_record(7, "2265004010", "4ST", 0.25, 0.80);
    let strategy = NonRoadRetrofitStrategy::new(vec![r]);
    let stored = &strategy.records()[0];
    assert_eq!(stored.id, 7);
    assert_eq!(stored.scc, "2265004010");
    assert_eq!(stored.tech_type, "4ST");
    assert!((stored.annual_frac_or_count - 0.25_f32).abs() < 1e-6_f32);
    assert!((stored.effectiveness - 0.80_f32).abs() < 1e-6_f32);
    assert_eq!(stored.year_retrofit_start, 2020);
    assert_eq!(stored.year_retrofit_end, 2025);
    assert_eq!(stored.year_model_start, 2000);
    assert_eq!(stored.year_model_end, 2015);
    assert_eq!(stored.pollutant, "HC");
}

#[test]
fn empty_records_slice_accepted() {
    let strategy = NonRoadRetrofitStrategy::new(vec![]);
    assert_eq!(strategy.records().len(), 0);
}

#[test]
fn wildcard_scc_all_accepted() {
    let strategy = NonRoadRetrofitStrategy::new(vec![make_record(1, "ALL", "ALL", 0.5, 0.5)]);
    assert_eq!(strategy.records()[0].scc, "ALL");
    assert_eq!(strategy.records()[0].tech_type, "ALL");
}

// ---------------------------------------------------------------------------
// Framework contract
// ---------------------------------------------------------------------------

#[test]
fn modified_tables_is_empty_nonroad_inline() {
    // NONROAD does not write into the onroad emissionRateAdjustment table;
    // reduction is applied inline during the geography loop.
    let strategy = NonRoadRetrofitStrategy::new(vec![]);
    assert!(
        strategy.modified_tables().is_empty(),
        "NonRoadRetrofit has no shared execution-DB table to invalidate"
    );
}

#[test]
fn subscriptions_empty_lifecycle_is_global() {
    let strategy = NonRoadRetrofitStrategy::new(vec![]);
    assert!(
        strategy.subscriptions().is_empty(),
        "NonRoadRetrofit has no per-iteration subscriptions"
    );
}

#[test]
fn pre_run_fails_loudly_with_multiple_records() {
    // The nonroad calculator reads retrofit records from
    // ReferenceData::retrofit_records, and no framework write API bridges this
    // strategy to it. Rather than silently dropping the reductions canonical
    // NONROAD (clcrtrft.f) always applies, a populated strategy fails loudly in
    // pre_run. (An empty strategy is a harmless no-op — see post_run_succeeds.)
    let recs = vec![
        make_record(1, "ALL", "ALL", 0.5, 0.6),
        make_record(2, "2265004010", "ALL", 0.3, 0.8),
        make_record(3, "ALL", "4ST", 0.4, 0.7),
        make_record(4, "2270003010", "2ST", 0.2, 0.9),
    ];
    let strategy = NonRoadRetrofitStrategy::new(recs);
    let mut store = InMemoryStore::new();
    assert!(
        strategy.pre_run(&mut store).is_err(),
        "a populated NonRoadRetrofit pre_run must fail until a write API exists"
    );
}

#[test]
fn post_run_succeeds() {
    let strategy = NonRoadRetrofitStrategy::new(vec![make_record(1, "ALL", "ALL", 0.3, 0.7)]);
    let ctx = CalculatorContext::new();
    strategy.post_run(&ctx).expect("post_run must succeed");
}

#[test]
fn strategy_is_trait_object_safe() {
    let strategy: Box<dyn InternalControlStrategy> =
        Box::new(NonRoadRetrofitStrategy::new(vec![make_record(
            1, "ALL", "ALL", 0.5, 0.6,
        )]));
    assert_eq!(strategy.name(), "NonRoadRetrofitStrategy");
}

#[test]
fn strategy_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NonRoadRetrofitStrategy>();
}
