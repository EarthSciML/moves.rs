//! Cross-strategy interaction tests.
//!
//! Validates that multiple control strategies coexist correctly in the
//! [`ControlStrategyRegistry`] and that their combined mathematical effect
//! matches the canonical Java order-of-application.
//!
//! # Java order-of-application (reference)
//!
//! Canonical MOVES registers strategies in this order:
//!
//! 1. `AvftControlStrategy` — modifies `AVFT`
//! 2. `RateOfProgressControlStrategy` — modifies `ratepollutantprocessmodelyeargroup`, `sourceTypeModelYear`
//! 3. `OnRoadRetrofitStrategy` — modifies `emissionRateAdjustment`
//! 4. `NonRoadRetrofitStrategy` — modifies no shared table (inline calculation)
//!
//! Because each strategy targets independent tables (except that ROP and
//! OnRoadRetrofit both feed into the final emission rate, via base rates vs.
//! multiplicative adjustment), the combined downstream emission factor is:
//!
//! ```text
//! emission_rate_final = base_rate × ROP_scale × OnRoadRetrofit_factor
//! ```
//!
//! All four strategies can be registered simultaneously without conflict.

use moves_avft::{model::AvftTable, AvftControlStrategy};
use moves_framework::{
    CalculatorContext, ControlStrategyRegistry, InMemoryStore, InternalControlStrategy,
};
use moves_nonroad::population::retrofit::RetrofitRecord as NonRoadRecord;
use moves_nonroad_retrofit::NonRoadRetrofitStrategy;
use moves_onroad_retrofit::{OnRoadRetrofitStrategy, RetrofitRecord, RetrofitTable};
use moves_rate_of_progress::{
    model::{RopKey, RopTable},
    RateOfProgressControlStrategy,
};

// ---------------------------------------------------------------------------
// Registry: instantiation order is preserved
// ---------------------------------------------------------------------------

// These zero-argument factories are needed because ControlStrategyFactory is
// fn() -> Box<dyn InternalControlStrategy>. Data-carrying tests below
// use direct instantiation to avoid the restriction.

fn avft_factory() -> Box<dyn InternalControlStrategy> {
    Box::new(AvftControlStrategy::from_completed(AvftTable::new()))
}
fn rop_factory() -> Box<dyn InternalControlStrategy> {
    Box::new(RateOfProgressControlStrategy::new(RopTable::new()))
}
fn onroad_retrofit_factory() -> Box<dyn InternalControlStrategy> {
    Box::new(OnRoadRetrofitStrategy::new(RetrofitTable::new()))
}
fn nonroad_retrofit_factory() -> Box<dyn InternalControlStrategy> {
    Box::new(NonRoadRetrofitStrategy::new(vec![]))
}

#[test]
fn registry_instantiates_all_four_strategies_in_registration_order() {
    let mut registry = ControlStrategyRegistry::new();
    registry.register(avft_factory);
    registry.register(rop_factory);
    registry.register(onroad_retrofit_factory);
    registry.register(nonroad_retrofit_factory);

    let strategies = registry.instantiate_all();
    assert_eq!(strategies.len(), 4);
    assert_eq!(strategies[0].name(), "AvftControlStrategy");
    assert_eq!(strategies[1].name(), "RateOfProgressControlStrategy");
    assert_eq!(strategies[2].name(), "OnRoadRetrofitStrategy");
    assert_eq!(strategies[3].name(), "NonRoadRetrofitStrategy");
}

#[test]
fn registry_runs_pre_run_for_all_strategies() {
    let mut registry = ControlStrategyRegistry::new();
    registry.register(avft_factory);
    registry.register(rop_factory);
    registry.register(onroad_retrofit_factory);
    registry.register(nonroad_retrofit_factory);

    let strategies = registry.instantiate_all();
    let mut store = InMemoryStore::new();
    for s in &strategies {
        let result = s.pre_run(&mut store);
        match s.name() {
            // RateOfProgress is not yet ported (the `RateOfProgressStrategy.sql`
            // model-year-group propagation depends on default-DB tables this data
            // plane does not expose). It reports the unported condition
            // explicitly rather than silently succeeding — see
            // RateOfProgressControlStrategy::pre_run.
            "RateOfProgressControlStrategy" => assert!(
                matches!(result, Err(moves_framework::Error::NotImplemented)),
                "ROP pre_run should report NotImplemented until ported, got {result:?}"
            ),
            // AVFT and the empty retrofit strategies have nothing to block on and
            // apply cleanly.
            other => result.unwrap_or_else(|e| panic!("{other} pre_run failed: {e}")),
        }
    }
}

#[test]
fn registry_runs_post_run_for_all_strategies() {
    let mut registry = ControlStrategyRegistry::new();
    registry.register(avft_factory);
    registry.register(rop_factory);
    registry.register(onroad_retrofit_factory);
    registry.register(nonroad_retrofit_factory);

    let strategies = registry.instantiate_all();
    let ctx = CalculatorContext::new();
    for s in &strategies {
        s.post_run(&ctx)
            .unwrap_or_else(|e| panic!("{} post_run failed: {e}", s.name()));
    }
}

// ---------------------------------------------------------------------------
// Modified-table declarations: each strategy targets independent tables
// ---------------------------------------------------------------------------

#[test]
fn all_strategies_target_independent_tables() {
    let strategies: Vec<Box<dyn InternalControlStrategy>> = vec![
        Box::new(AvftControlStrategy::from_completed(AvftTable::new())),
        Box::new(RateOfProgressControlStrategy::new(RopTable::new())),
        Box::new(OnRoadRetrofitStrategy::new(RetrofitTable::new())),
        Box::new(NonRoadRetrofitStrategy::new(vec![])),
    ];

    let avft_tables: Vec<&str> = strategies[0].modified_tables().to_vec();
    let rop_tables: Vec<&str> = strategies[1].modified_tables().to_vec();
    let onroad_tables: Vec<&str> = strategies[2].modified_tables().to_vec();
    let nonroad_tables: Vec<&str> = strategies[3].modified_tables().to_vec();

    // AVFT does not touch emission-rate tables.
    assert!(!avft_tables.contains(&"emissionRateAdjustment"));
    assert!(!avft_tables.contains(&"ratepollutantprocessmodelyeargroup"));

    // ROP does not touch the AVFT fleet-composition table.
    assert!(!rop_tables.contains(&"AVFT"));
    assert!(!rop_tables.contains(&"emissionRateAdjustment"));

    // OnRoadRetrofit does not overwrite the AVFT or raw rate tables directly.
    assert!(!onroad_tables.contains(&"AVFT"));

    // NonRoadRetrofit writes nothing to the shared execution DB.
    assert!(nonroad_tables.is_empty());
}

#[test]
fn combined_modified_tables_covers_all_affected_tables() {
    let strategies: Vec<Box<dyn InternalControlStrategy>> = vec![
        Box::new(AvftControlStrategy::from_completed(AvftTable::new())),
        Box::new(RateOfProgressControlStrategy::new(RopTable::new())),
        Box::new(OnRoadRetrofitStrategy::new(RetrofitTable::new())),
    ];
    let all_tables: Vec<&str> = strategies
        .iter()
        .flat_map(|s| s.modified_tables().iter().copied())
        .collect();

    assert!(all_tables.contains(&"AVFT"), "AVFT must be covered");
    assert!(
        all_tables.contains(&"emissionRateAdjustment"),
        "emissionRateAdjustment must be covered"
    );
    assert!(
        all_tables
            .iter()
            .any(|&t| t.contains("ratepollutant") || t.contains("sourceType")),
        "at least one emission-rate table must be covered"
    );
}

// ---------------------------------------------------------------------------
// No subscriptions: all four strategies apply globally in pre_run
// ---------------------------------------------------------------------------

#[test]
fn all_strategies_have_no_per_iteration_subscriptions() {
    let strategies: Vec<Box<dyn InternalControlStrategy>> = vec![
        Box::new(AvftControlStrategy::from_completed(AvftTable::new())),
        Box::new(RateOfProgressControlStrategy::new(RopTable::new())),
        Box::new(OnRoadRetrofitStrategy::new(RetrofitTable::new())),
        Box::new(NonRoadRetrofitStrategy::new(vec![])),
    ];
    for s in &strategies {
        assert!(
            s.subscriptions().is_empty(),
            "{} has unexpected subscriptions — all strategies apply globally in pre_run",
            s.name()
        );
    }
}

// ---------------------------------------------------------------------------
// Mathematical compound effect (Java order-of-application)
// ---------------------------------------------------------------------------

// ROP applies a global scale factor to emission rates.
// OnRoadRetrofit applies a multiplicative adjustment via emissionRateAdjustment.
// Combined downstream emission = base_rate × ROP_scale × Retrofit_factor.
//
// This test verifies the mathematical property directly (the data plane
// write is pending) by computing both factors independently and
// confirming their product matches the expected compound value.

#[test]
fn rop_and_onroad_retrofit_compound_multiplicatively() {
    // Fixture: NOx (pollutant=3), passenger cars (source=11), reg class 10,
    // model year 2010. Two independent strategies affect this combination.

    // ROP reduces NOx by 25% → scale factor = 0.75.
    let rop_table: RopTable = [moves_rate_of_progress::RopRecord::new(
        3, 11, 10, 2010, 0.25,
    )]
    .into_iter()
    .collect();
    let rop_scale = rop_table.scale_factor(&RopKey {
        pollutant_id: 3,
        source_type_id: 11,
        reg_class_id: 10,
        model_year_id: 2010,
    });
    assert!((rop_scale - 0.75).abs() < 1e-14, "ROP scale = 0.75");

    // OnRoadRetrofit: 50% of fleet retrofitted, 80% effective → factor = 0.60.
    let retrofit_programs: RetrofitTable = [RetrofitRecord::new(
        11,   // sourceTypeID
        2005, // startModelYear
        2015, // endModelYear
        2020, // retrofitYearID
        3,    // pollutantID (NOx)
        1,    // processID (running exhaust)
        0.50, // cumulativeRetrofitFraction
        0.80, // retrofitEffectiveness
    )]
    .into_iter()
    .collect();
    let retrofit_factor = retrofit_programs.combined_factor(11, 2010, 3, 1, 2025);
    assert!(
        (retrofit_factor - 0.60).abs() < 1e-12,
        "Retrofit factor = 0.60"
    );

    // Combined (Java order: ROP first, then OnRoadRetrofit).
    // base_rate × 0.75 × 0.60 = base_rate × 0.45
    let combined = rop_scale * retrofit_factor;
    assert!(
        (combined - 0.45).abs() < 1e-12,
        "Combined = 0.75 × 0.60 = 0.45; got {combined}"
    );
}

#[test]
fn avft_and_rop_are_orthogonal() {
    // AVFT modifies fleet-composition fractions; ROP modifies emission rates.
    // They operate on completely separate tables and never interfere.
    let avft_strategy = AvftControlStrategy::from_completed(AvftTable::new());
    let rop_strategy = RateOfProgressControlStrategy::new(RopTable::new());

    let avft_tables = avft_strategy.modified_tables();
    let rop_tables = rop_strategy.modified_tables();

    for t in avft_tables {
        assert!(
            !rop_tables.contains(t),
            "AVFT table '{t}' must not appear in ROP's modified_tables"
        );
    }
    for t in rop_tables {
        assert!(
            !avft_tables.contains(t),
            "ROP table '{t}' must not appear in AVFT's modified_tables"
        );
    }
}

#[test]
fn nonroad_retrofit_does_not_conflict_with_onroad_strategies() {
    // NonRoadRetrofit has empty modified_tables — it never writes to the
    // shared execution DB, so it cannot conflict with any onroad strategy.
    let nonroad = NonRoadRetrofitStrategy::new(vec![NonRoadRecord {
        record_index: 0,
        id: 1,
        year_retrofit_start: 2020,
        year_retrofit_end: 2025,
        year_model_start: 2000,
        year_model_end: 2015,
        scc: "ALL".to_string(),
        tech_type: "ALL".to_string(),
        hp_min: 0.0_f32,
        hp_max: 100.0_f32,
        annual_frac_or_count: 0.3_f32,
        effectiveness: 0.7_f32,
        pollutant: "HC".to_string(),
        pollutant_idx: 1,
    }]);
    assert!(nonroad.modified_tables().is_empty());

    let onroad = OnRoadRetrofitStrategy::new(RetrofitTable::new());
    for t in nonroad.modified_tables() {
        assert!(
            !onroad.modified_tables().contains(t),
            "NonRoadRetrofit must not declare any onroad table"
        );
    }
}

// ---------------------------------------------------------------------------
// All four strategies active simultaneously: full lifecycle passes
// ---------------------------------------------------------------------------

#[test]
fn full_lifecycle_four_strategies_active_simultaneously() {
    let rop_table: RopTable = [
        moves_rate_of_progress::RopRecord::new(3, 11, 10, 2022, 0.25),
        moves_rate_of_progress::RopRecord::new(2, 21, 10, 2022, 0.15),
    ]
    .into_iter()
    .collect();

    let retrofit_programs: RetrofitTable = [
        RetrofitRecord::new(11, 2005, 2015, 2020, 3, 1, 0.25, 0.80),
        RetrofitRecord::new(21, 2000, 2020, 2018, 98, 1, 0.40, 0.60),
    ]
    .into_iter()
    .collect();

    let nonroad_recs = vec![NonRoadRecord {
        record_index: 0,
        id: 1,
        year_retrofit_start: 2020,
        year_retrofit_end: 2025,
        year_model_start: 2000,
        year_model_end: 2015,
        scc: "ALL".to_string(),
        tech_type: "ALL".to_string(),
        hp_min: 0.0_f32,
        hp_max: 100.0_f32,
        annual_frac_or_count: 0.3_f32,
        effectiveness: 0.7_f32,
        pollutant: "HC".to_string(),
        pollutant_idx: 1,
    }];

    let strategies: Vec<Box<dyn InternalControlStrategy>> = vec![
        Box::new(AvftControlStrategy::from_completed(AvftTable::new())),
        Box::new(RateOfProgressControlStrategy::new(rop_table)),
        Box::new(OnRoadRetrofitStrategy::new(retrofit_programs)),
        Box::new(NonRoadRetrofitStrategy::new(nonroad_recs)),
    ];

    let mut store = InMemoryStore::new();
    for s in &strategies {
        let result = s.pre_run(&mut store);
        match s.name() {
            // AVFT (here built empty) applies cleanly.
            "AvftControlStrategy" => result.unwrap_or_else(|e| panic!("AVFT pre_run failed: {e}")),
            // ROP is unported; a populated table still reports NotImplemented
            // rather than silently dropping its reductions.
            "RateOfProgressControlStrategy" => assert!(
                matches!(result, Err(moves_framework::Error::NotImplemented)),
                "ROP pre_run should report NotImplemented until ported, got {result:?}"
            ),
            // A *populated* on-road retrofit cannot yet be applied through the
            // framework, so pre_run fails loudly rather than no-op (canonical
            // OnRoadRetrofitStrategy always applies its adjustment).
            "OnRoadRetrofitStrategy" => assert!(
                result.is_err(),
                "populated OnRoadRetrofit pre_run should fail until a write API \
                 exists, got {result:?}"
            ),
            // A *populated* nonroad retrofit likewise cannot be bridged to
            // ReferenceData::retrofit_records, so pre_run fails loudly.
            "NonRoadRetrofitStrategy" => assert!(
                result.is_err(),
                "populated NonRoadRetrofit pre_run should fail until a write API \
                 exists, got {result:?}"
            ),
            other => panic!("unexpected strategy {other}"),
        }
    }
    // post_run is a no-op for every strategy regardless of port status.
    let ctx = CalculatorContext::new();
    for s in &strategies {
        s.post_run(&ctx)
            .unwrap_or_else(|e| panic!("{} post_run failed: {e}", s.name()));
    }
}
