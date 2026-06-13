//! OnRoadRetrofit control-strategy validation.
//!
//! Canonical Java formula:
//! `factor = ∏ over active programs p of (1 − p.fraction × p.effectiveness)`
//!
//! "Active" means `p.retrofitYearID ≤ analysisYear` AND model year within
//! `[p.startModelYear, p.endModelYear]`. Source: `OnRoadRetrofit.java` in
//! `internalcontrolstrategies/onroadretrofit/`.

use moves_framework::{CalculatorContext, InMemoryStore, InternalControlStrategy};
use moves_onroad_retrofit::{OnRoadRetrofitStrategy, RetrofitRecord, RetrofitTable};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn rec(
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

// ---------------------------------------------------------------------------
// Single-program: factor = 1 − fraction × effectiveness
// ---------------------------------------------------------------------------

#[test]
fn single_program_factor_matches_canonical_formula() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2020, 3, 1, 0.25, 0.80)]
        .into_iter()
        .collect();
    let expected = 1.0 - 0.25 * 0.80; // 0.80
    let actual = programs.combined_factor(11, 2010, 3, 1, 2025);
    assert!(
        (actual - expected).abs() < 1e-12,
        "single program: expected {expected}, got {actual}"
    );
}

#[test]
fn full_fleet_retrofit_full_effectiveness_is_zero() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2020, 3, 1, 1.0, 1.0)]
        .into_iter()
        .collect();
    let actual = programs.combined_factor(11, 2010, 3, 1, 2025);
    assert!(
        actual.abs() < 1e-12,
        "complete elimination: expected 0.0, got {actual}"
    );
}

#[test]
fn zero_fraction_retrofitted_means_no_adjustment() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2020, 3, 1, 0.0, 0.80)]
        .into_iter()
        .collect();
    let actual = programs.combined_factor(11, 2010, 3, 1, 2025);
    assert!(
        (actual - 1.0).abs() < 1e-12,
        "zero fraction: no adjustment, expected 1.0"
    );
}

// ---------------------------------------------------------------------------
// Multiple retrofit-year buckets: the most-recent bucket ≤ analysis_year wins
// (cumFractionRetrofit is cumulative; canonical buckets do NOT multiply — see
// OnRoadRetrofitStrategy.java compile()/maxCalendarYear and
// RetrofitTable::combined_factor).
// ---------------------------------------------------------------------------

#[test]
fn two_programs_most_recent_retrofit_year_supersedes() {
    // Buckets at retrofitYear 2010 and 2015, analysis year 2020 → only the 2015
    // bucket applies: factor = 1 − 0.20×0.50 = 0.90.
    let programs: RetrofitTable = [
        rec(11, 2000, 2015, 2010, 3, 1, 0.30, 0.60),
        rec(11, 2000, 2015, 2015, 3, 1, 0.20, 0.50),
    ]
    .into_iter()
    .collect();
    let expected = 1.0 - 0.20 * 0.50;
    let actual = programs.combined_factor(11, 2010, 3, 1, 2020);
    assert!(
        (actual - expected).abs() < 1e-12,
        "two programs: expected {expected}, got {actual}"
    );
}

#[test]
fn three_programs_most_recent_retrofit_year_applies() {
    // Buckets at retrofitYear 2018, 2020, 2022; analysis year 2025 → only the
    // 2022 bucket applies: factor = 1 − 0.10×0.90 = 0.91.
    let programs: RetrofitTable = [
        rec(21, 2010, 2020, 2018, 98, 1, 0.40, 0.70),
        rec(21, 2010, 2020, 2020, 98, 1, 0.25, 0.80),
        rec(21, 2010, 2020, 2022, 98, 1, 0.10, 0.90),
    ]
    .into_iter()
    .collect();
    let expected = 1.0 - 0.10 * 0.90;
    let actual = programs.combined_factor(21, 2015, 98, 1, 2025);
    assert!(
        (actual - expected).abs() < 1e-12,
        "three programs: expected {expected}, got {actual}"
    );
}

// ---------------------------------------------------------------------------
// Gating: future retrofit years are excluded
// ---------------------------------------------------------------------------

#[test]
fn future_retrofit_year_not_yet_applied() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2030, 3, 1, 0.50, 0.80)]
        .into_iter()
        .collect();
    // Analysis year 2025 < retrofitYear 2030 → program not yet active.
    let actual = programs.combined_factor(11, 2010, 3, 1, 2025);
    assert!(
        (actual - 1.0).abs() < 1e-12,
        "future program not yet applied"
    );
}

#[test]
fn exactly_meeting_retrofit_year_applies() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2025, 3, 1, 0.50, 0.80)]
        .into_iter()
        .collect();
    let expected = 1.0 - 0.50 * 0.80;
    let actual = programs.combined_factor(11, 2010, 3, 1, 2025);
    assert!((actual - expected).abs() < 1e-12);
}

// ---------------------------------------------------------------------------
// Gating: model year outside program range is excluded
// ---------------------------------------------------------------------------

#[test]
fn model_year_below_range_excluded() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2020, 3, 1, 0.50, 0.80)]
        .into_iter()
        .collect();
    let actual = programs.combined_factor(11, 2004, 3, 1, 2025);
    assert!(
        (actual - 1.0).abs() < 1e-12,
        "model year 2004 < startModelYear 2005"
    );
}

#[test]
fn model_year_above_range_excluded() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2020, 3, 1, 0.50, 0.80)]
        .into_iter()
        .collect();
    let actual = programs.combined_factor(11, 2016, 3, 1, 2025);
    assert!(
        (actual - 1.0).abs() < 1e-12,
        "model year 2016 > endModelYear 2015"
    );
}

#[test]
fn model_year_at_range_boundary_is_inclusive() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2020, 3, 1, 0.50, 0.80)]
        .into_iter()
        .collect();
    let expected = 1.0 - 0.50 * 0.80;
    let lo = programs.combined_factor(11, 2005, 3, 1, 2025);
    let hi = programs.combined_factor(11, 2015, 3, 1, 2025);
    assert!((lo - expected).abs() < 1e-12, "boundary 2005 should apply");
    assert!((hi - expected).abs() < 1e-12, "boundary 2015 should apply");
}

// ---------------------------------------------------------------------------
// Multiple source types: each source type's programs are independent
// ---------------------------------------------------------------------------

#[test]
fn two_source_types_independent_factors() {
    let programs: RetrofitTable = [
        rec(11, 2005, 2015, 2020, 3, 1, 0.25, 0.80),
        rec(21, 2005, 2015, 2020, 3, 1, 0.40, 0.60),
    ]
    .into_iter()
    .collect();

    let f11 = programs.combined_factor(11, 2010, 3, 1, 2025);
    let f21 = programs.combined_factor(21, 2010, 3, 1, 2025);
    assert!((f11 - (1.0 - 0.25 * 0.80)).abs() < 1e-12);
    assert!((f21 - (1.0 - 0.40 * 0.60)).abs() < 1e-12);
    // Cross-type: source 21's programs do not bleed into source 11.
    assert!((f11 - f21).abs() > 1e-6, "source types must be independent");
}

// ---------------------------------------------------------------------------
// Strategy lifecycle
// ---------------------------------------------------------------------------

#[test]
fn strategy_lifecycle_with_fixture_programs() {
    let programs: RetrofitTable = [
        rec(11, 2005, 2015, 2020, 3, 1, 0.25, 0.80),
        rec(21, 2000, 2020, 2018, 98, 1, 0.40, 0.60),
        rec(52, 2010, 2020, 2022, 3, 1, 0.20, 0.90),
    ]
    .into_iter()
    .collect();
    let strategy = OnRoadRetrofitStrategy::new(programs);
    let mut store = InMemoryStore::new();
    // A *populated* on-road retrofit cannot yet be applied through the framework
    // (emissionRateAdjustment needs source-bin keys RetrofitRecord does not
    // carry), so pre_run reports the unported condition rather than silently
    // no-opping the adjustment canonical OnRoadRetrofitStrategy always applies.
    assert!(
        matches!(
            strategy.pre_run(&mut store),
            Err(moves_framework::Error::NotImplemented)
        ),
        "populated OnRoadRetrofit pre_run should report NotImplemented until ported"
    );
    // post_run remains a no-op regardless of port status.
    let ctx = CalculatorContext::new();
    strategy.post_run(&ctx).expect("post_run must succeed");
}

#[test]
fn strategy_modified_tables_contains_emission_rate_adjustment() {
    let strategy = OnRoadRetrofitStrategy::new(RetrofitTable::new());
    assert!(
        strategy
            .modified_tables()
            .contains(&"emissionRateAdjustment"),
        "OnRoadRetrofit must declare emissionRateAdjustment"
    );
}

#[test]
fn strategy_subscriptions_empty_retrofit_is_global() {
    let strategy = OnRoadRetrofitStrategy::new(RetrofitTable::new());
    assert!(
        strategy.subscriptions().is_empty(),
        "OnRoadRetrofit applies globally in pre_run"
    );
}

#[test]
fn strategy_is_trait_object_safe_with_programs() {
    let programs: RetrofitTable = [rec(11, 2005, 2015, 2020, 3, 1, 0.25, 0.80)]
        .into_iter()
        .collect();
    let strategy: Box<dyn InternalControlStrategy> =
        Box::new(OnRoadRetrofitStrategy::new(programs));
    assert_eq!(strategy.name(), "OnRoadRetrofitStrategy");
}
