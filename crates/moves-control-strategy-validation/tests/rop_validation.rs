//! Rate-of-Progress control-strategy validation (Phase 6 Task 124).
//!
//! Canonical Java formula: `emission_scale_factor = 1.0 − reductionFraction`.
//! Source: `RateOfProgressStrategy.java` in `internalcontrolstrategies/rateofprogress/`.

use std::path::Path;

use moves_framework::{CalculatorContext, InMemoryStore, InternalControlStrategy};
use moves_rate_of_progress::{
    csv_io as rop_csv,
    model::{RopKey, RopTable},
    RateOfProgressControlStrategy,
};

static FIXTURE: &str = include_str!("fixtures/rop_fixture.csv");

fn load_fixture() -> RopTable {
    rop_csv::read_reader(FIXTURE.as_bytes(), Path::new("rop_fixture.csv"))
        .expect("fixture CSV must parse")
        .table
}

// ---------------------------------------------------------------------------
// Fixture integrity
// ---------------------------------------------------------------------------

#[test]
fn fixture_csv_parses_all_rows() {
    let report = rop_csv::read_reader(FIXTURE.as_bytes(), Path::new("rop_fixture.csv"))
        .expect("fixture must parse");
    assert_eq!(report.table.len(), 8, "fixture has 8 data rows");
    assert!(report.duplicate_keys.is_empty());
    assert!(report.invalid_fractions.is_empty());
}

#[test]
fn fixture_covers_three_pollutants() {
    let t = load_fixture();
    let mut pollutants = t.pollutants();
    pollutants.dedup();
    assert_eq!(pollutants, vec![1, 2, 3], "VOC=1, CO=2, NOx=3");
}

#[test]
fn fixture_covers_three_source_types() {
    let t = load_fixture();
    let mut types = t.source_types();
    types.dedup();
    assert_eq!(types, vec![11, 21, 52]);
}

// ---------------------------------------------------------------------------
// Scale-factor correctness — canonical Java formula: 1.0 − reductionFraction
// ---------------------------------------------------------------------------

struct ScaleCase {
    pollutant: i32,
    source_type: i32,
    reg_class: i32,
    model_year: i32,
    expected_scale: f64,
}

fn scale_cases() -> Vec<ScaleCase> {
    vec![
        ScaleCase {
            pollutant: 3,
            source_type: 11,
            reg_class: 10,
            model_year: 2022,
            expected_scale: 1.0 - 0.25,
        },
        ScaleCase {
            pollutant: 3,
            source_type: 11,
            reg_class: 10,
            model_year: 2023,
            expected_scale: 1.0 - 0.30,
        },
        ScaleCase {
            pollutant: 3,
            source_type: 21,
            reg_class: 10,
            model_year: 2022,
            expected_scale: 1.0 - 0.15,
        },
        ScaleCase {
            pollutant: 3,
            source_type: 21,
            reg_class: 10,
            model_year: 2023,
            expected_scale: 1.0 - 0.20,
        },
        ScaleCase {
            pollutant: 2,
            source_type: 11,
            reg_class: 10,
            model_year: 2022,
            expected_scale: 1.0 - 0.40,
        },
        ScaleCase {
            pollutant: 2,
            source_type: 21,
            reg_class: 10,
            model_year: 2022,
            expected_scale: 1.0 - 0.10,
        },
        ScaleCase {
            pollutant: 1,
            source_type: 11,
            reg_class: 20,
            model_year: 2022,
            expected_scale: 1.0 - 0.50,
        },
        ScaleCase {
            pollutant: 2,
            source_type: 52,
            reg_class: 41,
            model_year: 2021,
            expected_scale: 1.0 - 0.35,
        },
    ]
}

#[test]
fn emission_scale_factors_match_canonical_formula() {
    let t = load_fixture();
    for c in scale_cases() {
        let key = RopKey {
            pollutant_id: c.pollutant,
            source_type_id: c.source_type,
            reg_class_id: c.reg_class,
            model_year_id: c.model_year,
        };
        let actual = t.scale_factor(&key);
        assert!(
            (actual - c.expected_scale).abs() < 1e-14,
            "key ({},{},{},{}): expected scale {:.6}, got {:.6}",
            c.pollutant,
            c.source_type,
            c.reg_class,
            c.model_year,
            c.expected_scale,
            actual
        );
    }
}

#[test]
fn scale_factor_is_one_for_absent_key() {
    let t = load_fixture();
    // Pollutant 99 is not in the fixture — expect no change (factor 1.0).
    let key = RopKey {
        pollutant_id: 99,
        source_type_id: 11,
        reg_class_id: 10,
        model_year_id: 2022,
    };
    assert!((t.scale_factor(&key) - 1.0).abs() < 1e-14);
}

#[test]
fn scale_factor_is_one_for_absent_model_year() {
    let t = load_fixture();
    // Model year 2000 is not in the fixture for any row.
    let key = RopKey {
        pollutant_id: 3,
        source_type_id: 11,
        reg_class_id: 10,
        model_year_id: 2000,
    };
    assert!((t.scale_factor(&key) - 1.0).abs() < 1e-14);
}

// Verify that a 50% reduction produces exactly 0.5 scale factor.
#[test]
fn half_reduction_produces_half_scale_factor() {
    let t = load_fixture();
    let key = RopKey {
        pollutant_id: 1,
        source_type_id: 11,
        reg_class_id: 20,
        model_year_id: 2022,
    };
    assert!((t.scale_factor(&key) - 0.5).abs() < 1e-14);
}

// ---------------------------------------------------------------------------
// CSV round-trip
// ---------------------------------------------------------------------------

#[test]
fn csv_round_trip_preserves_all_rows() {
    let t = load_fixture();
    let mut buf: Vec<u8> = Vec::new();
    rop_csv::write_writer(&t, &mut buf).expect("write must succeed");
    let text = String::from_utf8(buf).unwrap();
    let back = rop_csv::read_reader(text.as_bytes(), Path::new("round_trip")).unwrap();
    assert_eq!(back.table.len(), t.len());
    for rec in t.iter() {
        let key = rec.key();
        assert!(
            (back.table.scale_factor(&key) - t.scale_factor(&key)).abs() < 1e-14,
            "round-trip diverged for key ({},{},{},{})",
            key.pollutant_id,
            key.source_type_id,
            key.reg_class_id,
            key.model_year_id
        );
    }
}

// ---------------------------------------------------------------------------
// Strategy lifecycle with fixture data
// ---------------------------------------------------------------------------

#[test]
fn strategy_lifecycle_with_fixture_data() {
    let t = load_fixture();
    let strategy = RateOfProgressControlStrategy::new(t);
    let mut store = InMemoryStore::new();
    strategy.pre_run(&mut store).expect("pre_run must succeed");
    let ctx = CalculatorContext::new();
    strategy.post_run(&ctx).expect("post_run must succeed");
}

#[test]
fn strategy_modified_tables_non_empty() {
    let strategy = RateOfProgressControlStrategy::new(RopTable::new());
    assert!(
        !strategy.modified_tables().is_empty(),
        "ROP must declare the tables it modifies"
    );
}

#[test]
fn strategy_subscriptions_empty_rop_is_global() {
    let strategy = RateOfProgressControlStrategy::new(RopTable::new());
    assert!(
        strategy.subscriptions().is_empty(),
        "ROP applies globally in pre_run — no per-iteration subscriptions"
    );
}

#[test]
fn strategy_is_trait_object_safe_with_fixture() {
    let t = load_fixture();
    let strategy: Box<dyn InternalControlStrategy> =
        Box::new(RateOfProgressControlStrategy::new(t));
    let mut store = InMemoryStore::new();
    strategy.pre_run(&mut store).expect("pre_run ok");
    assert_eq!(strategy.name(), "RateOfProgressControlStrategy");
}
