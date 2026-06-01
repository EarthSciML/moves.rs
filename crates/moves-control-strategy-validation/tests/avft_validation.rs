//! AVFT control-strategy validation.
//!
//! Validates [`AvftControlStrategy`] constructed from both a pre-built table
//! (`from_completed`) and from raw tool inputs (`from_tool_inputs`), confirming
//! that the Rust port's gap-fill + projection logic produces a valid output:
//! fractions sum to 1.0 per `(sourceTypeID, modelYearID)` group.
//!
//! Source: `AVFTTool.java` in the MOVES source. The canonical property is
//! that after gap-fill and projection, the sum of `fuelEngFraction` over
//! all `(fuelTypeID, engTechID)` combinations for a given
//! `(sourceTypeID, modelYearID)` equals 1.0.

use std::path::Path;

use moves_avft::{
    csv_io as avft_csv,
    model::AvftTable,
    spec::{GapFillingMethod, MethodEntry, ProjectionMethod, ToolSpec},
    AvftControlStrategy,
};
use moves_framework::{CalculatorContext, InMemoryStore, InternalControlStrategy};

static USER_AVFT: &str = include_str!("fixtures/avft_fixture.csv");
static DEFAULT_AVFT: &str = include_str!("fixtures/avft_default.csv");

fn load_user_table() -> AvftTable {
    avft_csv::read_reader(USER_AVFT.as_bytes(), Path::new("avft_fixture.csv"))
        .expect("fixture must parse")
        .table
}

fn load_default_table() -> AvftTable {
    avft_csv::read_reader(DEFAULT_AVFT.as_bytes(), Path::new("avft_default.csv"))
        .expect("default fixture must parse")
        .table
}

fn spec_for(source_types: &[i32]) -> ToolSpec {
    ToolSpec {
        last_complete_model_year: 2023,
        analysis_year: 2023,
        methods: source_types
            .iter()
            .map(|&id| MethodEntry {
                source_type_id: id,
                enabled: true,
                gap_filling: GapFillingMethod::DefaultsPreserveInputs,
                projection: ProjectionMethod::Constant,
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Fixture integrity
// ---------------------------------------------------------------------------

#[test]
fn user_fixture_csv_parses_correctly() {
    let report =
        avft_csv::read_reader(USER_AVFT.as_bytes(), Path::new("avft_fixture.csv")).unwrap();
    assert_eq!(report.table.len(), 10);
    assert!(report.duplicate_keys.is_empty());
}

#[test]
fn default_fixture_csv_parses_correctly() {
    let report =
        avft_csv::read_reader(DEFAULT_AVFT.as_bytes(), Path::new("avft_default.csv")).unwrap();
    assert_eq!(report.table.len(), 10);
}

// ---------------------------------------------------------------------------
// from_completed: pre-built table is preserved unchanged
// ---------------------------------------------------------------------------

#[test]
fn from_completed_preserves_table() {
    let t = load_user_table();
    let strategy = AvftControlStrategy::from_completed(t.clone());
    assert_eq!(strategy.completed_table().to_vec(), t.to_vec());
}

#[test]
fn from_completed_fractions_sum_to_one_per_group() {
    let t = load_user_table();
    let strategy = AvftControlStrategy::from_completed(t);
    check_fractions_sum_to_one(strategy.completed_table());
}

// ---------------------------------------------------------------------------
// from_tool_inputs: gap-fill + projection must produce valid output
// ---------------------------------------------------------------------------

#[test]
fn from_tool_inputs_with_user_and_defaults() {
    let user = load_user_table();
    let default = load_default_table();
    let known = AvftTable::new();
    let spec = spec_for(&[11, 21]);

    let strategy = AvftControlStrategy::from_tool_inputs(&spec, &user, &default, &known)
        .expect("tool must succeed");
    assert!(
        !strategy.completed_table().is_empty(),
        "completed table must be non-empty"
    );
}

#[test]
fn from_tool_inputs_fractions_sum_to_one_per_group() {
    let user = load_user_table();
    let default = load_default_table();
    let known = AvftTable::new();
    let spec = spec_for(&[11, 21]);

    let strategy = AvftControlStrategy::from_tool_inputs(&spec, &user, &default, &known)
        .expect("tool must succeed");
    check_fractions_sum_to_one(strategy.completed_table());
}

#[test]
fn from_tool_inputs_covers_all_source_types_in_spec() {
    let user = load_user_table();
    let default = load_default_table();
    let known = AvftTable::new();
    let spec = spec_for(&[11, 21]);

    let strategy = AvftControlStrategy::from_tool_inputs(&spec, &user, &default, &known)
        .expect("tool must succeed");
    let types = strategy.completed_table().source_types();
    assert!(types.contains(&11), "source type 11 must be in output");
    assert!(types.contains(&21), "source type 21 must be in output");
}

// ---------------------------------------------------------------------------
// Strategy lifecycle
// ---------------------------------------------------------------------------

#[test]
fn strategy_lifecycle_from_completed() {
    let strategy = AvftControlStrategy::from_completed(load_user_table());
    let mut store = InMemoryStore::new();
    strategy.pre_run(&mut store).expect("pre_run must succeed");
    let ctx = CalculatorContext::new();
    strategy.post_run(&ctx).expect("post_run must succeed");
}

#[test]
fn strategy_modified_tables_contains_avft() {
    let strategy = AvftControlStrategy::from_completed(AvftTable::new());
    assert!(
        strategy.modified_tables().contains(&"AVFT"),
        "AvftControlStrategy must declare AVFT in modified_tables"
    );
}

#[test]
fn strategy_subscriptions_empty_avft_is_global() {
    let strategy = AvftControlStrategy::from_completed(AvftTable::new());
    assert!(
        strategy.subscriptions().is_empty(),
        "AVFT applies globally in pre_run — no per-iteration subscriptions"
    );
}

#[test]
fn strategy_is_trait_object_safe_from_completed() {
    let strategy: Box<dyn InternalControlStrategy> =
        Box::new(AvftControlStrategy::from_completed(load_user_table()));
    assert_eq!(strategy.name(), "AvftControlStrategy");
    let mut store = InMemoryStore::new();
    strategy.pre_run(&mut store).expect("pre_run ok");
}

// ---------------------------------------------------------------------------
// CSV round-trip: a completed AVFT table survives write→read unchanged
// ---------------------------------------------------------------------------

#[test]
fn avft_csv_round_trip() {
    let t = load_user_table();
    let mut buf: Vec<u8> = Vec::new();
    avft_csv::write_writer(&t, &mut buf).expect("write must succeed");
    let text = String::from_utf8(buf).unwrap();
    let back = avft_csv::read_reader(text.as_bytes(), Path::new("round_trip")).unwrap();
    assert_eq!(back.table.len(), t.len());
    for rec in t.to_vec() {
        let key = rec.key();
        let original = t.get(&key).expect("key must exist in original");
        let recovered = back
            .table
            .get(&key)
            .expect("key must exist after round-trip");
        assert!(
            (recovered - original).abs() < 1e-14,
            "fraction diverged at key ({},{},{},{}): expected {original}, got {recovered}",
            key.source_type_id,
            key.model_year_id,
            key.fuel_type_id,
            key.eng_tech_id
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Assert fractions sum to 1.0 ± 1e-9 for every (sourceTypeID, modelYearID) group.
fn check_fractions_sum_to_one(t: &AvftTable) {
    use std::collections::BTreeMap;

    let mut sums: BTreeMap<(i32, i32), f64> = BTreeMap::new();
    for rec in t.to_vec() {
 *sums
            .entry((rec.source_type_id, rec.model_year_id))
            .or_insert(0.0) += rec.fuel_eng_fraction;
    }
    for ((st, my), sum) in &sums {
        assert!(
            (sum - 1.0).abs() < 1e-9,
            "sourceType={st} modelYear={my}: fractions sum to {sum:.9}, expected 1.0"
        );
    }
}
