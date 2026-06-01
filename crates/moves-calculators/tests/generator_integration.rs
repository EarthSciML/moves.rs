//! Generator integration-validation harness — ().
//!
//! This integration test is the entry point of the generator
//! integration-validation gate. The harness machinery lives in the
//! [`generator_validation`] module tree (`tests/generator_validation/`);
//! this file wires it together and runs it.
//!
//! The tests below, in order:
//!
//! 1. pin the 23 onroad fixtures and confirm each parses;
//! 2. pin the 16 generators;
//! 3. confirm the coverage matrix reaches every fixture;
//! 4. route live `MeteorologyGenerator` output through the adapter
//! and the comparison engine — the harness machinery exercised on
//! real port output;
//! 5. confirm the diff engine catches a perturbed generator value;
//! 6. confirm the tolerance budget parses and carries the documented
//! `MeteorologyGenerator` divergence;
//! 7. validate against canonical snapshots when present — dormant
//! until the compute-node run populates them;
//! 8. print a harness-status banner.
//!
//! See `tests/generator_validation/mod.rs` for what runs today versus
//! what is gated behind the snapshot capture and the
//! data plane, and `characterization/generator-validation/README.md`
//! for the gate overview.

mod generator_validation;

use moves_snapshot::DiffOptions;

use generator_validation::compare::ValidationStatus;
use generator_validation::coverage::CoverageMatrix;
use generator_validation::{
    adapter, compare, fixtures, generators, snapshots_root, SNAPSHOTS_DIR_ENV,
};

/// The generator the harness has a live-port adapter for today. The
/// machinery is generator-agnostic; each remaining generator gets an
/// adapter as the data plane lands (see `adapter` module).
const LIVE_PORT_GENERATOR: &str = "MeteorologyGenerator";

#[test]
fn all_23_onroad_fixtures_present_and_parse() {
    let loaded = fixtures::load_all_fixtures()
        .unwrap_or_else(|e| panic!("the 23 onroad fixtures must load: {e}"));
    assert_eq!(loaded.len(), 23, "expected 23 onroad fixtures");

    for fixture in &loaded {
        assert!(
            fixture.is_onroad,
            "{} is not an ONROAD RunSpec",
            fixture.name
        );
        assert!(
            !fixture.process_ids.is_empty(),
            "{} exercises no emission process",
            fixture.name
        );
    }
}

#[test]
fn all_17_generators_registered() {
    let registered = generators::all_generators();
    assert_eq!(
        registered.len(),
        generators::GENERATOR_COUNT,
        "expected 17 generators (16 core + ProjectTAG)"
    );

    let names = generators::sorted_generator_names();
    let mut deduped = names.clone();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        names.len(),
        "duplicate generator name: {names:?}"
    );

 // Reading each generator's master-loop metadata must not panic.
    for generator in &registered {
        let _ = generator.subscriptions();
        let _ = generator.output_tables();
    }
}

#[test]
fn coverage_matrix_reaches_every_fixture() {
    let loaded = fixtures::load_all_fixtures().expect("fixtures must load");
    let registered = generators::all_generators();
    let matrix = CoverageMatrix::build(&loaded, &registered);

    for fixture in matrix.fixture_names() {
        assert!(
            !matrix.generators_for_fixture(fixture).is_empty(),
            "fixture `{fixture}` exercises no generator — untestable by this gate"
        );
    }
    assert!(
        matrix.exercised_pair_count() > 0,
        "the coverage matrix is empty"
    );
}

#[test]
fn harness_composes_with_live_meteorology_output() {
 // Run the real MeteorologyGenerator numeric compute core over the
 // sample grid and shape its output into a snapshot table.
    let cells = adapter::sample_cells();
    let produced = adapter::run_meteorology(&cells).expect("meteorology table builds");
    assert_eq!(produced.row_count(), cells.len());
    assert_eq!(produced.name(), adapter::ZONE_MONTH_HOUR_TABLE);

 // Plumbing check: live port output routed through the comparison
 // engine against itself shows no divergence. This exercises
 // adapter → compare_table on genuine `moves-calculators` output.
 // It is *not* a fidelity check against canonical MOVES — that
 // needs the snapshots (see the dormant test below).
    let diff = compare::compare_table(&produced, &produced, &DiffOptions::default())
        .expect("self-comparison runs");
    assert!(diff.is_empty(), "self-comparison must be empty:\n{diff:?}");
}

#[test]
fn diff_engine_catches_a_perturbed_generator_value() {
    let cells = adapter::sample_cells();
    let produced = adapter::run_meteorology(&cells).expect("table builds");

 // Recompute with one cell's temperature perturbed well beyond any
 // tolerance budget; the diff must report the resulting divergence.
    let mut perturbed_cells = cells.clone();
    perturbed_cells[2].inputs.temperature_f += 5.0;
    let perturbed = adapter::run_meteorology(&perturbed_cells).expect("table builds");

    let diff =
        compare::compare_table(&perturbed, &produced, &DiffOptions::default()).expect("diff runs");
    assert!(
        !diff.is_empty(),
        "a perturbed generator value must surface as a divergence"
    );
    assert!(diff.summary().cells_changed > 0);
}

#[test]
fn tolerance_budget_parses_and_carries_the_meteorology_exception() {
    let opts = compare::tolerance_options()
        .unwrap_or_else(|e| panic!("the committed tolerance.toml must parse: {e}"));

 // The budget records the one *expected* divergence the generator
 // port already documents: MeteorologyGenerator routes
 // specificHumidity / molWaterFraction through fahrenheit_to_kelvin,
 // whose exact 5.0/9.0 ratio differs ~8e-6 relative from MariaDB's
 // (5/9). Those columns must be widened past the strict default.
    for column in ["specificHumidity", "molWaterFraction"] {
        let key = ("ZoneMonthHour".to_string(), column.to_string());
        let tol = opts.per_column_tolerance.get(&key).copied();
        assert!(
            tol.is_some_and(|t| t > opts.default_float_tolerance),
            "tolerance.toml must widen ZoneMonthHour.{column} past the default"
        );
    }
}

#[test]
fn canonical_snapshots_validate_when_present() {
    let root = snapshots_root();
    let loaded = fixtures::load_all_fixtures().expect("fixtures must load");
    let opts = compare::tolerance_options().expect("tolerance budget parses");

 // The live-port adapter exists for MeteorologyGenerator today;
 // build its output once and validate it for every fixture that
 // exercises the generator.
    let produced =
        adapter::run_meteorology(&adapter::sample_cells()).expect("meteorology table builds");
    let matrix = CoverageMatrix::build(&loaded, &generators::all_generators());

    let mut dormant = 0usize;
    let mut active = 0usize;
    for fixture in &loaded {
        if !matrix
            .generators_for_fixture(&fixture.name)
            .contains(&LIVE_PORT_GENERATOR)
        {
            continue;
        }
        let result =
            compare::validate_table(&root, &fixture.name, LIVE_PORT_GENERATOR, &produced, &opts)
                .unwrap_or_else(|e| panic!("validate_table errored for {}: {e}", fixture.name));
        match result.status {
            ValidationStatus::Dormant => dormant += 1,
            _ => active += 1,
        }
    }

    if active == 0 {
        eprintln!(
            "generator-validation gate: DORMANT — no canonical snapshots under {}. \
             Populate characterization/snapshots/<fixture>/ (or point {} at a capture \
             run) to activate. See characterization/generator-validation/README.md.",
            root.display(),
            SNAPSHOTS_DIR_ENV,
        );
        assert!(
            dormant > 0,
            "{LIVE_PORT_GENERATOR} must be exercised by at least one fixture"
        );
    } else {
 // The harness loaded the snapshots and ran the diff without
 // error — the dormant → active transition works. Divergences
 // are not asserted on here: until the data plane lands,
 // `produced` is the adapter's synthetic sample grid, not the
 // fixture's real generator output, so a mismatch is expected.
 // The pass/fail-on-divergence gate tightens when that wiring
 // replaces the sample grid with per-fixture output.
        eprintln!("generator-validation gate: ACTIVE — {active} fixture(s) loaded and diffed.");
    }
}

#[test]
fn harness_status() {
 // An always-on status line, visible under `cargo test -- --nocapture`.
    let loaded = fixtures::load_all_fixtures().expect("fixtures must load");
    let matrix = CoverageMatrix::build(&loaded, &generators::all_generators());

    eprintln!("── Generator integration-validation harness ──");
    eprintln!(
        "  onroad fixtures:   {}",
        fixtures::ONROAD_FIXTURE_NAMES.len()
    );
    eprintln!("  generators:        {}", generators::GENERATOR_COUNT);
    eprintln!(
        "  coverage cells:    {} exercised (fixture × generator)",
        matrix.exercised_pair_count()
    );
    match std::env::var_os(SNAPSHOTS_DIR_ENV) {
        Some(_) => eprintln!(
            "  snapshots root:    {} (from {SNAPSHOTS_DIR_ENV})",
            snapshots_root().display()
        ),
        None => eprintln!(
            "  snapshots root:    {} (in-repo default — gate dormant until populated)",
            snapshots_root().display()
        ),
    }
    eprintln!("\n{}", matrix.render_markdown());
}
