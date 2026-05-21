//! Calculator integration-validation harness — Task 73 (`mo-fvuf`).
//!
//! This integration test is the entry point of the calculator
//! integration-validation gate. The harness machinery lives in the
//! [`calculator_validation`] module tree
//! (`tests/calculator_validation/`); this file wires it together and
//! runs it.
//!
//! The tests below, in order:
//!
//! 1. pin the 23 Phase 0 onroad fixtures and confirm each parses;
//! 2. pin the 37 Phase 3 hot-path calculators (Tasks 45–72);
//! 3. confirm the coverage matrix reaches every fixture;
//! 4. confirm the diff engine catches a perturbed calculator value;
//! 5. confirm the tolerance budget parses (no expected divergences yet);
//! 6. validate against canonical snapshots when present — dormant
//!    until the Phase 0 compute-node run populates them;
//! 7. print a harness-status banner.
//!
//! See `tests/calculator_validation/mod.rs` for what runs today versus
//! what is gated behind the Phase 0 snapshot capture and the data
//! plane, and `characterization/calculator-validation/README.md` for
//! the gate overview.

mod calculator_validation;

use moves_snapshot::{ColumnKind, TableBuilder, Value};
use moves_snapshot::table::Table;

use calculator_validation::coverage::CoverageMatrix;
use calculator_validation::{calculators, compare, fixtures, snapshots_root, SNAPSHOTS_DIR_ENV};

fn make_table(name: &str, rows: &[(i64, f64)]) -> Table {
    let mut builder = TableBuilder::new(
        name,
        [
            ("id".to_string(), ColumnKind::Int64),
            ("value".to_string(), ColumnKind::Float64),
        ],
    )
    .unwrap()
    .with_natural_key(["id"])
    .unwrap();
    for &(id, value) in rows {
        builder.push_row([Value::Int64(id), Value::Float64(value)]).unwrap();
    }
    builder.build().unwrap()
}

#[test]
fn all_23_onroad_fixtures_present_and_parse() {
    let loaded = fixtures::load_all_fixtures()
        .unwrap_or_else(|e| panic!("the 23 Phase 0 onroad fixtures must load: {e}"));
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
        assert!(
            !fixture.ppa_ids.is_empty(),
            "{} has no (pollutant, process) pairs",
            fixture.name
        );
    }
}

#[test]
fn all_37_calculators_registered() {
    let registered = calculators::all_calculators();
    assert_eq!(
        registered.len(),
        calculators::CALCULATOR_COUNT,
        "expected {} Phase 3 calculators, got {}",
        calculators::CALCULATOR_COUNT,
        registered.len()
    );

    let names = calculators::sorted_calculator_names();
    let mut deduped = names.clone();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        names.len(),
        "duplicate calculator name(s): {names:?}"
    );
}

#[test]
fn coverage_matrix_reaches_every_fixture() {
    let loaded_fixtures =
        fixtures::load_all_fixtures().expect("the 23 onroad fixtures must load");
    let calcs = calculators::all_calculators();
    let matrix = CoverageMatrix::build(&loaded_fixtures, &calcs);

    assert!(
        matrix.every_fixture_has_coverage(),
        "at least one fixture has no exercised or chained-only calculator — \
         check fixture (pollutant, process) pairs against calculator registrations"
    );
}

#[test]
fn coverage_matrix_every_calculator_covered() {
    // Calculators whose (pollutant, process) registrations are not exercised
    // by any of the 23 Phase 0 onroad hot-path fixtures.
    //
    // These are known gaps — they register for pollutant IDs (speciation
    // products, NOx fractions, CO2AE process 90) that the current fixture
    // set does not cover by (pollutant, process) pair:
    //
    //   TOGSpeciationCalculator     — registers for TOG-speciation product
    //                                 pollutant IDs (1000+, 88, …); fixtures
    //                                 use the input TOG pollutant 86.
    //   NOCalculator / NO2Calculator — registers for NOx fractional pollutants
    //                                 not present in the fixture PPAs.
    //   CO2AERunningStartExtended…  — registers for process 90 (Extended Idle),
    //                                 which no Phase 0 fixture selects.
    //
    // Remove an entry from this list when a fixture is added that covers it.
    const KNOWN_UNCOVERED: &[&str] = &[
        "CO2AERunningStartExtendedIdleCalculator",
        "NOCalculator",
        "NO2Calculator",
        "TOGSpeciationCalculator",
    ];

    let loaded_fixtures =
        fixtures::load_all_fixtures().expect("the 23 onroad fixtures must load");
    let calcs = calculators::all_calculators();
    let matrix = CoverageMatrix::build(&loaded_fixtures, &calcs);

    let uncovered: Vec<&str> = matrix
        .calculator_names()
        .iter()
        .enumerate()
        .filter(|&(ci, _)| {
            !loaded_fixtures.iter().enumerate().any(|(fi, _)| {
                matrix.cell(fi, ci).kind.is_exercised_or_chained()
            })
        })
        .map(|(_, name)| name.as_str())
        .collect();

    let unexpected: Vec<&str> = uncovered
        .iter()
        .copied()
        .filter(|name| !KNOWN_UNCOVERED.contains(name))
        .collect();

    assert!(
        unexpected.is_empty(),
        "{} calculator(s) unexpectedly not covered by any fixture:\n  {}\n\
         If this is intentional, add them to KNOWN_UNCOVERED in this test \
         with an explanation. Otherwise verify registrations match fixture PPAs.",
        unexpected.len(),
        unexpected.join("\n  ")
    );

    // Print a note if any known-uncovered calculator has gained coverage so
    // the KNOWN_UNCOVERED list can be kept tidy.
    let now_covered: Vec<&str> = KNOWN_UNCOVERED
        .iter()
        .copied()
        .filter(|&name| !uncovered.contains(&name))
        .collect();
    if !now_covered.is_empty() {
        println!(
            "NOTE: {} formerly-uncovered calculator(s) are now covered — \
             remove from KNOWN_UNCOVERED:\n  {}",
            now_covered.len(),
            now_covered.join("\n  ")
        );
    }
}

#[test]
fn diff_engine_detects_perturbed_value() {
    let canonical = make_table("MOVESOutput", &[(1, 10.0), (2, 20.0)]);
    let perturbed = make_table("MOVESOutput", &[(1, 10.0), (2, 99.9)]);

    let opts = compare::tolerance_options().expect("tolerance.toml must parse");
    let diff = compare::compare_table(&perturbed, &canonical, &opts)
        .expect("compare_table must not fail");

    assert_eq!(
        diff.summary().cells_changed,
        1,
        "diff engine must detect the perturbed cell"
    );
}

#[test]
fn diff_engine_passes_identical_tables() {
    let canonical = make_table("MOVESOutput", &[(1, 10.0), (2, 20.0)]);
    let produced = make_table("MOVESOutput", &[(1, 10.0), (2, 20.0)]);

    let opts = compare::tolerance_options().expect("tolerance.toml must parse");
    let diff = compare::compare_table(&produced, &canonical, &opts)
        .expect("compare_table must not fail");

    assert!(diff.is_empty(), "identical tables must produce no diff");
}

#[test]
fn tolerance_budget_parses() {
    let opts = compare::tolerance_options();
    assert!(
        opts.is_ok(),
        "characterization/calculator-validation/tolerance.toml must parse: {:?}",
        opts
    );
}

#[test]
fn canonical_snapshots_dormant_or_validate() {
    let snapshots = snapshots_root();
    let loaded_fixtures =
        fixtures::load_all_fixtures().expect("the 23 onroad fixtures must load");

    let mut dormant_count = 0usize;
    let mut validated_count = 0usize;
    let mut failed: Vec<String> = Vec::new();

    for fixture in &loaded_fixtures {
        if !compare::canonical_snapshot_present(&snapshots, &fixture.name) {
            dormant_count += 1;
            continue;
        }
        let snap_dir = snapshots.join(&fixture.name);
        match moves_snapshot::Snapshot::load(&snap_dir) {
            Ok(snap) => {
                let table_count = snap.tables().count();
                if table_count == 0 {
                    failed.push(format!(
                        "{}: snapshot present but contains no tables",
                        fixture.name
                    ));
                } else {
                    validated_count += 1;
                }
            }
            Err(e) => {
                failed.push(format!("{}: snapshot load failed: {e}", fixture.name));
            }
        }
    }

    assert!(
        failed.is_empty(),
        "snapshot validation failed for {} fixture(s):\n{}",
        failed.len(),
        failed.join("\n")
    );

    let _ = (dormant_count, validated_count);
}

#[test]
fn harness_status() {
    let snapshots = snapshots_root();
    let loaded_fixtures =
        fixtures::load_all_fixtures().expect("the 23 onroad fixtures must load");
    let calcs = calculators::all_calculators();
    let matrix = CoverageMatrix::build(&loaded_fixtures, &calcs);

    let populated_snapshots = loaded_fixtures
        .iter()
        .filter(|f| compare::canonical_snapshot_present(&snapshots, &f.name))
        .count();

    println!();
    println!("=== Calculator integration-validation harness (Task 73) ===");
    println!("  Fixtures    : {}", loaded_fixtures.len());
    println!("  Calculators : {}", calcs.len());
    println!(
        "  Snapshots   : {}/{} populated ({}={})",
        populated_snapshots,
        loaded_fixtures.len(),
        SNAPSHOTS_DIR_ENV,
        std::env::var(SNAPSHOTS_DIR_ENV).unwrap_or_else(|_| snapshots.display().to_string())
    );
    println!();
    println!("{}", matrix.render());
    println!("  Status: machinery validated; canonical-capture diff dormant until");
    println!("          Phase 0 compute-node run + data plane (see README).");
    println!("===========================================================");
}
