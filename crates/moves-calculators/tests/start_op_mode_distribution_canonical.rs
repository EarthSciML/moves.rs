//! Canonical-capture gate for `StartOperatingModeDistributionGenerator`.
//!
//! Runs the real generator against the real canonical-MOVES captures under
//! `characterization/snapshots/` and diffs both of its outputs cell by cell:
//!
//! * **step 300** — `StartOpModeDistribution`, the soak fractions computed
//!   from `SampleVehicleTrip`. 124 rows across the corpus.
//! * **step 400** — `RatesOpModeDistribution`, the copy of the
//!   default-database `startsOpModeDistribution` table.
//!
//! Nine of the corpus's 42 traces run this generator; the other 33 never
//! instantiate it (their `execution-trace.json` does not name it) and have no
//! `startOpModeDistribution` capture. [`FIXTURES`] is that list, and the test
//! asserts every entry is present so a recapture that drops one is a failure
//! rather than a silent skip.
//!
//! # Why this gate exists
//!
//! Before the fix behind it, step 400 was modelled as a copy of the step-300
//! soak fractions. On `sample-runspec` that put `opModeFraction` **0.4444**
//! where canonical MOVES has **0.124754** — the two tables are built from
//! different inputs and are not interchangeable.
//!
//! # Comparing a `FLOAT` column
//!
//! `RatesOpModeDistribution.opModeFraction` is `FLOAT`
//! (`database/CreateExecutionRates.sql`), so the captured value is the
//! single-precision narrowing of the `DOUBLE` source, rendered by MySQL to
//! six significant digits. The port carries `f64` throughout, so the
//! comparison narrows to `f32` and renders to six significant digits before
//! comparing — that is the column's storage, not a tolerance. `f64` alone
//! is not enough: `startsOpModeDistribution` holds 0.071701249, whose
//! six-digit rendering is 0.0717012, while the capture (via `FLOAT`) holds
//! 0.0717013.
//!
//! `StartOpModeDistribution.opModeFraction` is a four-decimal `DECIMAL`
//! (see `op_mode_fraction`) and is compared **exactly**.
//!
//! Both comparisons are corpus-revision independent. Narrowing the canonical
//! value to `f32` before rendering is a no-op when the capture already holds
//! the full single-precision value, so the step-400 diff survives a recapture
//! that stops rounding significant digits away; and the row counts below are
//! floors rather than pins, because how many rows a fixture contributes is a
//! property of its RunSpec, not of the port. Both were re-derived against the
//! `moves-snapshot/v2` recapture (84 step-300 rows, 159 step-400 rows) and
//! match there too.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use moves_calculators::generators::start_operating_mode_distribution::{
    RatesOpModeDistributionRow, StartOpModeDistributionRow,
    StartOperatingModeDistributionGenerator, ALL_STARTS_OP_MODE_ID,
};
use moves_framework::{
    CalculatorContext, DataFrameStoreParquet, DataFrameStoreTyped, Generator, InMemoryStore,
};

/// The nine onroad fixtures whose canonical execution trace runs
/// `StartOperatingModeDistributionGenerator`.
const FIXTURES: [&str; 9] = [
    "expand-counties",
    "expand-criteria",
    "expand-day",
    "expand-fueltype-diesel",
    "expand-month",
    "expand-sourcetype",
    "mixed-onroad",
    "process-refueling",
    "sample-runspec",
];

/// The store table name each snapshot table is loaded under, and the
/// lowercase MySQL table name the capture files are keyed by.
const INPUTS: [(&str, &str); 7] = [
    ("SampleVehicleTrip", "samplevehicletrip"),
    ("SampleVehicleDay", "samplevehicleday"),
    ("OperatingMode", "operatingmode"),
    ("RunSpecHourDay", "runspechourday"),
    ("RunSpecSourceType", "runspecsourcetype"),
    ("RunSpecPollutantProcess", "runspecpollutantprocess"),
    ("startsOpModeDistribution", "startsopmodedistribution"),
];

fn snapshots_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate manifest dir has a repo-root grandparent")
        .join("characterization")
        .join("snapshots")
}

/// Locate `db__<execution database>__<table>.parquet` inside a capture. The
/// database segment carries the capture host's name, so it is matched by
/// suffix rather than spelled out.
fn table_path(fixture: &str, table: &str) -> Option<PathBuf> {
    let dir = snapshots_root().join(fixture).join("tables");
    let suffix = format!("__{table}.parquet");
    std::fs::read_dir(&dir)
        .ok()?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("db__") && n.ends_with(&suffix))
        })
}

/// Read one capture table into `store` under `store_name`.
fn load_table(store: &mut InMemoryStore, fixture: &str, snapshot_name: &str, store_name: &str) {
    let path = table_path(fixture, snapshot_name)
        .unwrap_or_else(|| panic!("{fixture}: no capture of `{snapshot_name}`"));
    let file = std::io::BufReader::new(
        std::fs::File::open(&path).unwrap_or_else(|e| panic!("open {}: {e}", path.display())),
    );
    store
        .read_parquet(store_name, file)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
}

/// A one-table store holding a capture table under its canonical name — the
/// canonical side of a diff, extracted through the same typed reader as the
/// port's own output.
fn canonical_store(fixture: &str, snapshot_name: &str, store_name: &str) -> InMemoryStore {
    let mut store = InMemoryStore::new();
    load_table(&mut store, fixture, snapshot_name, store_name);
    store
}

/// The `FLOAT` column round-trip: narrow to single precision, then render to
/// the six significant digits MySQL prints for a `FLOAT`.
fn as_stored_float(x: f64) -> String {
    format!("{:.5e}", f64::from(x as f32))
}

/// Run the generator over one capture's input tables and return its scratch
/// output.
fn run_generator(fixture: &str) -> CalculatorContext {
    let mut store = InMemoryStore::new();
    for (store_name, snapshot_name) in INPUTS {
        load_table(&mut store, fixture, snapshot_name, store_name);
    }
    let mut ctx = CalculatorContext::with_tables(store);
    StartOperatingModeDistributionGenerator
        .execute(&mut ctx)
        .unwrap_or_else(|e| panic!("{fixture}: execute failed: {e}"));
    ctx
}

#[test]
fn every_generator_fixture_has_a_capture() {
    for fixture in FIXTURES {
        assert!(
            snapshots_root()
                .join(fixture)
                .join("manifest.json")
                .is_file(),
            "no canonical capture for `{fixture}` under {}",
            snapshots_root().display()
        );
    }
}

#[test]
fn step_300_start_op_mode_distribution_matches_canonical() {
    let mut total_rows = 0usize;
    for fixture in FIXTURES {
        let ctx = run_generator(fixture);
        let produced: Vec<StartOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("StartOpModeDistribution")
            .expect("StartOpModeDistribution in scratch");

        let canonical: Vec<StartOpModeDistributionRow> = canonical_store(
            fixture,
            "startopmodedistribution",
            "StartOpModeDistribution",
        )
        .iter_typed("StartOpModeDistribution")
        .expect("canonical StartOpModeDistribution extracts");

        let key = |r: &StartOpModeDistributionRow| (r.source_type_id, r.hour_day_id, r.op_mode_id);
        let expected: BTreeMap<_, f64> = canonical
            .iter()
            .map(|r| (key(r), r.op_mode_fraction))
            .collect();
        let actual: BTreeMap<_, f64> = produced
            .iter()
            .map(|r| (key(r), r.op_mode_fraction))
            .collect();

        assert_eq!(
            actual.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>(),
            "{fixture}: StartOpModeDistribution (sourceTypeID, hourDayID, opModeID) set differs"
        );
        assert!(
            !expected.is_empty(),
            "{fixture}: canonical StartOpModeDistribution is empty — this \
             capture cannot exercise the gate"
        );
        for (k, want) in &expected {
            let got = actual[k];
            // A four-decimal DECIMAL — exact, no tolerance.
            assert_eq!(
                got, *want,
                "{fixture}: StartOpModeDistribution{k:?} opModeFraction {got} != canonical {want}"
            );
        }
        total_rows += expected.len();
    }
    // A floor, not a pin: the row count is a property of the corpus revision,
    // not of the port. `moves-snapshot/v1` holds 124 rows; the v2 recapture
    // corrects each fixture's day selection, so every fixture covers one
    // hour-day instead of two and the corpus holds 84. Both are diffed
    // row-for-row above; this only stops the gate passing vacuously.
    assert!(
        total_rows >= 80,
        "only {total_rows} StartOpModeDistribution rows compared across \
         {} fixtures — the corpus looks truncated",
        FIXTURES.len()
    );
}

#[test]
fn step_400_rates_op_mode_distribution_matches_canonical() {
    let mut total_rows = 0usize;
    for fixture in FIXTURES {
        let ctx = run_generator(fixture);
        let produced: Vec<RatesOpModeDistributionRow> = ctx
            .scratch()
            .store
            .iter_typed("RatesOpModeDistribution")
            .expect("RatesOpModeDistribution in scratch");

        let canonical: Vec<RatesOpModeDistributionRow> = canonical_store(
            fixture,
            "ratesopmodedistribution",
            "RatesOpModeDistribution",
        )
        .iter_typed("RatesOpModeDistribution")
        .expect("canonical RatesOpModeDistribution extracts");

        let key = |r: &RatesOpModeDistributionRow| {
            (
                r.source_type_id,
                r.pol_process_id,
                r.road_type_id,
                r.hour_day_id,
                r.op_mode_id,
                r.avg_speed_bin_id,
            )
        };
        let actual: BTreeMap<_, f64> = produced
            .iter()
            .map(|r| (key(r), r.op_mode_fraction))
            .collect();
        // `RatesOpModeDistribution` is shared: other generators contribute
        // their own processes (e.g. extended idle, polProcessID 9190 / op mode
        // 200 in `expand-sourcetype`). Restrict the canonical side to the rows
        // this generator owns — the start op-mode bands 101+ and the "All
        // Starts" row.
        let owned = |r: &RatesOpModeDistributionRow| {
            r.op_mode_id == i32::from(ALL_STARTS_OP_MODE_ID) || (101..=150).contains(&r.op_mode_id)
        };
        let expected: BTreeMap<_, f64> = canonical
            .iter()
            .filter(|r| owned(r))
            .map(|r| (key(r), r.op_mode_fraction))
            .collect();

        assert_eq!(
            actual.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>(),
            "{fixture}: RatesOpModeDistribution primary-key set differs"
        );
        assert!(
            !expected.is_empty(),
            "{fixture}: canonical RatesOpModeDistribution holds no start rows \
             — this capture cannot exercise the gate"
        );
        for (k, want) in &expected {
            let got = actual[k];
            assert_eq!(
                as_stored_float(got),
                as_stored_float(*want),
                "{fixture}: RatesOpModeDistribution{k:?} opModeFraction {got} != canonical {want}"
            );
        }
        total_rows += expected.len();
    }
    // A floor, not a pin — see the step-300 gate. `moves-snapshot/v1` holds
    // 282 start rows (284 across the nine traces, less `expand-sourcetype`'s
    // two extended-idle rows at polProcessID 9190 / op mode 200); the v2
    // recapture holds 159.
    assert!(
        total_rows >= 150,
        "only {total_rows} start RatesOpModeDistribution rows compared across \
         {} fixtures — the corpus looks truncated",
        FIXTURES.len()
    );
}

#[test]
fn step_400_does_not_copy_the_step_300_fractions() {
    // The regression this suite exists for, stated as a value: on
    // `sample-runspec` the step-300 soak fraction for source type 21 /
    // hour-day 72 / op mode 101 is 0.4444 and the step-400 fraction for the
    // same cell is 0.124754. A port that copies the former into the latter
    // emits 0.4444 where MOVES emits 0.124754.
    let ctx = run_generator("sample-runspec");
    let soak: Vec<StartOpModeDistributionRow> = ctx
        .scratch()
        .store
        .iter_typed("StartOpModeDistribution")
        .expect("StartOpModeDistribution in scratch");
    let rates: Vec<RatesOpModeDistributionRow> = ctx
        .scratch()
        .store
        .iter_typed("RatesOpModeDistribution")
        .expect("RatesOpModeDistribution in scratch");

    let soak_101 = soak
        .iter()
        .find(|r| r.source_type_id == 21 && r.hour_day_id == 72 && r.op_mode_id == 101)
        .expect("step-300 row for 21/72/101");
    assert_eq!(soak_101.op_mode_fraction, 0.4444);

    let rates_101 = rates
        .iter()
        .find(|r| {
            r.source_type_id == 21
                && r.hour_day_id == 72
                && r.op_mode_id == 101
                && r.pol_process_id == 9102
        })
        .expect("step-400 row for 21/72/101/9102");
    assert_eq!(as_stored_float(rates_101.op_mode_fraction), "1.24754e-1");
}
