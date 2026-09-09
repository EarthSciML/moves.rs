//! `calculator-dag.json` registration counts are upper bounds, not sizes.
//!
//! The DAG is the denominator for coverage measurement in this repo and in
//! the sibling `moves.esm` port. A module's `registrations_count` comes from
//! the pinned MOVES tree's committed `CalculatorInfo.txt`, which EPA
//! generates by running each calculator's constructor against **a** MOVES
//! database and recording the `(pollutant, process)` pairs it registers. The
//! file does not record *which* database, and it was plainly not the
//! `movesdb20241112` this corpus runs against: it credits
//! `TOGSpeciationCalculator` with 184 registrations across the CB05 mechanism
//! pseudo-pollutants 1000–1018, none of which exist in the pinned database.
//!
//! So a registration count is an **upper bound against a database, not a
//! size**. This test measures the gap from the corpus rather than asserting
//! the framing in prose: it intersects every module's registrations with the
//! pollutant universe the canonical captures actually carry, and pins the two
//! numbers the finding turns on.
//!
//! Related: `characterization/fixtures/README.md` §"`calculator-dag.json`
//! over-reports TOGSpeciationCalculator", which records the in-SIF
//! measurement against `movesdb20241112` itself (116 pollutants, max 3000).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use moves_calculator_info::CalculatorDag;
use moves_framework::{DataFrameStore, DataFrameStoreParquet, InMemoryStore};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate manifest dir has a repo-root grandparent")
        .to_path_buf()
}

fn load_dag() -> CalculatorDag {
    let path = repo_root().join("characterization/calculator-chains/calculator-dag.json");
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// Every `pollutantID` the canonical captures carry — the pollutant universe
/// of the pinned default database, read back out of the corpus rather than
/// hard-coded.
fn corpus_pollutant_ids() -> BTreeSet<i32> {
    let snapshots = repo_root().join("characterization/snapshots");
    let mut ids = BTreeSet::new();
    let mut tables_seen = 0usize;
    for entry in std::fs::read_dir(&snapshots)
        .unwrap_or_else(|e| panic!("read {}: {e}", snapshots.display()))
        .filter_map(Result::ok)
    {
        let tables = entry.path().join("tables");
        let Ok(files) = std::fs::read_dir(&tables) else {
            continue;
        };
        let Some(path) = files.filter_map(Result::ok).map(|e| e.path()).find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("db__") && n.ends_with("__pollutant.parquet"))
        }) else {
            continue;
        };
        let file = std::io::BufReader::new(
            std::fs::File::open(&path).unwrap_or_else(|e| panic!("open {}: {e}", path.display())),
        );
        let mut store = InMemoryStore::new();
        store
            .read_parquet("pollutant", file)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let df = store.get("pollutant").expect("just inserted");
        let col = df
            .column("pollutantID")
            .expect("pollutant.pollutantID")
            .cast(&polars::prelude::DataType::Int32)
            .expect("pollutantID casts to Int32");
        ids.extend(col.i32().expect("Int32").into_iter().flatten());
        tables_seen += 1;
    }
    assert!(
        tables_seen >= 40,
        "expected the full capture corpus, found {tables_seen} `pollutant` tables"
    );
    ids
}

#[test]
fn registration_counts_are_upper_bounds_against_a_database() {
    let dag = load_dag();
    let universe = corpus_pollutant_ids();

    assert_eq!(
        universe.len(),
        116,
        "the pinned default database ships 116 pollutants"
    );
    assert_eq!(universe.iter().next_back(), Some(&3000));
    assert!(
        (1000..=1018).all(|p| !universe.contains(&p)),
        "the CB05 mechanism pollutants 1000–1018 are not in the pinned database"
    );

    let mut declared: BTreeMap<&str, usize> = BTreeMap::new();
    let mut in_universe: BTreeMap<&str, usize> = BTreeMap::new();
    for r in &dag.registrations {
        *declared.entry(r.calculator.as_str()).or_default() += 1;
        if universe.contains(&i32::try_from(r.pollutant_id).expect("pollutantID fits i32")) {
            *in_universe.entry(r.calculator.as_str()).or_default() += 1;
        }
    }

    // The headline: 184 declared, 12 reachable — a 15x over-report, and by
    // far the largest gap in the DAG.
    assert_eq!(declared["TOGSpeciationCalculator"], 184);
    assert_eq!(
        in_universe["TOGSpeciationCalculator"], 12,
        "only pollutant 88 survives, across rocSpeciation's 12 processes"
    );
    // Not the whole story, and the DAG cannot tell it: the constructor
    // re-derives its registrations at runtime from `pollutant × rocSpeciation`,
    // so against the pinned database it registers 24 pairs — pollutants 88 and
    // 3000 over the same 12 processes. 3000 is absent from
    // `CalculatorInfo.txt`'s list entirely. Either way, `TOGSpeciationCalculator.sql`
    // writes exactly one pollutant (88); the mechanism pollutant is a gate.

    // No module is credited with fewer registrations than it can reach.
    for (module, reachable) in &in_universe {
        assert!(
            reachable <= &declared[module],
            "{module}: {reachable} reachable > {} declared",
            declared[module]
        );
    }

    // And the artifact says so itself, so a consumer reading only the JSON
    // cannot mistake a count for a size.
    assert!(
        dag.source
            .registration_semantics
            .contains("upper bound against the database"),
        "calculator-dag.json must carry its registration semantics; found: {:?}",
        dag.source.registration_semantics
    );
}
