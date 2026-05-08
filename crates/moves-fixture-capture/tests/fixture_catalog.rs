//! Integration test: every committed fixture in `characterization/fixtures/`
//! must parse via `RunSpec::from_file` and produce a unique fixture name.
//!
//! This guards the Phase 0 Task 5 + Task 6 acceptance: the fixture set is
//! the regression baseline every other phase verifies against, so a fixture
//! file that doesn't even round-trip through the host-side parser is a
//! regression in itself.

use std::collections::HashSet;
use std::path::PathBuf;

use moves_fixture_capture::runspec::RunSpec;

fn fixtures_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("crate is two levels under workspace root")
        .join("characterization/fixtures")
}

#[test]
fn fixture_catalog_parses_and_is_unique() {
    let dir = fixtures_dir();
    assert!(
        dir.is_dir(),
        "expected fixtures dir at {} — Phase 0 Task 5/6 deliverable missing",
        dir.display()
    );

    let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read characterization/fixtures")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "xml"))
        .collect();
    entries.sort();

    assert!(
        entries.len() >= 30,
        "fixture catalogue is below Phase 0 Task 5/6 floor of 30: got {}",
        entries.len()
    );
    assert!(
        entries.len() <= 35,
        "fixture catalogue is above Phase 0 Task 5/6 ceiling of 35: got {}",
        entries.len()
    );

    let mut names = HashSet::new();
    let mut output_dbs = HashSet::new();
    let mut sanitized_paths = HashSet::new();
    for path in &entries {
        let runspec = RunSpec::from_file(path)
            .unwrap_or_else(|err| panic!("failed to parse {}: {err}", path.display()));

        assert!(
            !runspec.fixture_name.is_empty(),
            "fixture {} produced empty fixture_name",
            path.display()
        );
        let stem_lower = path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_ascii_lowercase())
            .unwrap_or_default();
        assert_eq!(
            runspec.fixture_name,
            stem_lower,
            "fixture name should round-trip to lowercased filename stem for {}",
            path.display()
        );

        assert!(
            names.insert(runspec.fixture_name.clone()),
            "duplicate fixture_name {} (collision between filenames)",
            runspec.fixture_name
        );
        assert!(
            sanitized_paths.insert(stem_lower),
            "duplicate sanitized stem for {}",
            path.display()
        );

        assert!(
            !runspec.output_database.is_empty(),
            "fixture {} has empty <outputdatabase databasename> — capture pipeline \
             will reject this RunSpec at run time",
            path.display()
        );
        if !output_dbs.insert(runspec.output_database.clone()) {
            // sample-runspec.xml uses JUnitTestOutput (the canonical name);
            // every other fixture must pick a unique name so concurrent runs
            // don't write to the same MariaDB schema.
            panic!(
                "duplicate <outputdatabase databasename={}> in {}",
                runspec.output_database,
                path.display()
            );
        }
    }
}
