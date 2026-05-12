//! End-to-end pipeline test: feed a synthetic MOVES-style SQL fixture
//! through the macro expander and the section-marker preprocessor and
//! confirm the output matches the canonical Java behaviour byte-for-byte.

use moves_sql_macros::{process_sections, ExpandConfig};

fn fixtures_dir() -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p
}

#[test]
fn sample_fixture_expands_to_expected_sql() {
    let cfg = ExpandConfig::load(&fixtures_dir().join("sample.toml")).unwrap();
    let expander = cfg.build_expander().unwrap();
    let script = std::fs::read_to_string(fixtures_dir().join("sample.sql")).unwrap();
    let raw_lines: Vec<String> = script.lines().map(|s| s.to_string()).collect();

    // Stage 1: macro-expand each line.
    let mut macro_expanded = Vec::with_capacity(raw_lines.len());
    for line in &raw_lines {
        expander.expand_and_add(line, &mut macro_expanded);
    }

    // Stage 2: section-filter with the configured enabled sections + the
    // configured replacements.
    let enabled: Vec<&str> = cfg.enabled_sections.iter().map(String::as_str).collect();
    let repl = cfg.replacement_pairs();
    let out = process_sections(&macro_expanded, &enabled, &repl);

    // Expected output, traced by hand from the fixture + config:
    // * Top-level (non-section) comment lines pass through as-is — they
    //   are not section markers, and the outermost section state is
    //   enabled.
    // * "Create Remote Tables for Extracted Data" is enabled.
    //   * "Inventory" is enabled -> kept (year=2030).
    //   * "Rates" is NOT enabled -> dropped.
    //   * `bar##macro.sourceTypeID##` expands to 2 lines (21, 31).
    // * "Extract Data" enabled -> kept. hourID values sort
    //   lexicographically per Java's `TreeSet<String>`: ["10","7","8","9"].
    // * "Cleanup" enabled -> kept, fuelTypeID expands to 1,2,9.
    let expected = vec![
        "-- Synthetic MOVES-style SQL fixture exercising the macro expander and".to_string(),
        "-- section-marker preprocessor end-to-end. The shape is modelled on".to_string(),
        "-- ActivityCalculator.sql + BaseRateCalculator.sql but reduced to the".to_string(),
        "-- minimum that exercises every interesting code path.".to_string(),
        "-- Section Create Remote Tables for Extracted Data".to_string(),
        "CREATE TABLE foo (id INT, year INT);".to_string(),
        "-- Section Inventory".to_string(),
        "INSERT INTO foo VALUES (1, 2030);".to_string(),
        "-- End Section Inventory".to_string(),
        "DROP TABLE bar21;".to_string(),
        "DROP TABLE bar31;".to_string(),
        "-- End Section Create Remote Tables for Extracted Data".to_string(),
        "-- Section Extract Data".to_string(),
        "SELECT * FROM zonemonthhour WHERE hourID in (10,7,8,9);".to_string(),
        "-- End Section Extract Data".to_string(),
        "-- Section Cleanup".to_string(),
        "DELETE FROM scratch WHERE fuelTypeID in (1,2,9);".to_string(),
        "-- End Section Cleanup".to_string(),
    ];

    assert_eq!(out.lines, expected);
    // Five `-- Section` markers in the fixture, one (Rates) disabled.
    assert_eq!(out.sections_seen, 5);
    assert_eq!(out.sections_kept, 4);
    assert_eq!(out.sections_dropped, 1);
}

#[test]
fn fixture_is_deterministic_across_runs() {
    // Determinism: same fixture + same config -> byte-identical output.
    let cfg = ExpandConfig::load(&fixtures_dir().join("sample.toml")).unwrap();
    let script = std::fs::read_to_string(fixtures_dir().join("sample.sql")).unwrap();
    let raw_lines: Vec<String> = script.lines().map(|s| s.to_string()).collect();
    let enabled: Vec<&str> = cfg.enabled_sections.iter().map(String::as_str).collect();
    let repl = cfg.replacement_pairs();

    let run = || {
        let expander = cfg.build_expander().unwrap();
        let mut macro_expanded = Vec::new();
        for line in &raw_lines {
            expander.expand_and_add(line, &mut macro_expanded);
        }
        process_sections(&macro_expanded, &enabled, &repl).lines
    };

    let first = run();
    let second = run();
    let third = run();
    assert_eq!(first, second);
    assert_eq!(second, third);
}
