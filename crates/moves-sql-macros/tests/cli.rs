//! Integration test for the `moves-sql-expand` binary.

use std::path::PathBuf;
use std::process::Command;

fn fixtures_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p
}

fn bin_path() -> PathBuf {
    // CARGO_BIN_EXE_<name> is set by cargo for integration tests when the
    // crate declares a [[bin]] target.
    PathBuf::from(env!("CARGO_BIN_EXE_moves-sql-expand"))
}

#[test]
fn cli_emits_expected_expansion_to_stdout() {
    let output = Command::new(bin_path())
        .arg("--script")
        .arg(fixtures_dir().join("sample.sql"))
        .arg("--config")
        .arg(fixtures_dir().join("sample.toml"))
        .output()
        .expect("failed to invoke moves-sql-expand");

    assert!(
        output.status.success(),
        "non-zero exit code; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    // Spot-check the lines we know should be there post-expansion.
    assert!(stdout.contains("CREATE TABLE foo (id INT, year INT);"));
    assert!(stdout.contains("INSERT INTO foo VALUES (1, 2030);"));
    assert!(stdout.contains("DROP TABLE bar21;"));
    assert!(stdout.contains("DROP TABLE bar31;"));
    // Sort order is lexicographic over strings — Java `TreeSet<String>`.
    // hourID values sort as ["10", "7", "8", "9"].
    assert!(stdout.contains("WHERE hourID in (10,7,8,9)"));
    assert!(stdout.contains("WHERE fuelTypeID in (1,2,9)"));
    // The disabled `Rates` section's body must not appear.
    assert!(
        !stdout.contains("INSERT INTO foo VALUES (2,"),
        "Rates section should be filtered out; stdout was:\n{stdout}"
    );
    // Marker for a disabled section must not appear either.
    assert!(
        !stdout.contains("-- Section Rates"),
        "Rates section marker should be filtered out; stdout was:\n{stdout}"
    );
}

#[test]
fn cli_writes_to_output_file_when_requested() {
    let tempdir = tempfile::tempdir().unwrap();
    let out_path = tempdir.path().join("expanded.sql");

    let output = Command::new(bin_path())
        .arg("--script")
        .arg(fixtures_dir().join("sample.sql"))
        .arg("--config")
        .arg(fixtures_dir().join("sample.toml"))
        .arg("--output")
        .arg(&out_path)
        .arg("--summary")
        .output()
        .expect("failed to invoke moves-sql-expand");
    assert!(output.status.success());
    // stdout should be empty when --output is set.
    assert!(output.stdout.is_empty());
    // Summary lands on stderr when --summary is passed.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("[moves-sql-expand]"), "stderr={stderr}");

    let written = std::fs::read_to_string(&out_path).unwrap();
    assert!(written.contains("DROP TABLE bar21;"));
}

#[test]
fn cli_fails_with_nonzero_on_missing_config() {
    let output = Command::new(bin_path())
        .arg("--script")
        .arg(fixtures_dir().join("sample.sql"))
        .arg("--config")
        .arg(fixtures_dir().join("does_not_exist.toml"))
        .output()
        .expect("failed to invoke moves-sql-expand");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("error:"), "stderr={stderr}");
}
