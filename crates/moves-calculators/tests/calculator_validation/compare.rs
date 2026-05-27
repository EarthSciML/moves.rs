//! Diff a calculator's produced table against a canonical snapshot.
//!
//! The comparison reuses the Phase 0 [`moves_snapshot`] crate — the
//! canonical-MOVES captures *are* `moves_snapshot`-format snapshots,
//! so the gate diffs with the same engine Phase 0's own
//! regression-detection uses rather than reinventing one.
//!
//! Two layers:
//!
//! * [`compare_table`] is the pure, in-memory machinery — diff one
//!   produced [`Table`] against one canonical [`Table`] under a
//!   tolerance budget.
//! * [`validate_table`] is the disk-aware orchestration — locate the
//!   fixture's canonical snapshot, resolve the table matching the
//!   calculator's output, and run [`compare_table`]. With no snapshots
//!   in the repo it reports [`ValidationStatus::Dormant`].

use std::path::Path;

use moves_snapshot::{
    diff_snapshots, Diff, DiffOptions, DiffSummary, Snapshot, Table, ToleranceConfig,
    ToleranceError,
};
use serde::Serialize;

const MANIFEST_FILE: &str = "manifest.json";

/// Load the tolerance budget from `characterization/calculator-validation/`.
///
/// # Errors
///
/// Returns [`ToleranceError`] when the committed file is missing or malformed.
pub fn tolerance_options() -> Result<DiffOptions, ToleranceError> {
    let path = super::tolerance_config_path();
    let config = ToleranceConfig::from_file(&path)?;
    Ok(config.into())
}

/// Whether a populated canonical snapshot exists for `fixture`.
pub fn canonical_snapshot_present(snapshots_root: &Path, fixture: &str) -> bool {
    snapshots_root.join(fixture).join(MANIFEST_FILE).is_file()
}

/// Resolve the canonical snapshot table that best matches an output
/// table name from a calculator.
///
/// Tries, in order: exact name, case-insensitive exact, unique
/// substring match. Returns `None` on ambiguity or absence.
pub fn resolve_canonical_table<'a>(
    snapshot: &'a Snapshot,
    output_table: &str,
) -> Option<&'a Table> {
    if let Some(table) = snapshot.table(output_table) {
        return Some(table);
    }
    let ci: Vec<&Table> = snapshot
        .tables()
        .filter(|t| t.name().eq_ignore_ascii_case(output_table))
        .collect();
    if let [only] = ci.as_slice() {
        return Some(only);
    }
    let want = output_table.to_ascii_lowercase();
    let substring: Vec<&Table> = snapshot
        .tables()
        .filter(|t| t.name().to_ascii_lowercase().contains(&want))
        .collect();
    match substring.as_slice() {
        [only] => Some(only),
        _ => None,
    }
}

/// Diff one produced table against one canonical table.
///
/// Re-stamps the canonical table with the produced table's name so
/// `diff_snapshots` can pair them regardless of the scratch-vs-database
/// naming gap. The returned [`Diff`] has the canonical table as the
/// left (expected) side.
///
/// # Errors
///
/// Propagates a [`moves_snapshot::Error`] on a table-shape bug.
pub fn compare_table(
    produced: &Table,
    canonical: &Table,
    opts: &DiffOptions,
) -> Result<Diff, moves_snapshot::Error> {
    let canonical_aligned = Table::from_normalized(
        produced.name().to_string(),
        canonical.schema().to_vec(),
        canonical.natural_key().to_vec(),
        canonical.columns().to_vec(),
    )?;

    let mut expected = Snapshot::new();
    expected.add_table(canonical_aligned)?;
    let mut actual = Snapshot::new();
    actual.add_table(produced.clone())?;

    Ok(diff_snapshots(&expected, &actual, opts))
}

/// Outcome of one `(fixture, calculator, output_table)` validation cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    /// No canonical snapshot present — gate is dormant.
    Dormant,
    /// Canonical snapshot present but no matching table found.
    CanonicalTableMissing,
    /// Produced and canonical tables match within tolerance.
    Matched,
    /// Produced and canonical tables diverge beyond tolerance.
    Diverged,
}

/// One `(fixture, calculator, output_table)` validation result.
#[derive(Debug, Clone, Serialize)]
pub struct TableValidation {
    pub fixture: String,
    pub calculator: String,
    pub output_table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_table: Option<String>,
    pub status: ValidationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff: Option<Diff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<DiffSummary>,
}

/// Load the canonical snapshot for `fixture`, diff `produced` against
/// it, and return a [`TableValidation`].
///
/// # Errors
///
/// Returns an error if the snapshot load or diff machinery fails.
pub fn validate_table(
    snapshots_root: &Path,
    fixture: &str,
    calculator: &str,
    produced: &Table,
    opts: &DiffOptions,
) -> Result<TableValidation, moves_snapshot::Error> {
    let output_table = produced.name().to_string();
    let base = TableValidation {
        fixture: fixture.to_string(),
        calculator: calculator.to_string(),
        output_table: output_table.clone(),
        canonical_table: None,
        status: ValidationStatus::Dormant,
        diff: None,
        summary: None,
    };

    if !canonical_snapshot_present(snapshots_root, fixture) {
        return Ok(base);
    }

    let snapshot = Snapshot::load(&snapshots_root.join(fixture))?;
    let Some(canonical) = resolve_canonical_table(&snapshot, &output_table) else {
        return Ok(TableValidation {
            status: ValidationStatus::CanonicalTableMissing,
            ..base
        });
    };

    let diff = compare_table(produced, canonical, opts)?;
    let summary = diff.summary();
    let status = if diff.is_empty() {
        ValidationStatus::Matched
    } else {
        ValidationStatus::Diverged
    };
    Ok(TableValidation {
        canonical_table: Some(canonical.name().to_string()),
        status,
        diff: Some(diff),
        summary: Some(summary),
        ..base
    })
}

/// Serialize a slice of [`TableValidation`]s to pretty JSON.
pub fn report_json(validations: &[TableValidation]) -> String {
    serde_json::to_string_pretty(validations)
        .unwrap_or_else(|e| format!("{{\"serialization_error\":\"{e}\"}}"))
}

#[cfg(test)]
mod tests {
    use moves_snapshot::{ColumnKind, TableBuilder, Value};

    use super::*;

    fn table(name: &str, rows: &[(i64, f64)]) -> Table {
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
            builder
                .push_row([Value::Int64(id), Value::Float64(value)])
                .unwrap();
        }
        builder.build().unwrap()
    }

    fn strict() -> DiffOptions {
        DiffOptions::default()
    }

    #[test]
    fn tolerance_config_loads() {
        assert!(tolerance_options().is_ok(), "tolerance.toml must parse");
    }

    #[test]
    fn identical_tables_have_empty_diff() {
        let canonical = table("T", &[(1, 1.0), (2, 2.0)]);
        let produced = table("T", &[(1, 1.0), (2, 2.0)]);
        let diff = compare_table(&produced, &canonical, &strict()).unwrap();
        assert!(diff.is_empty());
    }

    #[test]
    fn perturbed_cell_is_one_change() {
        let canonical = table("T", &[(1, 1.0), (2, 2.0)]);
        let produced = table("T", &[(1, 1.0), (2, 99.9)]);
        let diff = compare_table(&produced, &canonical, &strict()).unwrap();
        assert_eq!(diff.summary().cells_changed, 1);
    }

    #[test]
    fn name_gap_does_not_block_pairing() {
        let canonical = table("db__movesoutput__movesoutput", &[(1, 1.0)]);
        let produced = table("MOVESOutput", &[(1, 1.0)]);
        let diff = compare_table(&produced, &canonical, &strict()).unwrap();
        assert!(diff.is_empty());
    }

    #[test]
    fn canonical_snapshot_not_present_for_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!canonical_snapshot_present(dir.path(), "process-brakewear"));
    }

    #[test]
    fn resolve_canonical_table_exact() {
        let mut snap = Snapshot::new();
        snap.add_table(table("MOVESOutput", &[(1, 1.0)])).unwrap();
        assert!(resolve_canonical_table(&snap, "MOVESOutput").is_some());
    }

    #[test]
    fn resolve_canonical_table_case_insensitive() {
        let mut snap = Snapshot::new();
        snap.add_table(table("movesoutput", &[(1, 1.0)])).unwrap();
        assert!(resolve_canonical_table(&snap, "MOVESOutput").is_some());
    }

    #[test]
    fn resolve_canonical_table_substring() {
        let mut snap = Snapshot::new();
        snap.add_table(table("db__movesoutput__movesoutput", &[(1, 1.0)]))
            .unwrap();
        assert!(resolve_canonical_table(&snap, "MOVESOutput").is_some());
    }

    #[test]
    fn resolve_canonical_table_ambiguous_is_none() {
        let mut snap = Snapshot::new();
        snap.add_table(table("a__movesoutput", &[(1, 1.0)]))
            .unwrap();
        snap.add_table(table("b__movesoutput", &[(2, 2.0)]))
            .unwrap();
        assert!(resolve_canonical_table(&snap, "MOVESOutput").is_none());
    }

    #[test]
    fn validate_table_dormant_without_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let produced = table("MOVESOutput", &[(1, 1.0)]);
        let result = validate_table(
            dir.path(),
            "process-brakewear",
            "BaseRateCalculator",
            &produced,
            &strict(),
        )
        .unwrap();
        assert_eq!(result.status, ValidationStatus::Dormant);
    }

    #[test]
    fn validate_table_matches_written_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let fixture_dir = root.path().join("process-brakewear");
        let mut canonical = Snapshot::new();
        canonical
            .add_table(table("MOVESOutput", &[(1, 70.0), (2, 71.0)]))
            .unwrap();
        canonical.write(&fixture_dir).unwrap();

        let produced = table("MOVESOutput", &[(1, 70.0), (2, 71.0)]);
        let matched = validate_table(
            root.path(),
            "process-brakewear",
            "BaseRateCalculator",
            &produced,
            &strict(),
        )
        .unwrap();
        assert_eq!(matched.status, ValidationStatus::Matched);

        let diverged_t = table("MOVESOutput", &[(1, 70.0), (2, 99.0)]);
        let diverged = validate_table(
            root.path(),
            "process-brakewear",
            "BaseRateCalculator",
            &diverged_t,
            &strict(),
        )
        .unwrap();
        assert_eq!(diverged.status, ValidationStatus::Diverged);
        assert_eq!(diverged.summary.unwrap().cells_changed, 1);
    }

    #[test]
    fn validate_table_flags_missing_canonical_table() {
        let root = tempfile::tempdir().unwrap();
        let fixture_dir = root.path().join("process-brakewear");
        let mut canonical = Snapshot::new();
        canonical
            .add_table(table("something_unrelated", &[(1, 1.0)]))
            .unwrap();
        canonical.write(&fixture_dir).unwrap();

        let produced = table("MOVESOutput", &[(1, 1.0)]);
        let result = validate_table(
            root.path(),
            "process-brakewear",
            "BaseRateCalculator",
            &produced,
            &strict(),
        )
        .unwrap();
        assert_eq!(result.status, ValidationStatus::CanonicalTableMissing);
    }

    #[test]
    fn report_json_round_trips() {
        let validations = vec![TableValidation {
            fixture: "process-brakewear".to_string(),
            calculator: "BaseRateCalculator".to_string(),
            output_table: "MOVESOutput".to_string(),
            canonical_table: None,
            status: ValidationStatus::Dormant,
            diff: None,
            summary: None,
        }];
        let json = report_json(&validations);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed[0]["fixture"], "process-brakewear");
        assert_eq!(parsed[0]["status"], "dormant");
    }
}
