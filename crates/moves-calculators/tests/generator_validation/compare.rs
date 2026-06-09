//! Diff a generator's produced table against a canonical snapshot.
//!
//! The comparison reuses the [`moves_snapshot`] crate — the
//! canonical-MOVES captures *are* `moves_snapshot`-format snapshots,
//! so the gate diffs with the same engine's own
//! regression-detection uses rather than reinventing one.
//!
//! Two layers:
//!
//! * [`compare_table`] is the pure, in-memory machinery — diff one
//! produced [`Table`] against one canonical [`Table`] under a
//! tolerance budget. It is what every machinery test exercises.
//! * [`validate_table`] is the disk-aware orchestration — locate the
//! fixture's canonical snapshot, resolve the table matching the
//! generator's output, and run [`compare_table`]. It is what the
//! activated gate calls per `(fixture, generator)` coverage cell;
//! today, with no snapshots in the repo, it reports [`Dormant`].
//!
//! [`Dormant`]: ValidationStatus::Dormant

use std::path::Path;

use moves_snapshot::{
    diff_snapshots, Diff, DiffOptions, DiffSummary, Snapshot, Table, ToleranceConfig,
    ToleranceError,
};
use serde::Serialize;

/// The `manifest.json` file every `moves_snapshot` snapshot carries/// its presence is how the harness tells a populated snapshot
/// directory from an empty placeholder.
const MANIFEST_FILE: &str = "manifest.json";

/// Load the tolerance budget from `characterization/generator-validation/`.
///
/// The committed `tolerance.toml` is the version-controlled budget/// the per-(table, column) absolute tolerances the canonical-capture
/// diff applies, including the documented *expected* divergences.
///
/// # Errors
///
/// Returns [`ToleranceError`] when the committed file is missing or
/// malformed — a real defect the harness should fail loudly on.
pub fn tolerance_options() -> Result<DiffOptions, ToleranceError> {
    let path = super::tolerance_config_path();
    let config = ToleranceConfig::from_file(&path)?;
    Ok(config.into())
}

/// Whether a populated canonical snapshot exists for `fixture` under
/// `snapshots_root` — i.e. `<snapshots_root>/<fixture>/manifest.json`
/// is present. A bare directory holding only a `README` is not
/// populated.
pub fn canonical_snapshot_present(snapshots_root: &Path, fixture: &str) -> bool {
    snapshots_root.join(fixture).join(MANIFEST_FILE).is_file()
}

/// Resolve the canonical snapshot table that corresponds to a
/// generator's output-table name.
///
/// A generator's `output_tables()` entry is a scratch-namespace name
/// (`ZoneMonthHour`); the canonical capture stores it under the MOVES
/// database/table path it was dumped from (e.g.
/// `db__movesexecution__zonemonthhour`). The match is best-effort:
///
/// 1. exact name match,
/// 2. case-insensitive exact match,
/// 3. a *unique* table whose lower-cased name contains the
/// lower-cased output name as a substring.
///
/// An ambiguous or absent match yields `None`; the precise mapping is
/// finalised against a real capture when the gate activates.
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

/// Diff a produced table against a canonical table under a tolerance
/// budget — the pure comparison machinery.
///
/// `diff_snapshots` pairs tables by name. The *canonical* table is
/// re-stamped with the produced table's name first, so two things
/// hold regardless of the scratch-vs-database naming gap: the tables
/// pair, and the diff is keyed on the stable generator-output name
/// (`ZoneMonthHour`) — which is what `tolerance.toml` budgets are
/// authored against. The returned [`Diff`] has the canonical table as
/// the left (expected) side.
///
/// # Errors
///
/// Propagates a [`moves_snapshot::Error`] only on a table-shape bug
/// (a duplicate column or row-width mismatch), which a table built by
/// [`super::adapter`] cannot hit.
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

/// The outcome of validating one generator's produced table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    /// No populated canonical snapshot for the fixture — the gate is
    /// dormant for this `(fixture, generator)` pair.
    Dormant,
    /// The canonical snapshot exists but holds no table matching the
    /// generator's output — a naming-resolution gap to settle at
    /// activation, or a generator producing an uncaptured table.
    CanonicalTableMissing,
    /// The produced table matched the canonical capture within the
    /// tolerance budget.
    Matched,
    /// The produced table diverged from the canonical capture.
    Diverged,
}

/// The validation result for one `(fixture, generator)` coverage cell.
#[derive(Debug, Clone, Serialize)]
pub struct TableValidation {
    /// Fixture name.
    pub fixture: String,
    /// Generator name.
    pub generator: String,
    /// The generator-output table that was validated.
    pub output_table: String,
    /// The canonical snapshot table [`Self::output_table`] resolved
    /// to — `Some` once a canonical capture is present and matched,
    /// preserving which database table the diff ran against.
    pub canonical_table: Option<String>,
    /// The verdict.
    pub status: ValidationStatus,
    /// The structured diff — `Some` only when [`Self::status`] is
    /// [`Matched`](ValidationStatus::Matched) or
    /// [`Diverged`](ValidationStatus::Diverged).
    pub diff: Option<Diff>,
    /// Aggregate diff counts, mirrored from [`Self::diff`] for quick
    /// CI scanning.
    pub summary: Option<DiffSummary>,
}

/// Validate one generator's produced table for one fixture.
///
/// Locates `<snapshots_root>/<fixture>/`, and:
///
/// * with no populated snapshot there, returns
/// [`ValidationStatus::Dormant`] — the repository state today;
/// * with a snapshot but no table matching `produced.name()`,
/// returns [`ValidationStatus::CanonicalTableMissing`];
/// * otherwise diffs and returns [`Matched`](ValidationStatus::Matched)
/// or [`Diverged`](ValidationStatus::Diverged).
///
/// # Errors
///
/// Propagates a [`moves_snapshot::Error`] when a present snapshot
/// fails to load (corrupt manifest, content-hash mismatch) or the
/// diff hits a table-shape bug.
pub fn validate_table(
    snapshots_root: &Path,
    fixture: &str,
    generator: &str,
    produced: &Table,
    opts: &DiffOptions,
) -> Result<TableValidation, moves_snapshot::Error> {
    let output_table = produced.name().to_string();
    let base = TableValidation {
        fixture: fixture.to_string(),
        generator: generator.to_string(),
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

/// Serialise a batch of validations as pretty JSON — the CI artifact
/// handed on for triage, mirroring the fidelity report.
pub fn report_json(validations: &[TableValidation]) -> String {
    serde_json::to_string_pretty(validations)
        .unwrap_or_else(|e| format!("{{\"serialization_error\":\"{e}\"}}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use moves_snapshot::{ColumnKind, TableBuilder, Value};

    /// A two-column table: an `id` key and a `value` float.
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
        for (id, value) in rows {
            builder
                .push_row([Value::Int64(*id), Value::Float64(*value)])
                .unwrap();
        }
        builder.build().unwrap()
    }

    fn strict() -> DiffOptions {
        DiffOptions::default()
    }

    #[test]
    fn identical_tables_have_an_empty_diff() {
        let canonical = table("T", &[(1, 1.0), (2, 2.0)]);
        let produced = table("T", &[(1, 1.0), (2, 2.0)]);
        let diff = compare_table(&produced, &canonical, &strict()).unwrap();
        assert!(diff.is_empty(), "self-diff must be empty");
    }

    #[test]
    fn a_perturbed_cell_is_one_change() {
        let canonical = table("T", &[(1, 1.0), (2, 2.0)]);
        let produced = table("T", &[(1, 1.0), (2, 2.5)]);
        let diff = compare_table(&produced, &canonical, &strict()).unwrap();
        assert!(!diff.is_empty());
        assert_eq!(diff.summary().cells_changed, 1);
    }

    #[test]
    fn tolerance_absorbs_a_sub_budget_difference() {
        let canonical = table("T", &[(1, 1.0)]);
        let produced = table("T", &[(1, 1.0 + 1e-7)]);
        // Strict: the difference shows.
        assert!(!compare_table(&produced, &canonical, &strict())
            .unwrap()
            .is_empty());
        // Within a 1e-6 budget: absorbed.
        let lenient = DiffOptions::default().with_default_float_tolerance(1e-6);
        assert!(compare_table(&produced, &canonical, &lenient)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn the_name_gap_does_not_block_pairing() {
        // Produced is the scratch name; canonical is the database path.
        let canonical = table("db__movesexecution__zonemonthhour", &[(1, 1.0)]);
        let produced = table("ZoneMonthHour", &[(1, 1.0)]);
        let diff = compare_table(&produced, &canonical, &strict()).unwrap();
        assert!(
            diff.is_empty(),
            "re-stamping should pair the two tables: {diff:?}"
        );
    }

    #[test]
    fn resolve_matches_exact_then_case_then_substring() {
        let mut exact = Snapshot::new();
        exact
            .add_table(table("ZoneMonthHour", &[(1, 1.0)]))
            .unwrap();
        assert!(resolve_canonical_table(&exact, "ZoneMonthHour").is_some());

        let mut ci = Snapshot::new();
        ci.add_table(table("zonemonthhour", &[(1, 1.0)])).unwrap();
        assert!(resolve_canonical_table(&ci, "ZoneMonthHour").is_some());

        let mut sub = Snapshot::new();
        sub.add_table(table("db__exec__zonemonthhour", &[(1, 1.0)]))
            .unwrap();
        assert!(resolve_canonical_table(&sub, "ZoneMonthHour").is_some());
    }

    #[test]
    fn resolve_is_none_when_ambiguous_or_absent() {
        let mut ambiguous = Snapshot::new();
        ambiguous
            .add_table(table("a__zonemonthhour", &[(1, 1.0)]))
            .unwrap();
        ambiguous
            .add_table(table("b__zonemonthhour", &[(2, 2.0)]))
            .unwrap();
        assert!(resolve_canonical_table(&ambiguous, "ZoneMonthHour").is_none());

        let empty = Snapshot::new();
        assert!(resolve_canonical_table(&empty, "ZoneMonthHour").is_none());
    }

    #[test]
    fn validate_table_is_dormant_without_a_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let produced = table("ZoneMonthHour", &[(1, 1.0)]);
        let result = validate_table(
            dir.path(),
            "process-brakewear",
            "MeteorologyGenerator",
            &produced,
            &strict(),
        )
        .unwrap();
        assert_eq!(result.status, ValidationStatus::Dormant);
        assert!(result.diff.is_none());
    }

    #[test]
    fn validate_table_matches_against_a_written_snapshot() {
        // Build a canonical snapshot on disk, then validate an identical
        // produced table against it — the activated gate's happy path.
        let root = tempfile::tempdir().unwrap();
        let fixture_dir = root.path().join("process-brakewear");
        let mut canonical = Snapshot::new();
        canonical
            .add_table(table("ZoneMonthHour", &[(1, 70.0), (2, 71.0)]))
            .unwrap();
        canonical.write(&fixture_dir).unwrap();

        let produced = table("ZoneMonthHour", &[(1, 70.0), (2, 71.0)]);
        let matched = validate_table(
            root.path(),
            "process-brakewear",
            "MeteorologyGenerator",
            &produced,
            &strict(),
        )
        .unwrap();
        assert_eq!(matched.status, ValidationStatus::Matched);

        // A diverging produced table flips the verdict.
        let diverged_table = table("ZoneMonthHour", &[(1, 70.0), (2, 99.0)]);
        let diverged = validate_table(
            root.path(),
            "process-brakewear",
            "MeteorologyGenerator",
            &diverged_table,
            &strict(),
        )
        .unwrap();
        assert_eq!(diverged.status, ValidationStatus::Diverged);
        assert_eq!(diverged.summary.unwrap().cells_changed, 1);
    }

    #[test]
    fn validate_table_flags_a_missing_canonical_table() {
        let root = tempfile::tempdir().unwrap();
        let fixture_dir = root.path().join("process-brakewear");
        let mut canonical = Snapshot::new();
        canonical
            .add_table(table("something_unrelated", &[(1, 1.0)]))
            .unwrap();
        canonical.write(&fixture_dir).unwrap();

        let produced = table("ZoneMonthHour", &[(1, 1.0)]);
        let result = validate_table(
            root.path(),
            "process-brakewear",
            "MeteorologyGenerator",
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
            generator: "MeteorologyGenerator".to_string(),
            output_table: "ZoneMonthHour".to_string(),
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
