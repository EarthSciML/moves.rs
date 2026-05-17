//! Importer validation suite — Phase 4 Task 88.
//!
//! Each module runs one Rust importer against the committed fixtures
//! under `fixtures/`, normalizes the Parquet output through the
//! [`moves_importer_validation`] harness, and:
//!
//! * verifies the output is a well-formed, snapshot-stable table
//!   (`assert_snapshot_stable`) — this always runs in CI;
//! * diffs the output against the canonical-MOVES capture for the same
//!   inputs (`compare_to_canonical`) — this runs only when the operator
//!   has produced the snapshot (the capture suite is HPC-gated; see the
//!   crate `README.md`), and otherwise reports a skip.
//!
//! A drift against canonical MOVES is a candidate importer bug, which is
//! exactly the signal Task 88 is built to surface.

use std::path::PathBuf;

use moves_importer_validation::{
    characterization_dir, compare_importer_output, find_canonical_table, load_canonical_snapshot,
    parquet_to_table, read_importer_table,
};
use moves_snapshot::{diff_snapshots, DiffOptions, Snapshot, Table};

/// Absolute path to a committed importer fixture.
fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(rel)
}

/// Assert a normalized importer table survives a snapshot write/reload
/// cycle unchanged — i.e. the importer output is snapshot-comparable, the
/// precondition for diffing it against a canonical-MOVES capture.
fn assert_snapshot_stable(table: &Table) {
    let mut snapshot = Snapshot::new();
    snapshot
        .add_table(table.clone())
        .expect("table name is unique within the snapshot");
    let dir = tempfile::tempdir().unwrap();
    snapshot.write(dir.path()).expect("snapshot writes");
    let reloaded = Snapshot::load(dir.path()).expect("snapshot reloads");
    let diff = diff_snapshots(&snapshot, &reloaded, &DiffOptions::default());
    assert!(
        diff.is_empty(),
        "table '{}' is not snapshot-stable: {diff:?}",
        table.name()
    );
}

/// Gated canonical comparison: diff importer output against the canonical
/// MOVES capture for `moves_table`. Skips — rather than fails — when the
/// snapshot has not been produced. See the crate `README.md` for the
/// operator procedure that produces `snapshot_name`.
fn compare_to_canonical(snapshot_name: &str, moves_table: &str, importer_parquet: &[u8]) {
    let snapshot = match load_canonical_snapshot(snapshot_name) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            eprintln!(
                "[skip] canonical snapshot '{snapshot_name}' not captured — the \
                 importer-vs-canonical comparison is HPC-gated (see crate README)"
            );
            return;
        }
        Err(err) => panic!("loading canonical snapshot '{snapshot_name}': {err}"),
    };
    let Some(canonical_id) = find_canonical_table(&snapshot, moves_table) else {
        eprintln!("[skip] snapshot '{snapshot_name}' has no db__..__{moves_table} table");
        return;
    };
    let report = compare_importer_output(
        &canonical_id,
        importer_parquet,
        &snapshot,
        &DiffOptions::default(),
    )
    .expect("comparison runs");
    assert!(
        !report.has_importer_bug(),
        "importer output for '{moves_table}' drifted from canonical MOVES \
         ({} row diff(s), {} schema bug(s)) — each difference is a candidate \
         importer bug",
        report.row_diffs.len(),
        report.schema_bugs.len(),
    );
    if !report.columns_omitted_by_importer.is_empty() {
        eprintln!(
            "[note] '{moves_table}': importer omits canonical columns {:?} \
             (expected — MOVES synthesizes these in its SQL load script)",
            report.columns_omitted_by_importer
        );
    }
}

#[test]
fn characterization_dir_is_discoverable() {
    // The gated comparison depends on locating `characterization/`.
    assert!(
        characterization_dir().is_some(),
        "characterization/ directory should be reachable from the crate"
    );
}

#[test]
fn canonical_snapshots_are_gated_not_required() {
    // A snapshot that has certainly not been captured must resolve to a
    // clean skip, never an error — this is what keeps CI green while the
    // capture suite stays HPC-gated.
    let result = load_canonical_snapshot("importer-validation-does-not-exist").unwrap();
    assert!(result.is_none());
}

// ---------------------------------------------------------------------------
// County Database (CDB) importers — Task 83.
// ---------------------------------------------------------------------------
mod cdb {
    use super::*;

    use moves_importer::{
        read_csv_table, validate_table, write_table_parquet, ImportedTable, Importer, TableOutput,
        ValidationContext,
    };
    use moves_importer_county::{
        AgeDistributionImporter, SourceTypePopulationImporter, ZoneImporter, ZoneRoadTypeImporter,
    };

    /// Run a CDB importer against its fixture CSV(s): read, validate
    /// (per-column + cross-row), and encode Parquet. The committed
    /// fixtures are clean inputs, so any validation *error* fails the
    /// suite. Returns one [`TableOutput`] per importer table, in
    /// descriptor order.
    fn run(importer: &dyn Importer, csvs: &[PathBuf]) -> Vec<TableOutput> {
        let descriptors = importer.tables();
        assert_eq!(
            descriptors.len(),
            csvs.len(),
            "{}: one fixture CSV is required per importer table",
            importer.name()
        );
        let ctx = ValidationContext::without_default_db();
        let mut imported: Vec<ImportedTable> = Vec::with_capacity(descriptors.len());
        for (descriptor, csv) in descriptors.iter().zip(csvs) {
            let rows = read_csv_table(csv, descriptor).expect("fixture CSV reads cleanly");
            let table = ImportedTable::new(descriptor, rows.source_path, rows.batch);
            let messages = validate_table(&table, &ctx).expect("column validation runs");
            let errors: Vec<_> = messages.iter().filter(|m| m.is_error()).collect();
            assert!(
                errors.is_empty(),
                "{}: fixture has column validation errors: {errors:?}",
                importer.name()
            );
            imported.push(table);
        }
        let cross = importer.validate_imported(&imported, &ctx);
        let cross_errors: Vec<_> = cross.iter().filter(|m| m.is_error()).collect();
        assert!(
            cross_errors.is_empty(),
            "{}: fixture has cross-row validation errors: {cross_errors:?}",
            importer.name()
        );
        descriptors
            .iter()
            .zip(&imported)
            .map(|(descriptor, table)| {
                write_table_parquet(descriptor, &table.batch, None).expect("Parquet encodes")
            })
            .collect()
    }

    #[test]
    fn source_type_population_importer() {
        let out = run(
            &SourceTypePopulationImporter,
            &[fixture("cdb/SourceTypeYear.csv")],
        );
        let table = parquet_to_table("SourceTypeYear", &out[0].bytes, &["yearID", "sourceTypeID"])
            .expect("importer Parquet normalizes");
        assert_eq!(table.row_count(), 14);
        assert_snapshot_stable(&table);
        compare_to_canonical("importer-validation-cdb", "SourceTypeYear", &out[0].bytes);
    }

    #[test]
    fn zone_road_type_importer() {
        let out = run(&ZoneRoadTypeImporter, &[fixture("cdb/ZoneRoadType.csv")]);
        let table = parquet_to_table("ZoneRoadType", &out[0].bytes, &["zoneID", "roadTypeID"])
            .expect("importer Parquet normalizes");
        assert_eq!(table.row_count(), 4);
        assert_snapshot_stable(&table);
        compare_to_canonical("importer-validation-cdb", "ZoneRoadType", &out[0].bytes);
    }

    #[test]
    fn age_distribution_importer() {
        let out = run(
            &AgeDistributionImporter,
            &[fixture("cdb/SourceTypeAgeDistribution.csv")],
        );
        let table = parquet_to_table(
            "SourceTypeAgeDistribution",
            &out[0].bytes,
            &["sourceTypeID", "yearID", "ageID"],
        )
        .expect("importer Parquet normalizes");
        assert_eq!(table.row_count(), 10);
        assert_snapshot_stable(&table);
        compare_to_canonical(
            "importer-validation-cdb",
            "SourceTypeAgeDistribution",
            &out[0].bytes,
        );
    }

    #[test]
    fn zone_importer_emits_zone_and_zone_road_type() {
        // ZoneImporter is a two-table importer: `zone` then `zoneRoadType`.
        let out = run(
            &ZoneImporter,
            &[fixture("cdb/Zone.csv"), fixture("cdb/ZoneRoadType.csv")],
        );
        assert_eq!(out.len(), 2);

        let zone =
            parquet_to_table("Zone", &out[0].bytes, &["zoneID"]).expect("Zone Parquet normalizes");
        assert_eq!(zone.row_count(), 1);
        assert_snapshot_stable(&zone);
        compare_to_canonical("importer-validation-cdb", "Zone", &out[0].bytes);

        let zone_road_type =
            parquet_to_table("ZoneRoadType", &out[1].bytes, &["zoneID", "roadTypeID"])
                .expect("ZoneRoadType Parquet normalizes");
        assert_eq!(zone_road_type.row_count(), 4);
        assert_snapshot_stable(&zone_road_type);
    }
}

// ---------------------------------------------------------------------------
// Project Database (PDB) importer — Task 84.
// ---------------------------------------------------------------------------
mod pdb {
    use super::*;

    use moves_importer_pdb::{filter::RunSpecFilter, ImportSession};

    /// MOVES natural key for each project-scale table — the columns the
    /// canonical snapshot sorts and keys rows on.
    fn natural_key(table: &str) -> &'static [&'static str] {
        match table {
            "Link" => &["linkID"],
            "linkSourceTypeHour" => &["linkID", "sourceTypeID"],
            "driveScheduleSecondLink" => &["linkID", "secondID"],
            "offNetworkLink" => &["zoneID", "sourceTypeID"],
            "OpModeDistribution" => &[
                "sourceTypeID",
                "hourDayID",
                "linkID",
                "polProcessID",
                "opModeID",
            ],
            other => panic!("unknown PDB table: {other}"),
        }
    }

    #[test]
    fn project_importer_emits_all_five_tables() {
        let out_dir = tempfile::tempdir().unwrap();
        let runspec = RunSpecFilter::default();
        let session = ImportSession::builder(out_dir.path(), &runspec)
            .with_link(fixture("pdb/Link.csv"))
            .with_link_source_type_hour(fixture("pdb/linkSourceTypeHour.csv"))
            .with_drive_schedule_second_link(fixture("pdb/driveScheduleSecondLink.csv"))
            .with_off_network_link(fixture("pdb/offNetworkLink.csv"))
            .with_op_mode_distribution(fixture("pdb/OpModeDistribution.csv"))
            .build()
            .expect("import session builds");
        let manifest = session.write_to_disk().expect("PDB import writes");
        assert_eq!(manifest.tables.len(), 5);

        let mut total_rows = 0u64;
        for entry in &manifest.tables {
            let bytes = std::fs::read(&entry.output_path).expect("importer Parquet readable");
            let table = parquet_to_table(&entry.name, &bytes, natural_key(&entry.name))
                .expect("importer Parquet normalizes");
            assert_eq!(
                table.row_count() as u64,
                entry.row_count,
                "{}: normalized row count disagrees with the importer manifest",
                entry.name
            );
            assert_snapshot_stable(&table);
            compare_to_canonical("importer-validation-pdb", &entry.name, &bytes);
            total_rows += entry.row_count;
        }
        // 4 links + 9 link-source-type-hour + 5 drive-schedule + 2
        // off-network + 4 op-mode rows.
        assert_eq!(total_rows, 24);
    }
}

// ---------------------------------------------------------------------------
// Nonroad input-database importer — Task 85.
// ---------------------------------------------------------------------------
mod nonroad {
    use super::*;

    use moves_nonroad_import::{import, ImportOptions};

    #[test]
    fn nonroad_importer_output_is_snapshot_comparable() {
        let out_dir = tempfile::tempdir().unwrap();
        let mut opts = ImportOptions::new(fixture("nonroad"), out_dir.path());
        // Fixed stamp keeps the manifest byte-stable across runs.
        opts.generated_at_utc = Some("2026-05-16T00:00:00Z".to_string());
        let (manifest, report) = import(&opts).expect("nonroad import runs");

        // Two of the four built-in tables have committed fixtures; the
        // rest are absent and skip cleanly (require_all_tables = false).
        assert_eq!(report.tables_written.len(), 2);
        assert_eq!(manifest.tables.len(), 2);

        for entry in &manifest.tables {
            // The Nonroad manifest records the primary key per table.
            let key: Vec<&str> = entry.primary_key.iter().map(String::as_str).collect();
            let path = out_dir.path().join(&entry.path);
            let table =
                read_importer_table(&entry.name, &path, &key).expect("importer Parquet normalizes");
            assert_eq!(table.row_count() as u64, entry.row_count);
            assert_snapshot_stable(&table);
        }
    }
}

// ---------------------------------------------------------------------------
// LEV / NLEV alternative-rate importer — Task 87.
// ---------------------------------------------------------------------------
mod lev {
    use super::*;

    use moves_import_lev::import_lev;

    #[test]
    fn lev_importer_output_is_snapshot_comparable() {
        let out_dir = tempfile::tempdir().unwrap();
        let report = import_lev(&fixture("lev/EmissionRateByAgeLEV.csv"), out_dir.path())
            .expect("LEV import runs");
        assert_eq!(report.row_count, 4);

        let bytes = std::fs::read(&report.output_path).expect("importer Parquet readable");
        let table = parquet_to_table(
            "EmissionRateByAgeLEV",
            &bytes,
            &["sourceBinID", "polProcessID", "opModeID", "ageGroupID"],
        )
        .expect("importer Parquet normalizes");
        assert_eq!(table.row_count(), 4);
        assert_snapshot_stable(&table);
    }
}

// ---------------------------------------------------------------------------
// AVFT importer — Task 86.
// ---------------------------------------------------------------------------
mod avft {
    use super::*;

    use moves_avft::{import, parquet_io};

    #[test]
    fn avft_importer_output_is_snapshot_comparable() {
        let read = import::read_csv(fixture("avft/avft.csv")).expect("AVFT CSV reads");
        import::validate(&read.table).expect("AVFT fixture passes validation");
        let bytes = parquet_io::encode_parquet(&read.table).expect("AVFT Parquet encodes");

        let table = parquet_to_table(
            "avft",
            &bytes,
            &["sourceTypeID", "modelYearID", "fuelTypeID", "engTechID"],
        )
        .expect("importer Parquet normalizes");
        assert_eq!(table.row_count(), 3);
        assert_snapshot_stable(&table);
    }
}
