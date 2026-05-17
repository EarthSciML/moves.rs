//! `moves import-cdb` — import County-database (CDB) input CSVs to Parquet.
//!
//! Walks the County-scale importers in [`moves_importer_county::ALL`]. For
//! each importer and each table it declares, the command looks for a
//! `<TableName>.csv` in the input directory, reads it through the importer
//! framework ([`moves_importer`]), runs per-column and cross-row
//! validation, and — if the table is clean — writes a deterministic
//! `<TableName>.parquet` to the output directory.
//!
//! # Default-DB validation
//!
//! Foreign-key columns validate against the MOVES default database. Pass
//! `--default-db <dir>` (a converted default-DB Parquet tree) to make FK
//! violations hard errors. Without it, the importer framework downgrades FK
//! checks to warnings — numeric-range and cross-row checks still apply — so
//! the command is usable before a default-DB snapshot is staged.
//!
//! A table declared by more than one importer (e.g. `ZoneRoadType`, owned by
//! both the standalone `ZoneRoadType` importer and the zone-domain `Zone`
//! importer) is validated under each importer's rules and reported once per
//! importer. The Parquet write is deterministic, so a repeated write is a
//! harmless no-op.
//!
//! # Phase 2 scope
//!
//! `moves-importer-county` currently ships four of MOVES's ~24 County
//! importers (migration-plan Task 83); the rest land in follow-up tasks.
//! `import-cdb` automatically covers each importer the crate adds — no CLI
//! change needed.

use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use moves_data_default::DefaultDb;
use moves_importer::{
    read_csv_table, validate_table, write_table_parquet, ImportedTable, Severity,
    ValidationContext, ValidationMessage,
};

/// Inputs for one `moves import-cdb` invocation.
#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// Directory holding the user's `<TableName>.csv` files.
    pub input: PathBuf,
    /// Directory the validated `<TableName>.parquet` files are written to.
    /// Created if absent.
    pub output: PathBuf,
    /// Optional converted default-DB Parquet tree. When set, foreign-key
    /// columns are validated against it (violations become errors);
    /// otherwise FK checks degrade to warnings.
    pub default_db: Option<PathBuf>,
}

/// The fate of one table in an [`import_cdb`] run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportStatus {
    /// Validated clean and written to a `.parquet` file.
    Written,
    /// Validation found errors; no Parquet was written.
    Rejected,
    /// No `<TableName>.csv` was present in the input directory.
    Missing,
}

/// Per-table result of an [`import_cdb`] run.
#[derive(Debug, Clone)]
pub struct ImportedTableReport {
    /// Human-readable name of the importer that owns this table.
    pub importer: &'static str,
    /// Canonical table name (matches the `<TableName>.csv` / `.parquet`).
    pub table: &'static str,
    /// What happened to the table.
    pub status: ImportStatus,
    /// The CSV that was read. `None` when [`ImportStatus::Missing`].
    pub source: Option<PathBuf>,
    /// The Parquet that was written. `Some` only for [`ImportStatus::Written`].
    pub destination: Option<PathBuf>,
    /// Data-row count of the table (0 when missing).
    pub row_count: u64,
    /// Formatted validation errors (empty unless [`ImportStatus::Rejected`]).
    pub errors: Vec<String>,
    /// Formatted validation warnings — surfaced for any read table.
    pub warnings: Vec<String>,
}

/// What [`import_cdb`] did, table by table.
#[derive(Debug, Clone)]
pub struct ImportOutcome {
    /// One entry per declared table across every County importer, in
    /// importer-then-declaration order.
    pub tables: Vec<ImportedTableReport>,
    /// Whether a default-DB handle was wired for foreign-key validation.
    pub default_db_used: bool,
}

impl ImportOutcome {
    /// Count of tables written to Parquet.
    #[must_use]
    pub fn written(&self) -> usize {
        self.count(ImportStatus::Written)
    }

    /// Count of tables rejected by validation.
    #[must_use]
    pub fn rejected(&self) -> usize {
        self.count(ImportStatus::Rejected)
    }

    /// Count of declared tables with no CSV present.
    #[must_use]
    pub fn missing(&self) -> usize {
        self.count(ImportStatus::Missing)
    }

    /// Whether any present table failed validation.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.rejected() > 0
    }

    fn count(&self, status: ImportStatus) -> usize {
        self.tables.iter().filter(|t| t.status == status).count()
    }
}

/// Import the County-database CSVs found in `opts.input`.
///
/// # Errors
///
/// Fails if the input path is not a directory, the output directory cannot
/// be created, an optional `--default-db` cannot be opened, a present CSV
/// is structurally unreadable (missing required column, bad cell type), or
/// no recognised CSV is present at all. Per-row / cross-row validation
/// *errors* do not fail the call — they mark the table
/// [`ImportStatus::Rejected`] in the returned outcome.
pub fn import_cdb(opts: &ImportOptions) -> Result<ImportOutcome> {
    if !opts.input.is_dir() {
        bail!("input path {} is not a directory", opts.input.display());
    }
    fs::create_dir_all(&opts.output)
        .with_context(|| format!("creating output directory {}", opts.output.display()))?;

    // An optional default-DB handle; `ctx` borrows it for the whole run.
    let default_db = match &opts.default_db {
        Some(path) => Some(
            DefaultDb::open(path)
                .with_context(|| format!("opening default DB at {}", path.display()))?,
        ),
        None => None,
    };
    let ctx = match &default_db {
        Some(db) => ValidationContext::new(db),
        None => ValidationContext::without_default_db(),
    };

    let mut tables = Vec::new();
    let mut any_csv_present = false;

    for &importer in moves_importer_county::ALL {
        let descriptors = importer.tables();

        // Read every declared table that has a CSV present. A CSV that is
        // present but structurally unreadable is a hard error (propagated
        // with `?`); a *validation* failure is reported as Rejected below.
        let mut imported: Vec<ImportedTable> = Vec::new();
        for descriptor in descriptors {
            let csv = opts.input.join(format!("{}.csv", descriptor.name));
            if !csv.is_file() {
                continue;
            }
            any_csv_present = true;
            let rows = read_csv_table(&csv, descriptor)
                .with_context(|| format!("reading {}", csv.display()))?;
            imported.push(ImportedTable::new(descriptor, rows.source_path, rows.batch));
        }

        // Cross-row checks index the table slice positionally, so they can
        // only run when every table the importer declares is present.
        let cross = if !imported.is_empty() && imported.len() == descriptors.len() {
            importer.validate_imported(&imported, &ctx)
        } else {
            Vec::new()
        };

        // One report per declared table, in declaration order.
        for descriptor in descriptors {
            let Some(table) = imported
                .iter()
                .find(|t| t.descriptor.name == descriptor.name)
            else {
                tables.push(ImportedTableReport {
                    importer: importer.name(),
                    table: descriptor.name,
                    status: ImportStatus::Missing,
                    source: None,
                    destination: None,
                    row_count: 0,
                    errors: Vec::new(),
                    warnings: Vec::new(),
                });
                continue;
            };

            let column_msgs = validate_table(table, &ctx)
                .with_context(|| format!("validating {}", table.source_path.display()))?;
            let errors: Vec<String> = column_msgs
                .iter()
                .chain(cross.iter())
                .filter(|m| m.table == descriptor.name && m.is_error())
                .map(format_message)
                .collect();
            let warnings: Vec<String> = column_msgs
                .iter()
                .chain(cross.iter())
                .filter(|m| m.table == descriptor.name && matches!(m.severity, Severity::Warning))
                .map(format_message)
                .collect();
            let row_count = table.batch.num_rows() as u64;

            if errors.is_empty() {
                let destination = opts.output.join(format!("{}.parquet", descriptor.name));
                write_table_parquet(table.descriptor, &table.batch, Some(&destination))
                    .with_context(|| format!("writing {}", destination.display()))?;
                tables.push(ImportedTableReport {
                    importer: importer.name(),
                    table: descriptor.name,
                    status: ImportStatus::Written,
                    source: Some(table.source_path.clone()),
                    destination: Some(destination),
                    row_count,
                    errors,
                    warnings,
                });
            } else {
                tables.push(ImportedTableReport {
                    importer: importer.name(),
                    table: descriptor.name,
                    status: ImportStatus::Rejected,
                    source: Some(table.source_path.clone()),
                    destination: None,
                    row_count,
                    errors,
                    warnings,
                });
            }
        }
    }

    if !any_csv_present {
        bail!(
            "no County-database CSV files found in {} \
             (expected files named after the table, e.g. SourceTypeYear.csv)",
            opts.input.display()
        );
    }

    Ok(ImportOutcome {
        tables,
        default_db_used: default_db.is_some(),
    })
}

/// Render a [`ValidationMessage`] as a single readable line:
/// `Table.column (row N): message`.
fn format_message(message: &ValidationMessage) -> String {
    let mut line = message.table.to_string();
    if let Some(column) = message.column {
        line.push('.');
        line.push_str(column);
    }
    if let Some(row) = message.row {
        line.push_str(&format!(" (row {row})"));
    }
    line.push_str(": ");
    line.push_str(&message.message);
    line
}
