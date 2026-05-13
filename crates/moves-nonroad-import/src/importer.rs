//! End-to-end import orchestration.
//!
//! Caller hands us:
//!
//! * `input_dir` — a directory containing the per-table CSV templates.
//! * `output_dir` — where to write the Parquet files plus the manifest.
//! * `tables` — an explicit list of tables to import, defaulting to all
//!   four built-ins. Selecting a subset is useful for partial updates
//!   (a user revising only their retrofit factors) and for tests.
//!
//! We return an [`ImportReport`] summarising what was written. Skipped
//! tables (named in `tables` but with no CSV on disk) are surfaced via
//! the report; the caller can promote that to an error or accept it.
//!
//! The output layout is intentionally flat:
//!
//! ```text
//! <output_dir>/
//!   manifest.json
//!   nrbaseyearequippopulation.parquet
//!   nrengtechfraction.parquet
//!   nrretrofitfactors.parquet
//!   nrmonthallocation.parquet
//! ```
//!
//! Phase 4 Task 80 partitions the default-DB tree to keep memory use
//! bounded; user-input tables are several orders of magnitude smaller
//! (a fully-populated `nrmonthallocation` is ~6k rows for all states ×
//! 25 equip types × 12 months) so partitioning would only add noise.

use std::path::{Path, PathBuf};

use crate::convert::convert_table;
use crate::csv::read_csv;
use crate::error::{Error, Result};
use crate::manifest::{Manifest, TableManifest, MANIFEST_FILENAME};
use crate::parquet_writer::{encode_parquet, write_atomic};
use crate::tables::{self, ImporterEntry};

/// Configuration for an import run.
#[derive(Debug, Clone)]
pub struct ImportOptions {
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    /// If empty, all built-in tables are processed. Otherwise only the
    /// named tables (case-insensitive). Tables in this list that aren't
    /// recognised raise [`Error::Internal`] — typos shouldn't silently
    /// no-op.
    pub tables: Vec<String>,
    /// If `false` (default), CSVs that don't exist on disk are recorded
    /// as `skipped` rather than erroring. Set `true` to require every
    /// selected table to be present.
    pub require_all_tables: bool,
    /// Optional override for the `generated_at_utc` field in the
    /// manifest. Tests pass a fixed string for byte-stable comparisons.
    pub generated_at_utc: Option<String>,
}

impl ImportOptions {
    pub fn new(input_dir: impl Into<PathBuf>, output_dir: impl Into<PathBuf>) -> Self {
        Self {
            input_dir: input_dir.into(),
            output_dir: output_dir.into(),
            tables: Vec::new(),
            require_all_tables: false,
            generated_at_utc: None,
        }
    }
}

/// Per-run summary for telemetry / CI logging.
#[derive(Debug, Clone, Default)]
pub struct ImportReport {
    pub tables_written: Vec<String>,
    pub tables_skipped: Vec<String>,
    pub total_rows: u64,
}

/// Run the import pipeline end-to-end.
pub fn import(opts: &ImportOptions) -> Result<(Manifest, ImportReport)> {
    let entries = select_tables(&opts.tables)?;

    std::fs::create_dir_all(&opts.output_dir).map_err(|source| Error::Io {
        path: opts.output_dir.clone(),
        source,
    })?;

    let generated_at = opts
        .generated_at_utc
        .clone()
        .unwrap_or_else(now_iso8601_utc);
    let mut manifest = Manifest::new(generated_at);
    let mut report = ImportReport::default();

    for entry in entries {
        let csv_path = opts.input_dir.join(entry.csv_filename);
        if !csv_path.exists() {
            if opts.require_all_tables {
                return Err(Error::Io {
                    path: csv_path,
                    source: std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "input CSV does not exist",
                    ),
                });
            }
            report.tables_skipped.push(entry.schema.name.to_string());
            continue;
        }

        let csv = read_csv(&csv_path)?;
        let converted = convert_table(entry.schema, &csv)?;
        let parquet = encode_parquet(entry.schema.columns, &converted.rows)?;
        let parquet_path = opts.output_dir.join(entry.parquet_filename);
        write_atomic(&parquet_path, &parquet.bytes)?;

        manifest.push(TableManifest {
            name: entry.schema.name.to_string(),
            row_count: parquet.row_count,
            columns: converted.columns,
            primary_key: converted.primary_key,
            path: entry.parquet_filename.to_string(),
            sha256: parquet.sha256,
            bytes: parquet.bytes.len() as u64,
        });
        report.tables_written.push(entry.schema.name.to_string());
        report.total_rows += parquet.row_count;
    }

    manifest.finalize();
    let manifest_path = opts.output_dir.join(MANIFEST_FILENAME);
    let manifest_json = manifest.to_pretty_json()?;
    write_atomic(&manifest_path, manifest_json.as_bytes())?;

    Ok((manifest, report))
}

fn select_tables(requested: &[String]) -> Result<Vec<&'static ImporterEntry>> {
    if requested.is_empty() {
        return Ok(tables::all().iter().collect());
    }
    let mut out = Vec::with_capacity(requested.len());
    for name in requested {
        let entry = tables::find(name).ok_or_else(|| Error::Internal {
            message: format!(
                "table '{name}' is not a registered Nonroad importer; \
                 known tables: nrbaseyearequippopulation, nrengtechfraction, \
                 nrretrofitfactors, nrmonthallocation"
            ),
        })?;
        out.push(entry);
    }
    Ok(out)
}

fn now_iso8601_utc() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Tests pass `generated_at_utc` explicitly so this branch is only
    // exercised by humans running the importer interactively. We avoid
    // pulling in `chrono`/`time` for one call site by formatting the
    // Unix epoch ourselves.
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (year, month, day, hour, minute, second) = unix_to_civil(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Convert a Unix timestamp (seconds since 1970-01-01 UTC) into a civil
/// date-time tuple. Adapted from Howard Hinnant's date algorithms — no
/// external dependency.
fn unix_to_civil(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let hour = (secs_of_day / 3_600) as u32;
    let minute = ((secs_of_day % 3_600) / 60) as u32;
    let second = (secs_of_day % 60) as u32;

    // Days since 1970-01-01 → civil(year, month, day).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = (if month <= 2 { y + 1 } else { y }) as i32;
    (year, month, day, hour, minute, second)
}

/// Read a previously-written manifest. Convenience wrapper for callers
/// that want to inspect or re-serialise without depending on `serde_json`
/// directly.
pub fn read_manifest(path: &Path) -> Result<Manifest> {
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_tables_empty_returns_all() {
        let tables = select_tables(&[]).unwrap();
        assert_eq!(tables.len(), 4);
    }

    #[test]
    fn select_tables_filters_by_name_case_insensitive() {
        let tables = select_tables(&["NRMONTHALLOCATION".to_string()]).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].schema.name, "nrmonthallocation");
    }

    #[test]
    fn select_tables_unknown_name_errors() {
        let err = select_tables(&["nrbogus".to_string()]).unwrap_err();
        match err {
            Error::Internal { message } => assert!(message.contains("nrbogus")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn unix_to_civil_known_epochs() {
        // 1970-01-01T00:00:00Z
        assert_eq!(unix_to_civil(0), (1970, 1, 1, 0, 0, 0));
        // 2000-01-01T00:00:00Z = 946_684_800
        assert_eq!(unix_to_civil(946_684_800), (2000, 1, 1, 0, 0, 0));
        // 2024-02-29T12:34:56Z (leap day) = 1_709_210_096
        assert_eq!(unix_to_civil(1_709_210_096), (2024, 2, 29, 12, 34, 56));
    }
}
