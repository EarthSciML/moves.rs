//! Top-level orchestration: read CSV → validate → write Parquet.

use std::path::{Path, PathBuf};

use crate::csv_reader;
use crate::error::Result;
use crate::parquet_writer::{encode, write_atomic, ParquetOutput};
use crate::schema::LevKind;
use crate::validate::validate;

/// Summary of a successful import run.
#[derive(Debug, Clone)]
pub struct ImportReport {
    /// Which alternative-rate table was written.
    pub kind: LevKind,
    /// Absolute path of the input CSV.
    pub input_path: PathBuf,
    /// Absolute path of the Parquet file produced.
    pub output_path: PathBuf,
    /// Number of data rows written to Parquet.
    pub row_count: u64,
    /// SHA-256 of the written Parquet bytes (lower-case hex).
    pub sha256: String,
}

/// Import a LEV (`EmissionRateByAgeLEV`) CSV. Writes a single Parquet
/// file at `<output_dir>/EmissionRateByAgeLEV/EmissionRateByAgeLEV.parquet`.
pub fn import_lev(input_csv: &Path, output_dir: &Path) -> Result<ImportReport> {
    run(LevKind::Lev, input_csv, output_dir)
}

/// Import an NLEV (`EmissionRateByAgeNLEV`) CSV. Writes a single
/// Parquet file at `<output_dir>/EmissionRateByAgeNLEV/EmissionRateByAgeNLEV.parquet`.
pub fn import_nlev(input_csv: &Path, output_dir: &Path) -> Result<ImportReport> {
    run(LevKind::Nlev, input_csv, output_dir)
}

/// Import either kind, selected by `kind`. The CSV columns and
/// validation rules are identical for both.
pub fn import(kind: LevKind, input_csv: &Path, output_dir: &Path) -> Result<ImportReport> {
    run(kind, input_csv, output_dir)
}

fn run(kind: LevKind, input_csv: &Path, output_dir: &Path) -> Result<ImportReport> {
    let csv = csv_reader::read(input_csv)?;
    let rows = validate(&csv)?;
    let encoded = encode(&rows)?;
    let output_path = parquet_path_for(kind, output_dir);
    write_atomic(&output_path, &encoded.bytes)?;
    Ok(report(kind, input_csv, output_path, &encoded))
}

/// Canonical Parquet path for a given kind, anchored at `output_dir`.
/// The layout matches the converter's monolithic-table layout so the
/// import output can be opened by `moves-data-default` once the
/// importer also writes a manifest (TBD: integration with Task 24's
/// `InputDataManager`).
#[must_use]
pub fn parquet_path_for(kind: LevKind, output_dir: &Path) -> PathBuf {
    let table = kind.table_name();
    output_dir.join(table).join(format!("{table}.parquet"))
}

fn report(
    kind: LevKind,
    input_csv: &Path,
    output_path: PathBuf,
    encoded: &ParquetOutput,
) -> ImportReport {
    ImportReport {
        kind,
        input_path: input_csv.to_path_buf(),
        output_path,
        row_count: encoded.row_count,
        sha256: encoded.sha256.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parquet_path_for_lev_matches_default_db_layout() {
        let p = parquet_path_for(LevKind::Lev, Path::new("/out"));
        assert_eq!(
            p,
            Path::new("/out/EmissionRateByAgeLEV/EmissionRateByAgeLEV.parquet")
        );
    }

    #[test]
    fn parquet_path_for_nlev_matches_default_db_layout() {
        let p = parquet_path_for(LevKind::Nlev, Path::new("/out"));
        assert_eq!(
            p,
            Path::new("/out/EmissionRateByAgeNLEV/EmissionRateByAgeNLEV.parquet")
        );
    }
}
