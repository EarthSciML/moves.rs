//! Emission-factor-files dispatcher (`rdefls.f`).
//!
//! Task 97. The Fortran `rdefls.f` opens every per-pollutant emission
//! factor file (and per-pollutant deterioration file), then calls:
//!
//! - `rdemfc` for exhaust pollutants `IDXTHC..=IDXCRA`,
//! - `rdspil` (this crate's [`super::spillage`]) when the evap
//!   pollutant index is `IDXSPL`,
//! - `rdevemfc` for the remaining evap pollutants,
//! - `rddetr` (this crate's [`super::deterioration`]) for any
//!   deterioration files configured.
//!
//! The Rust port models the dispatch table here. Callers pass a
//! [`PollutantBundle`] with optional file paths; we route each present
//! file to the right parser and collect the results into [`EmissionFactorTables`].
//!
//! Note: until [`super::emfc`] (Task 96) lands, exhaust and evap
//! tables remain empty and the dispatcher only loads spillage and
//! deterioration data.
//!
//! # Fortran source
//!
//! Ports `rdefls.f` (228 lines).

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use crate::{Error, Result};

use super::deterioration::{read_detr, DeteriorationRecord};
use super::spillage::{read_spil, SpillageRecord};

/// Per-pollutant input bundle.
#[derive(Debug, Default, Clone)]
pub struct PollutantBundle {
    /// Pollutant name (e.g., `THC`, `CO`, `NOX`).
    pub name: String,
    /// Optional emission-factor file path (`facfl(idxpol)` in Fortran).
    pub factor_file: Option<PathBuf>,
    /// Optional deterioration file path (`detfl(idxpol)`).
    pub det_file: Option<PathBuf>,
    /// Whether the pollutant is the spillage pollutant (`IDXSPL`).
    pub is_spillage: bool,
}

/// Loaded emission-factor data, organized by pollutant.
#[derive(Debug, Default, Clone)]
pub struct EmissionFactorTables {
    /// Spillage records (only populated for the `IDXSPL` pollutant).
    pub spillage: Vec<SpillageRecord>,
    /// Deterioration records, keyed by pollutant name.
    pub deterioration: HashMap<String, Vec<DeteriorationRecord>>,
    /// Pollutants whose factor file was non-empty but skipped because
    /// the EMF parser (Task 96) is not yet wired.
    pub deferred_emf: Vec<String>,
}

/// Dispatch over a list of pollutant bundles, loading every file we
/// already know how to parse.
pub fn load_emission_factors(bundles: &[PollutantBundle]) -> Result<EmissionFactorTables> {
    let mut tables = EmissionFactorTables::default();

    for bundle in bundles {
        if let Some(path) = bundle.factor_file.as_ref() {
            if bundle.is_spillage {
                let reader = open_reader(path)?;
                tables.spillage.extend(read_spil(reader)?);
            } else {
                tables.deferred_emf.push(bundle.name.clone());
            }
        }
        if let Some(path) = bundle.det_file.as_ref() {
            let reader = open_reader(path)?;
            tables
                .deterioration
                .insert(bundle.name.clone(), read_detr(reader)?);
        }
    }

    Ok(tables)
}

fn open_reader(path: &std::path::Path) -> Result<BufReader<File>> {
    let file = File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(BufReader::new(file))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp(contents: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn dispatches_to_deterioration_parser() {
        let det = write_temp("/DETFAC/\nBASE 0.04 1.0 1.5 HC\n/END/\n");
        let bundles = vec![PollutantBundle {
            name: "HC".into(),
            factor_file: None,
            det_file: Some(det.path().to_path_buf()),
            is_spillage: false,
        }];
        let tables = load_emission_factors(&bundles).unwrap();
        assert_eq!(tables.deterioration["HC"].len(), 1);
        assert!(tables.deferred_emf.is_empty());
    }

    #[test]
    fn dispatches_to_spillage_parser() {
        // Re-use the spillage module's sample format: 30 fields per record.
        let record = "2270001000 PUMP TNK 0.0 25.0 BASE GALLONS \
0.5 0.6 70.0 1.0 0.5 50.0 0.0 0.0 \
0.0 0.0 0.0 0.0 0.05 0.2 0.2 0.2 0.2 0.2 \
0.0 1.5 1.0 1.0 1.0";
        let body = format!("/EMSFAC/\n{}\n/END/\n", record);
        let f = write_temp(&body);

        let bundles = vec![PollutantBundle {
            name: "SPILLAGE".into(),
            factor_file: Some(f.path().to_path_buf()),
            det_file: None,
            is_spillage: true,
        }];
        let tables = load_emission_factors(&bundles).unwrap();
        assert_eq!(tables.spillage.len(), 1);
    }

    #[test]
    fn defers_emf_until_task_96() {
        let f = write_temp("/EMSFAC/\n/END/\n");
        let bundles = vec![PollutantBundle {
            name: "THC".into(),
            factor_file: Some(f.path().to_path_buf()),
            det_file: None,
            is_spillage: false,
        }];
        let tables = load_emission_factors(&bundles).unwrap();
        assert_eq!(tables.deferred_emf, vec!["THC"]);
        assert!(tables.spillage.is_empty());
    }
}
