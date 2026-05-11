//! Emission-factor-files dispatcher (`rdefls.f`).
//!
//! Task 97 introduced this dispatcher; Task 96 wires it up to the
//! exhaust ([`super::emfc`]) and evap ([`super::evemfc`]) emission
//! factor parsers. The Fortran `rdefls.f` opens every
//! per-pollutant emission factor file (and per-pollutant
//! deterioration file), then calls:
//!
//! - `rdemfc` for exhaust pollutants `IDXTHC..=IDXCRA`,
//! - `rdspil` (this crate's [`super::spillage`]) when the evap
//!   pollutant index is `IDXSPL`,
//! - `rdevemfc` for the remaining evap pollutants,
//! - `rddetr` (this crate's [`super::deterioration`]) for any
//!   deterioration files configured.
//!
//! Callers pass a [`PollutantBundle`] per pollutant; each bundle
//! carries the pollutant name, an optional emission-factor file
//! path, an optional deterioration file path, and a
//! [`PollutantKind`] that drives the dispatch + unit-policy
//! decisions.
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
use super::emfc::{read_emf, EmissionFactorRecord};
use super::evemfc::{read_evemf, EvapEmissionFactorRecord, EvapPollutantKind};
use super::spillage::{read_spil, SpillageRecord};

/// Which parser to dispatch a pollutant's emission-factor file to.
///
/// Mirrors the index-range branches at `rdefls.f` and
/// `rdemfc.f` :173 / `rdevemfc.f` :177-180.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollutantKind {
    /// Exhaust pollutant — dispatched to [`read_emf`].
    Exhaust {
        /// `true` for the crankcase HC pollutant (`IDXCRA`),
        /// which triggers the `MULT`-units rule at
        /// `rdemfc.f` :173.
        is_crankcase: bool,
    },
    /// Evap pollutant — dispatched to [`read_evemf`] with the
    /// unit policy encoded by [`EvapPollutantKind`].
    Evap(EvapPollutantKind),
    /// Spillage — dispatched to [`read_spil`].
    Spillage,
}

/// Per-pollutant input bundle.
#[derive(Debug, Clone)]
pub struct PollutantBundle {
    /// Pollutant name (e.g., `THC`, `CO`, `NOX`, `CRA`, `DIU`).
    pub name: String,
    /// Optional emission-factor file path
    /// (`facfl(idxpol)` in Fortran).
    pub factor_file: Option<PathBuf>,
    /// Optional deterioration file path (`detfl(idxpol)`).
    pub det_file: Option<PathBuf>,
    /// Which parser to use for this pollutant.
    pub kind: PollutantKind,
}

/// Loaded emission-factor data, organized by pollutant.
#[derive(Debug, Default, Clone)]
pub struct EmissionFactorTables {
    /// Exhaust emission-factor records, keyed by pollutant name.
    pub exhaust: HashMap<String, Vec<EmissionFactorRecord>>,
    /// Evap emission-factor records, keyed by pollutant name.
    pub evap: HashMap<String, Vec<EvapEmissionFactorRecord>>,
    /// Spillage records (populated for the spillage pollutant).
    pub spillage: Vec<SpillageRecord>,
    /// Deterioration records, keyed by pollutant name.
    pub deterioration: HashMap<String, Vec<DeteriorationRecord>>,
}

/// Dispatch over a list of pollutant bundles, parsing each
/// present file with the right reader and collecting the results.
pub fn load_emission_factors(bundles: &[PollutantBundle]) -> Result<EmissionFactorTables> {
    let mut tables = EmissionFactorTables::default();

    for bundle in bundles {
        if let Some(path) = bundle.factor_file.as_ref() {
            let reader = open_reader(path)?;
            match bundle.kind {
                PollutantKind::Spillage => {
                    tables.spillage.extend(read_spil(reader)?);
                }
                PollutantKind::Exhaust { is_crankcase } => {
                    let records = read_emf(reader, &bundle.name, is_crankcase)?;
                    tables.exhaust.insert(bundle.name.clone(), records);
                }
                PollutantKind::Evap(kind) => {
                    let records = read_evemf(reader, &bundle.name, kind)?;
                    tables.evap.insert(bundle.name.clone(), records);
                }
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

    fn at(spec: &[(usize, &str)]) -> String {
        let mut out = String::new();
        for (col, value) in spec {
            let col0 = col.saturating_sub(1);
            while out.len() < col0 {
                out.push(' ');
            }
            out.push_str(value);
        }
        out
    }

    #[test]
    fn dispatches_to_deterioration_parser() {
        let det = write_temp("/DETFAC/\nBASE 0.04 1.0 1.5 HC\n/END/\n");
        let bundles = vec![PollutantBundle {
            name: "HC".into(),
            factor_file: None,
            det_file: Some(det.path().to_path_buf()),
            kind: PollutantKind::Exhaust {
                is_crankcase: false,
            },
        }];
        let tables = load_emission_factors(&bundles).unwrap();
        assert_eq!(tables.deterioration["HC"].len(), 1);
        assert!(tables.exhaust.is_empty());
    }

    #[test]
    fn dispatches_to_spillage_parser() {
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
            kind: PollutantKind::Spillage,
        }];
        let tables = load_emission_factors(&bundles).unwrap();
        assert_eq!(tables.spillage.len(), 1);
    }

    #[test]
    fn dispatches_to_exhaust_emf_parser() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "BASE      "),
            (45, "G/HP-HR   "),
            (55, "THC       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.30")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let f = write_temp(&body);

        let bundles = vec![PollutantBundle {
            name: "THC".into(),
            factor_file: Some(f.path().to_path_buf()),
            det_file: None,
            kind: PollutantKind::Exhaust {
                is_crankcase: false,
            },
        }];
        let tables = load_emission_factors(&bundles).unwrap();
        let recs = tables.exhaust.get("THC").unwrap();
        assert_eq!(recs.len(), 1);
        assert!((recs[0].factor - 0.30).abs() < 1e-6);
        assert!(tables.evap.is_empty());
    }

    #[test]
    fn dispatches_to_evap_emf_parser() {
        let header = at(&[
            (6, "2270001000"),
            (21, " 25.0"),
            (26, " 50.0"),
            (35, "E00000000 "),
            (45, "MULT      "),
            (55, "DIU       "),
        ]);
        let data = at(&[(1, " 2010"), (35, "      0.05")]);
        let body = format!("/EMSFAC/\n{header}\n{data}\n/END/\n");
        let f = write_temp(&body);

        let bundles = vec![PollutantBundle {
            name: "DIU".into(),
            factor_file: Some(f.path().to_path_buf()),
            det_file: None,
            kind: PollutantKind::Evap(EvapPollutantKind::Diurnal),
        }];
        let tables = load_emission_factors(&bundles).unwrap();
        let recs = tables.evap.get("DIU").unwrap();
        assert_eq!(recs.len(), 1);
        assert!(tables.exhaust.is_empty());
    }
}
