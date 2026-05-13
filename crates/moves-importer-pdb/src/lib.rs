//! `moves-importer-pdb` — Project-scale (PDB) input-database importer.
//!
//! Phase 4 Task 84 of the Rust port. Reads the project-scale CSV
//! tables a MOVES user authors against the PDB template, validates
//! per-cell types and (against an optional [`RunSpecFilter`])
//! membership constraints, runs the cross-row invariants Java
//! enforces inside `getProjectDataStatus`, and writes Parquet that
//! matches the default-DB schema for the same tables.
//!
//! ## Scope (Java parity)
//!
//! Java's `ImporterInstantiator` tags five importers as
//! project-only (`|project|` purpose without `|county|`):
//!
//! | Java importer                     | Table                     | Module                                       |
//! |-----------------------------------|---------------------------|----------------------------------------------|
//! | `LinkImporter`                    | `Link`                    | [`tables::link`]                              |
//! | `LinkSourceTypeHourImporter`      | `linkSourceTypeHour`      | [`tables::link_source_type_hour`]             |
//! | `DriveScheduleSecondLinkImporter` | `driveScheduleSecondLink` | [`tables::drive_schedule_second_link`]        |
//! | `OffNetworkLinkImporter`          | `offNetworkLink`          | [`tables::off_network_link`]                  |
//! | `LinkOpmodeDistributionImporter`  | `OpModeDistribution`      | [`tables::op_mode_distribution`]              |
//!
//! The other importers Java reuses across project + county domains
//! (`AgeDistribution`, `Fuel`, `Meteorology`, `Zone`, `Hotelling`,
//! `IM`, `OnRoadRetrofit`, `Generic`, `AVFT`) belong to the CDB
//! importer crate (Task 83) — they share the same `BasicDataHandler`
//! plumbing, so once that lands the per-table modules will consume the
//! same [`csv_reader`] and [`parquet_writer`] this crate already
//! exposes.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::path::Path;
//! use moves_importer_pdb::{filter::RunSpecFilter, ImportSession};
//!
//! let runspec = RunSpecFilter::default()
//!     .with_counties([26161])
//!     .with_zones([261610]);
//!
//! let session = ImportSession::builder("/tmp/pdb-out", &runspec)
//!     .with_link("/tmp/link.csv")
//!     .with_link_source_type_hour("/tmp/link_source_type_hour.csv")
//!     .with_drive_schedule_second_link("/tmp/drive_schedule.csv")
//!     .build()?;
//! let manifest = session.write_to_disk()?;
//! println!("loaded {} tables", manifest.tables.len());
//! # Ok::<(), moves_importer_pdb::Error>(())
//! ```
//!
//! ## What we don't replicate
//!
//! * **Excel input.** Java's `CellFileReader` reads `.xls`/`.xlsx`
//!   transparently via Apache POI. We support CSV only.
//! * **GUI template generation.** The Java importers emit
//!   pre-populated CSV templates the user fills in. That's a CLI
//!   concern — see Phase 5+ tooling.
//! * **MariaDB `LOAD DATA INFILE`.** Java loads into a temporary
//!   MariaDB instance after CSV parsing. The Rust port writes
//!   straight to Parquet; the runtime reader (`moves-data-default`,
//!   Task 82) consumes that Parquet directly.

pub mod csv_reader;
pub mod error;
pub mod filter;
pub mod manifest;
pub mod parquet_writer;
pub mod schema;
pub mod tables;
pub mod validate;

pub use crate::csv_reader::{ImportReport, ImportWarning};
pub use crate::error::{Error, Result};
pub use crate::filter::{Filter, RunSpecFilter};
pub use crate::manifest::{Manifest, TableManifest};
pub use crate::parquet_writer::ParquetOutput;
pub use crate::schema::TableSchema;

use std::path::{Path, PathBuf};

/// Single-pass importer: collect zero or more table-CSV pairs, write
/// them all to a single Parquet output directory, and produce a
/// [`Manifest`].
///
/// Construction goes through [`ImportSessionBuilder`] so callers can
/// mix and match (a project can omit, e.g., `OpModeDistribution` if
/// the run has no off-network link). The builder validates that at
/// least one table was selected — a session with zero tables is
/// almost certainly a misconfiguration.
#[derive(Debug)]
pub struct ImportSession<'a> {
    output_root: PathBuf,
    runspec: &'a RunSpecFilter,
    plan: Vec<PlanEntry>,
}

#[derive(Debug, Clone)]
struct PlanEntry {
    table_name: &'static str,
    schema: &'static TableSchema,
    source_path: PathBuf,
}

impl<'a> ImportSession<'a> {
    /// Begin building a session. `output_root` is the directory that
    /// will receive `<table>.parquet` files plus `manifest.json`. The
    /// directory is created on `write_to_disk` if it doesn't exist.
    pub fn builder<P: Into<PathBuf>>(
        output_root: P,
        runspec: &'a RunSpecFilter,
    ) -> ImportSessionBuilder<'a> {
        ImportSessionBuilder {
            output_root: output_root.into(),
            runspec,
            plan: Vec::new(),
        }
    }

    /// Read every planned CSV, write the Parquet files atomically,
    /// and write `manifest.json` alongside them. Returns the manifest.
    pub fn write_to_disk(&self) -> Result<Manifest> {
        std::fs::create_dir_all(&self.output_root).map_err(|source| Error::Io {
            path: self.output_root.clone(),
            source,
        })?;
        let mut tables = Vec::with_capacity(self.plan.len());
        for entry in &self.plan {
            let report = csv_reader::read_csv(&entry.source_path, entry.schema, self.runspec)?;
            let parquet = parquet_writer::encode(&report.batch)?;
            let output_path = self
                .output_root
                .join(format!("{}.parquet", entry.table_name));
            parquet_writer::write_atomic(&output_path, &parquet.bytes)?;
            tables.push(TableManifest {
                name: entry.table_name.to_string(),
                source_path: entry.source_path.clone(),
                output_path,
                sha256: parquet.sha256,
                row_count: parquet.row_count,
                warnings: report.warnings,
            });
        }
        let manifest = Manifest::new(tables);
        let manifest_path = self.output_root.join(manifest::MANIFEST_FILENAME);
        let json = serde_json::to_vec_pretty(&manifest)?;
        std::fs::write(&manifest_path, &json).map_err(|source| Error::Io {
            path: manifest_path,
            source,
        })?;
        Ok(manifest)
    }
}

/// Builder for [`ImportSession`].
pub struct ImportSessionBuilder<'a> {
    output_root: PathBuf,
    runspec: &'a RunSpecFilter,
    plan: Vec<PlanEntry>,
}

impl<'a> ImportSessionBuilder<'a> {
    pub fn with_link<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.plan.push(PlanEntry {
            table_name: schema::LINK.name,
            schema: &schema::LINK,
            source_path: path.into(),
        });
        self
    }

    pub fn with_link_source_type_hour<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.plan.push(PlanEntry {
            table_name: schema::LINK_SOURCE_TYPE_HOUR.name,
            schema: &schema::LINK_SOURCE_TYPE_HOUR,
            source_path: path.into(),
        });
        self
    }

    pub fn with_drive_schedule_second_link<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.plan.push(PlanEntry {
            table_name: schema::DRIVE_SCHEDULE_SECOND_LINK.name,
            schema: &schema::DRIVE_SCHEDULE_SECOND_LINK,
            source_path: path.into(),
        });
        self
    }

    pub fn with_off_network_link<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.plan.push(PlanEntry {
            table_name: schema::OFF_NETWORK_LINK.name,
            schema: &schema::OFF_NETWORK_LINK,
            source_path: path.into(),
        });
        self
    }

    pub fn with_op_mode_distribution<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.plan.push(PlanEntry {
            table_name: schema::OP_MODE_DISTRIBUTION.name,
            schema: &schema::OP_MODE_DISTRIBUTION,
            source_path: path.into(),
        });
        self
    }

    pub fn build(self) -> Result<ImportSession<'a>> {
        if self.plan.is_empty() {
            return Err(Error::Validation {
                table: "(session)".into(),
                message: "no tables selected for import; pass at least one with_* call".into(),
            });
        }
        Ok(ImportSession {
            output_root: self.output_root,
            runspec: self.runspec,
            plan: self.plan,
        })
    }
}

/// Convenience: read the in-memory representation of one table CSV
/// without involving the Parquet writer or manifest. Useful for
/// callers that want to chain validation steps before deciding to
/// commit.
pub fn read_table(
    path: &Path,
    schema: &TableSchema,
    runspec: &RunSpecFilter,
) -> Result<ImportReport> {
    csv_reader::read_csv(path, schema, runspec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    fn write_csv(content: &str) -> NamedTempFile {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "{content}").unwrap();
        tmp
    }

    #[test]
    fn empty_session_is_rejected() {
        let runspec = RunSpecFilter::default();
        let err = ImportSession::builder("/tmp/x", &runspec)
            .build()
            .unwrap_err();
        match err {
            Error::Validation { message, .. } => assert!(message.contains("no tables")),
            other => panic!("wanted Validation, got {other:?}"),
        }
    }

    #[test]
    fn full_three_table_round_trip() {
        let dir = TempDir::new().unwrap();
        let link = write_csv(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,a,0
2,26161,261610,2,1.0,500,40,b,0
",
        );
        let lsth = write_csv(
            "linkID,sourceTypeID,sourceTypeHourFraction
1,21,1.0
2,21,1.0
",
        );
        let ds = write_csv(
            "linkID,secondID,speed,grade
1,0,0,0
1,1,5.5,0.5
",
        );
        let runspec = RunSpecFilter::default();
        let session = ImportSession::builder(dir.path(), &runspec)
            .with_link(link.path())
            .with_link_source_type_hour(lsth.path())
            .with_drive_schedule_second_link(ds.path())
            .build()
            .unwrap();
        let manifest = session.write_to_disk().unwrap();
        assert_eq!(manifest.tables.len(), 3);
        for entry in &manifest.tables {
            assert!(
                entry.output_path.exists(),
                "missing {:?}",
                entry.output_path
            );
            assert_eq!(entry.sha256.len(), 64);
        }
        assert!(dir.path().join("manifest.json").exists());
    }

    #[test]
    fn manifest_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let link = write_csv(
            "linkID,countyID,zoneID,roadTypeID,linkLength,linkVolume,linkAvgSpeed,linkDescription,linkAvgGrade
1,26161,261610,4,0.5,1000,55,a,0
",
        );
        let runspec = RunSpecFilter::default();
        let session = ImportSession::builder(dir.path(), &runspec)
            .with_link(link.path())
            .build()
            .unwrap();
        let written = session.write_to_disk().unwrap();
        let json = std::fs::read_to_string(dir.path().join("manifest.json")).unwrap();
        let read_back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(written, read_back);
    }
}
