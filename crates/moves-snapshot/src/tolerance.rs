//! TOML-based tolerance configuration for [`crate::diff::diff_snapshots`].
//!
//! File shape:
//!
//! ```toml
//! # Default applied to every Float64 column not overridden below.
//! default_float_tolerance = 1e-9
//!
//! # Per-column overrides. Table-name keys must be quoted because MOVES table
//! # names contain double underscores and dots in the snapshot layout.
//! [tables."db__movesoutput__movesoutput"]
//! emissionQuant = 1e-6
//! emissionRate  = 1e-6
//!
//! [tables."db__movesoutput__activityoutput"]
//! activity = 1e-3
//! ```
//!
//! The TOML is parsed into [`ToleranceConfig`] and converted into the
//! diff-side [`crate::diff::DiffOptions`] via [`From`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::diff::DiffOptions;

/// Errors specific to loading a tolerance configuration. Kept separate from
/// the snapshot-level [`crate::error::Error`] so the CLI can map TOML
/// failures to user-facing messages without conflating with snapshot I/O.
#[derive(Debug, thiserror::Error)]
pub enum ToleranceError {
    #[error("io error reading tolerance config at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parse error in tolerance config at {path}: {source}")]
    Toml {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error(
        "tolerance config at {path}: tolerance for {table:?}.{column:?} is {value} \
         (must be a finite, non-negative number)"
    )]
    NegativeTolerance {
        path: PathBuf,
        table: String,
        column: String,
        value: f64,
    },

    #[error(
        "tolerance config at {path}: default_float_tolerance is {value} \
         (must be a finite, non-negative number)"
    )]
    NegativeDefault { path: PathBuf, value: f64 },
}

/// Deserialized tolerance config.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct ToleranceConfig {
    /// Default absolute tolerance applied to every Float64 column unless
    /// overridden by [`Self::tables`]. `0.0` means strict equality.
    #[serde(default)]
    pub default_float_tolerance: f64,

    /// Per-table, per-column overrides. Outer key: table name. Inner map:
    /// column name -> absolute tolerance.
    #[serde(default)]
    pub tables: BTreeMap<String, BTreeMap<String, f64>>,
}

impl ToleranceConfig {
    /// Read and parse a TOML config from disk. Validates that every
    /// tolerance value is finite and non-negative.
    pub fn from_file(path: &Path) -> Result<Self, ToleranceError> {
        let bytes = std::fs::read(path).map_err(|source| ToleranceError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let text = String::from_utf8_lossy(&bytes);
        let cfg: ToleranceConfig =
            toml::from_str(&text).map_err(|source| ToleranceError::Toml {
                path: path.to_path_buf(),
                source,
            })?;
        cfg.validate(path)?;
        Ok(cfg)
    }

    fn validate(&self, path: &Path) -> Result<(), ToleranceError> {
        if !self.default_float_tolerance.is_finite() || self.default_float_tolerance < 0.0 {
            return Err(ToleranceError::NegativeDefault {
                path: path.to_path_buf(),
                value: self.default_float_tolerance,
            });
        }
        for (table, cols) in &self.tables {
            for (column, value) in cols {
                if !value.is_finite() || *value < 0.0 {
                    return Err(ToleranceError::NegativeTolerance {
                        path: path.to_path_buf(),
                        table: table.clone(),
                        column: column.clone(),
                        value: *value,
                    });
                }
            }
        }
        Ok(())
    }
}

impl From<ToleranceConfig> for DiffOptions {
    fn from(cfg: ToleranceConfig) -> Self {
        let mut opts =
            DiffOptions::default().with_default_float_tolerance(cfg.default_float_tolerance);
        for (table, cols) in cfg.tables {
            for (column, tolerance) in cols {
                opts = opts.with_column_tolerance(&table, &column, tolerance);
            }
        }
        opts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_tmp(contents: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn parses_default_only() {
        let f = write_tmp("default_float_tolerance = 1e-9\n");
        let cfg = ToleranceConfig::from_file(f.path()).unwrap();
        assert_eq!(cfg.default_float_tolerance, 1e-9);
        assert!(cfg.tables.is_empty());
    }

    #[test]
    fn parses_per_column() {
        let f = write_tmp(
            r#"
default_float_tolerance = 1e-9

[tables."db__movesoutput__movesoutput"]
emissionQuant = 1e-6
emissionRate = 5e-7

[tables."db__movesoutput__activityoutput"]
activity = 1e-3
"#,
        );
        let cfg = ToleranceConfig::from_file(f.path()).unwrap();
        assert_eq!(cfg.default_float_tolerance, 1e-9);
        let mo = &cfg.tables["db__movesoutput__movesoutput"];
        assert_eq!(mo["emissionQuant"], 1e-6);
        assert_eq!(mo["emissionRate"], 5e-7);
        assert_eq!(
            cfg.tables["db__movesoutput__activityoutput"]["activity"],
            1e-3
        );
    }

    #[test]
    fn empty_file_yields_strict_defaults() {
        let f = write_tmp("");
        let cfg = ToleranceConfig::from_file(f.path()).unwrap();
        assert_eq!(cfg.default_float_tolerance, 0.0);
        assert!(cfg.tables.is_empty());
    }

    #[test]
    fn negative_default_rejected() {
        let f = write_tmp("default_float_tolerance = -1.0\n");
        let err = ToleranceConfig::from_file(f.path()).unwrap_err();
        assert!(matches!(err, ToleranceError::NegativeDefault { .. }));
    }

    #[test]
    fn negative_per_column_rejected() {
        let f = write_tmp(
            r#"
[tables."t"]
v = -0.1
"#,
        );
        let err = ToleranceConfig::from_file(f.path()).unwrap_err();
        match err {
            ToleranceError::NegativeTolerance { table, column, .. } => {
                assert_eq!(table, "t");
                assert_eq!(column, "v");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn nan_default_rejected() {
        let f = write_tmp("default_float_tolerance = nan\n");
        let err = ToleranceConfig::from_file(f.path()).unwrap_err();
        assert!(matches!(err, ToleranceError::NegativeDefault { .. }));
    }

    #[test]
    fn malformed_toml_surfaces_path() {
        let f = write_tmp("default_float_tolerance = \n");
        let err = ToleranceConfig::from_file(f.path()).unwrap_err();
        match err {
            ToleranceError::Toml { path, .. } => assert_eq!(path, f.path()),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_file_surfaces_path() {
        let err = ToleranceConfig::from_file(Path::new("/no/such/file.toml")).unwrap_err();
        match err {
            ToleranceError::Io { path, .. } => assert_eq!(path, Path::new("/no/such/file.toml")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn into_diff_options_preserves_overrides() {
        let cfg = ToleranceConfig {
            default_float_tolerance: 1e-9,
            tables: BTreeMap::from([("t".to_string(), BTreeMap::from([("v".to_string(), 1e-6)]))]),
        };
        let opts: DiffOptions = cfg.into();
        // Smoke-test by constructing the opposite options manually.
        let manual = DiffOptions::default()
            .with_default_float_tolerance(1e-9)
            .with_column_tolerance("t", "v", 1e-6);
        assert_eq!(
            opts.per_column_tolerance.get(&("t".into(), "v".into())),
            manual.per_column_tolerance.get(&("t".into(), "v".into()))
        );
        assert_eq!(opts.default_float_tolerance, manual.default_float_tolerance);
    }
}
