//! `moves-cli` — the command-line driver that ties RunSpec parsing,
//! calculator execution, and input-database importing together.
//!
//! The `moves` binary lives in `src/main.rs`; this library surface holds
//! the reusable command logic so integration tests can exercise each
//! subcommand without spawning a subprocess.
//!
//! # Subcommands
//!
//! | Subcommand | Entry point | Backing crate(s) |
//! |------------|-------------|------------------|
//! | `run` | [`run_simulation`] | [`moves_framework`] (`MOVESEngine`) |
//! | `import-cdb` | [`import_cdb`] | [`moves_importer`] / [`moves_importer_county`] |
//! | `convert-runspec` | [`convert_runspec`] | [`moves_runspec`] |
//!
//! See `moves-rust-migration-plan.md`:
//!
//! * Task 11 — Workspace and project skeleton (this crate).
//! * Task 28 — CLI and end-to-end smoke test (this commit). Closes Phase 2.
//!
//! # Phase 2 status
//!
//! `run` drives the full [`moves_framework::MOVESEngine`] pipeline — parse
//! RunSpec, plan and chunk the calculator graph, walk one `MasterLoop` per
//! chunk, finalise the `OutputProcessor`. No Phase 3 calculators are ported
//! yet, so the engine reports every planned module as unimplemented and the
//! run produces an empty-but-correctly-shaped `MOVESRun.parquet`. The
//! `crates/moves-cli/tests/end_to_end.rs` smoke test exercises exactly that
//! path against `characterization/fixtures/sample-runspec.xml`.

mod convert;
mod import;
mod run;

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use moves_runspec::{from_toml_str, from_xml_str, RunSpec};

pub use convert::{convert_runspec, ConvertOptions, ConvertOutcome};
pub use import::{import_cdb, ImportOptions, ImportOutcome, ImportStatus, ImportedTableReport};
pub use run::{run_simulation, RunOptions};

/// Re-exported so callers (and the integration tests) can inspect a
/// [`run_simulation`] result without depending on `moves-framework` directly.
pub use moves_framework::EngineOutcome;

/// The two interchange formats a RunSpec can be stored in.
///
/// MOVES ships RunSpecs as XML; the Rust port adds a human-friendlier TOML
/// surface (migration-plan Task 13). The two are isomorphic — see
/// [`convert_runspec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunSpecFormat {
    /// Legacy MOVES XML (`.xml` / `.mrs`).
    Xml,
    /// The recommended TOML format (`.toml`).
    Toml,
}

impl RunSpecFormat {
    /// Infer the format from a path's extension: `xml` / `mrs` → [`Xml`],
    /// `toml` → [`Toml`]. Case-insensitive. `None` for anything else.
    ///
    /// [`Xml`]: RunSpecFormat::Xml
    /// [`Toml`]: RunSpecFormat::Toml
    #[must_use]
    pub fn from_path(path: &Path) -> Option<Self> {
        let ext = path.extension()?.to_str()?;
        if ext.eq_ignore_ascii_case("xml") || ext.eq_ignore_ascii_case("mrs") {
            Some(Self::Xml)
        } else if ext.eq_ignore_ascii_case("toml") {
            Some(Self::Toml)
        } else {
            None
        }
    }

    /// The other format — the target of an XML↔TOML conversion.
    #[must_use]
    pub fn opposite(self) -> Self {
        match self {
            Self::Xml => Self::Toml,
            Self::Toml => Self::Xml,
        }
    }

    /// The canonical file extension for this format (no leading dot).
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            Self::Xml => "xml",
            Self::Toml => "toml",
        }
    }

    /// A short human label for diagnostics.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Xml => "XML",
            Self::Toml => "TOML",
        }
    }
}

/// Read and parse a RunSpec file, selecting the parser from its extension.
///
/// # Errors
///
/// Fails if the extension is not a recognised RunSpec format, the file
/// cannot be read, or the contents do not parse as that format.
pub fn load_run_spec(path: &Path) -> Result<RunSpec> {
    let format = RunSpecFormat::from_path(path).with_context(|| {
        format!(
            "cannot infer RunSpec format from {}: expected a .xml, .mrs, or .toml extension",
            path.display()
        )
    })?;
    let text =
        fs::read_to_string(path).with_context(|| format!("reading RunSpec {}", path.display()))?;
    let spec = match format {
        RunSpecFormat::Xml => from_xml_str(&text),
        RunSpecFormat::Toml => from_toml_str(&text),
    }
    .with_context(|| format!("parsing {} RunSpec {}", format.label(), path.display()))?;
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn format_from_path_is_case_insensitive() {
        assert_eq!(
            RunSpecFormat::from_path(Path::new("a.xml")),
            Some(RunSpecFormat::Xml)
        );
        assert_eq!(
            RunSpecFormat::from_path(Path::new("A.XML")),
            Some(RunSpecFormat::Xml)
        );
        assert_eq!(
            RunSpecFormat::from_path(Path::new("legacy.mrs")),
            Some(RunSpecFormat::Xml)
        );
        assert_eq!(
            RunSpecFormat::from_path(Path::new("spec.toml")),
            Some(RunSpecFormat::Toml)
        );
        assert_eq!(RunSpecFormat::from_path(Path::new("spec.json")), None);
        assert_eq!(RunSpecFormat::from_path(Path::new("noext")), None);
    }

    #[test]
    fn format_opposite_and_extension_round_trip() {
        assert_eq!(RunSpecFormat::Xml.opposite(), RunSpecFormat::Toml);
        assert_eq!(RunSpecFormat::Toml.opposite(), RunSpecFormat::Xml);
        assert_eq!(RunSpecFormat::Xml.extension(), "xml");
        assert_eq!(RunSpecFormat::Toml.extension(), "toml");
    }

    #[test]
    fn load_run_spec_rejects_unknown_extension() {
        let err = load_run_spec(&PathBuf::from("/nonexistent/spec.json")).unwrap_err();
        assert!(
            err.to_string().contains("cannot infer RunSpec format"),
            "got: {err}"
        );
    }

    #[test]
    fn load_run_spec_reports_missing_file() {
        let err = load_run_spec(&PathBuf::from("/nonexistent/spec.xml")).unwrap_err();
        assert!(err.to_string().contains("reading RunSpec"), "got: {err}");
    }
}
