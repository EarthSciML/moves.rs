//! Tool spec — TOML surface mirroring the Java `AVFTToolSpec`.
//!
//! The Java tool reads an XML `AVFTToolSpec` document with:
//!
//! * `lastCompleteModelYear` — model years up to (and including) this
//!   year come from the user input; later years are projected.
//! * `analysisYear` — the projection target.
//! * `methodEntries[]` — per-source-type `{enabled, gapFilling,
//!   projection}` triples.
//! * `inputAVFTFile`, `knownFractionsFile`, `outputAVFTFile` — file paths.
//!
//! The TOML form keeps the field names short and uses kebab-case enum
//! values for the gap-filling / projection methods. File paths are
//! handled by the CLI rather than embedded in the spec — this keeps the
//! spec relocatable and is the same shape the rest of `moves.rs` uses
//! (RunSpec TOML, the converter plan, etc.).
//!
//! Example:
//!
//! ```toml
//! last_complete_model_year = 2022
//! analysis_year = 2050
//!
//! [[method]]
//! source_type_id = 11
//! enabled = true
//! gap_filling = "automatic"
//! projection = "proportional"
//!
//! [[method]]
//! source_type_id = 21
//! enabled = true
//! gap_filling = "defaults-preserve-inputs"
//! projection = "known-fractions"
//! ```

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::model::{ModelYearId, SourceTypeId};

/// Canonical gap-filling method name strings, in the same wording the
/// Java GUI displays. Exposed for downstream tooling that wants to
/// log the resolved method.
pub const GAP_FILLING_AUTOMATIC: &str = "Automatic";
/// See [`GAP_FILLING_AUTOMATIC`].
pub const GAP_FILLING_DEFAULTS_RENORMALIZE_INPUTS: &str = "Use defaults, renormalize inputs";
/// See [`GAP_FILLING_AUTOMATIC`].
pub const GAP_FILLING_DEFAULTS_PRESERVE_INPUTS: &str = "Use defaults, preserve inputs";

/// Canonical projection method name strings, in the same wording the
/// Java GUI displays.
pub const PROJECTION_NATIONAL: &str = "National Average";
/// See [`PROJECTION_NATIONAL`].
pub const PROJECTION_PROPORTIONAL: &str = "Proportional";
/// See [`PROJECTION_NATIONAL`].
pub const PROJECTION_KNOWN: &str = "Known Fractions";
/// See [`PROJECTION_NATIONAL`].
pub const PROJECTION_CONSTANT: &str = "Constant";

/// Gap-filling method, applied per-source-type before projection.
///
/// See the corresponding SQL stored procedures in
/// `gov/epa/otaq/moves/master/gui/avfttool/AVFTTool.sql`:
///
/// * [`GapFillingMethod::Automatic`] → `AVFTTool_GapFilling_Automatic`
/// * [`GapFillingMethod::DefaultsRenormalizeInputs`] →
///   `AVFTTool_GapFilling_Defaults_Renormalize_Inputs`
/// * [`GapFillingMethod::DefaultsPreserveInputs`] →
///   `AVFTTool_GapFilling_Defaults_Preserve_Inputs`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GapFillingMethod {
    /// Fill missing model years with zeros (renormalizing the inputs
    /// first); fall back to defaults for source-type / model-year
    /// combinations the user did not supply at all.
    Automatic,
    /// Fill missing rows from the default AVFT, then rescale the user's
    /// rows so the full distribution sums to 1.
    DefaultsRenormalizeInputs,
    /// Fill missing rows from the default AVFT, then rescale the
    /// *default* rows so the full distribution sums to 1 — the user's
    /// values are kept as supplied.
    DefaultsPreserveInputs,
}

impl GapFillingMethod {
    /// Human-readable label matching the Java GUI strings.
    pub fn label(self) -> &'static str {
        match self {
            GapFillingMethod::Automatic => GAP_FILLING_AUTOMATIC,
            GapFillingMethod::DefaultsRenormalizeInputs => GAP_FILLING_DEFAULTS_RENORMALIZE_INPUTS,
            GapFillingMethod::DefaultsPreserveInputs => GAP_FILLING_DEFAULTS_PRESERVE_INPUTS,
        }
    }
}

/// Projection method, applied per-source-type after gap-filling.
///
/// See the corresponding SQL stored procedures in
/// `gov/epa/otaq/moves/master/gui/avfttool/AVFTTool.sql`:
///
/// * [`ProjectionMethod::Constant`] → `AVFTTool_Projection_Constant`
/// * [`ProjectionMethod::National`] → `AVFTTool_Projection_National`
/// * [`ProjectionMethod::Proportional`] → `AVFTTool_Projection_Proportional`
/// * [`ProjectionMethod::KnownFractions`] → `AVFTTool_Projection_KnownFractions`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProjectionMethod {
    /// Carry the last-complete-model-year row forward without change.
    Constant,
    /// Use the model-supplied default AVFT for the projection years.
    National,
    /// Scale each fuel/engine row by the user-vs-default ratio observed
    /// at `lastCompleteModelYear`, then renormalize.
    Proportional,
    /// Read explicit projection-year rows from a "known fractions" CSV.
    /// Default-derived rows fill gaps and renormalize alongside.
    KnownFractions,
}

impl ProjectionMethod {
    /// Human-readable label matching the Java GUI strings.
    pub fn label(self) -> &'static str {
        match self {
            ProjectionMethod::Constant => PROJECTION_CONSTANT,
            ProjectionMethod::National => PROJECTION_NATIONAL,
            ProjectionMethod::Proportional => PROJECTION_PROPORTIONAL,
            ProjectionMethod::KnownFractions => PROJECTION_KNOWN,
        }
    }
}

/// One source-type's entry in the tool spec.
///
/// `enabled = false` rows in the Java GUI are silently skipped (no
/// projection for that source type, no output rows emitted). The Rust
/// tool follows the same contract.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MethodEntry {
    pub source_type_id: SourceTypeId,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub gap_filling: GapFillingMethod,
    pub projection: ProjectionMethod,
}

fn default_enabled() -> bool {
    true
}

/// Complete tool spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    /// Inclusive upper bound for model years that come from the user
    /// input. Projection runs from `last_complete_model_year + 1`
    /// through `analysis_year`.
    pub last_complete_model_year: ModelYearId,
    /// Target year for the projection.
    pub analysis_year: ModelYearId,
    /// One row per source type the tool should process.
    #[serde(default, rename = "method")]
    pub methods: Vec<MethodEntry>,
}

impl ToolSpec {
    /// Parse a TOML string into a [`ToolSpec`].
    pub fn from_toml_str(input: &str) -> std::result::Result<Self, toml::de::Error> {
        toml::from_str(input)
    }

    /// Load a TOML file from disk.
    pub fn from_toml_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| Error::io(path, e))?;
        toml::from_str(&text).map_err(|source| Error::TomlParse {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Lookup the entry for a given source type.
    pub fn find(&self, source_type_id: SourceTypeId) -> Option<&MethodEntry> {
        self.methods
            .iter()
            .find(|m| m.source_type_id == source_type_id)
    }

    /// Validate the spec for internal consistency. Surfaces missing
    /// required fields (e.g., `analysis_year < last_complete_model_year`)
    /// before any database work happens.
    pub fn validate(&self) -> Result<()> {
        if self.last_complete_model_year < 1950 {
            return Err(Error::ToolSpec(format!(
                "last_complete_model_year={} is before 1950 (AVFT default DB starts at 1950)",
                self.last_complete_model_year
            )));
        }
        if self.analysis_year < self.last_complete_model_year {
            return Err(Error::ToolSpec(format!(
                "analysis_year={} is before last_complete_model_year={}",
                self.analysis_year, self.last_complete_model_year
            )));
        }
        // Duplicate source-type entries would be silently last-wins via
        // `find`; surface them instead.
        for (i, m) in self.methods.iter().enumerate() {
            if self.methods[..i]
                .iter()
                .any(|n| n.source_type_id == m.source_type_id)
            {
                return Err(Error::ToolSpec(format!(
                    "duplicate method entry for sourceTypeID={}",
                    m.source_type_id
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kebab_case_enums() {
        let toml = r#"
            last_complete_model_year = 2022
            analysis_year = 2050

            [[method]]
            source_type_id = 11
            enabled = true
            gap_filling = "automatic"
            projection = "proportional"

            [[method]]
            source_type_id = 21
            gap_filling = "defaults-preserve-inputs"
            projection = "known-fractions"
        "#;
        let spec: ToolSpec = toml::from_str(toml).unwrap();
        assert_eq!(spec.last_complete_model_year, 2022);
        assert_eq!(spec.analysis_year, 2050);
        assert_eq!(spec.methods.len(), 2);
        assert_eq!(spec.methods[0].source_type_id, 11);
        assert_eq!(spec.methods[0].gap_filling, GapFillingMethod::Automatic);
        assert_eq!(spec.methods[0].projection, ProjectionMethod::Proportional);
        // default enabled
        assert!(spec.methods[1].enabled);
        assert_eq!(
            spec.methods[1].gap_filling,
            GapFillingMethod::DefaultsPreserveInputs
        );
        assert_eq!(spec.methods[1].projection, ProjectionMethod::KnownFractions);
    }

    #[test]
    fn validate_rejects_analysis_before_last_complete() {
        let s = ToolSpec {
            last_complete_model_year: 2022,
            analysis_year: 2020,
            methods: vec![],
        };
        match s.validate() {
            Err(Error::ToolSpec(m)) => assert!(m.contains("analysis_year")),
            other => panic!("expected ToolSpec error, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_duplicate_source_type() {
        let s = ToolSpec {
            last_complete_model_year: 2022,
            analysis_year: 2050,
            methods: vec![
                MethodEntry {
                    source_type_id: 11,
                    enabled: true,
                    gap_filling: GapFillingMethod::Automatic,
                    projection: ProjectionMethod::Proportional,
                },
                MethodEntry {
                    source_type_id: 11,
                    enabled: true,
                    gap_filling: GapFillingMethod::Automatic,
                    projection: ProjectionMethod::Constant,
                },
            ],
        };
        match s.validate() {
            Err(Error::ToolSpec(m)) => assert!(m.contains("duplicate")),
            other => panic!("expected ToolSpec error, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_pre_1950_last_complete() {
        let s = ToolSpec {
            last_complete_model_year: 1949,
            analysis_year: 2050,
            methods: vec![],
        };
        match s.validate() {
            Err(Error::ToolSpec(m)) => assert!(m.contains("1950")),
            other => panic!("expected ToolSpec error, got {other:?}"),
        }
    }

    #[test]
    fn round_trips_through_toml() {
        let s = ToolSpec {
            last_complete_model_year: 2022,
            analysis_year: 2050,
            methods: vec![MethodEntry {
                source_type_id: 11,
                enabled: true,
                gap_filling: GapFillingMethod::Automatic,
                projection: ProjectionMethod::Proportional,
            }],
        };
        let text = toml::to_string(&s).unwrap();
        let parsed: ToolSpec = toml::from_str(&text).unwrap();
        assert_eq!(parsed, s);
    }
}
