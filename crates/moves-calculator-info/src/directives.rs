//! Parser for `CalculatorInfo.txt`.
//!
//! The format is the one written by
//! `gov.epa.otaq.moves.master.framework.InterconnectionTracker`. Comment
//! lines begin `// ` and document the column layout; data lines are
//! tab-separated and use the type tag in the first column to discriminate
//! between the three directive shapes.
//!
//! ```text
//! Registration\tOutputPollutantName\tOutputPollutantID\tProcessName\tProcessID\tModuleName
//! Subscribe   \tModuleName         \tProcessName      \tProcessID \tGranularity\tPriority
//! Chain       \tOutputModuleName   \tInputModuleName
//! ```
//!
//! The parser is strict about column counts but tolerates trailing
//! whitespace and BOMs. Unknown leading tags are an error.

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::loop_meta::{Granularity, Priority};

/// Source location of a directive — file path + 1-based line number. Used
/// for error reporting and for stable cross-references in the output JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveLocation {
    pub line: usize,
}

/// `Registration\tPollutantName\tPollutantID\tProcessName\tProcessID\tCalculator`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationDirective {
    pub pollutant_name: String,
    pub pollutant_id: u32,
    pub process_name: String,
    pub process_id: u32,
    pub calculator: String,
    pub location: DirectiveLocation,
}

/// `Subscribe\tModule\tProcessName\tProcessID\tGranularity\tPriority`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeDirective {
    pub module: String,
    pub process_name: String,
    pub process_id: u32,
    pub granularity: Granularity,
    pub priority: Priority,
    pub location: DirectiveLocation,
}

/// `Chain\tOutput\tInput` — the output module's results require the input
/// module's results (per `InterconnectionTracker.recordChain`'s contract).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainDirective {
    /// The downstream consumer (the calculator that needs the input's
    /// output to do its own work).
    pub output: String,
    /// The upstream producer (the calculator whose results are consumed).
    pub input: String,
    pub location: DirectiveLocation,
}

/// Aggregate of every directive read from a single `CalculatorInfo.txt`,
/// preserving file-order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalculatorInfo {
    pub registrations: Vec<RegistrationDirective>,
    pub subscribes: Vec<SubscribeDirective>,
    pub chains: Vec<ChainDirective>,
    /// SHA-256 of the input file, lowercase hex, for provenance.
    pub source_sha256: String,
}

/// Read and parse a `CalculatorInfo.txt`. The returned [`CalculatorInfo`]
/// preserves the order directives appeared in the file.
pub fn parse_calculator_info(path: &Path) -> Result<CalculatorInfo> {
    let bytes = fs::read(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let text = std::str::from_utf8(&bytes).map_err(|e| Error::Directive {
        path: path.to_path_buf(),
        line: 0,
        message: format!("not valid UTF-8: {e}"),
    })?;
    parse_calculator_info_str(text, path)
}

/// Parse an in-memory `CalculatorInfo.txt`. The `path` is only used for
/// error messages and provenance; the SHA-256 is taken from `text`'s bytes.
pub fn parse_calculator_info_str(text: &str, path: &Path) -> Result<CalculatorInfo> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let source_sha256 = format!("{:x}", hasher.finalize());

    let mut registrations = Vec::new();
    let mut subscribes = Vec::new();
    let mut chains = Vec::new();

    for (idx, raw_line) in text.lines().enumerate() {
        let line_no = idx + 1;
        // Strip optional UTF-8 BOM on the first line.
        let trimmed_bom = if line_no == 1 {
            raw_line.strip_prefix('\u{feff}').unwrap_or(raw_line)
        } else {
            raw_line
        };
        let line = trimmed_bom.trim_end();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        let fields: Vec<&str> = line.split('\t').collect();
        let tag = fields[0];

        match tag {
            "Registration" => {
                let dir = parse_registration(&fields, path, line_no)?;
                registrations.push(dir);
            }
            "Subscribe" => {
                let dir = parse_subscribe(&fields, path, line_no)?;
                subscribes.push(dir);
            }
            "Chain" => {
                let dir = parse_chain(&fields, path, line_no)?;
                chains.push(dir);
            }
            _ => {
                return Err(Error::Directive {
                    path: path.to_path_buf(),
                    line: line_no,
                    message: format!("unknown directive tag '{tag}'"),
                });
            }
        }
    }

    Ok(CalculatorInfo {
        registrations,
        subscribes,
        chains,
        source_sha256,
    })
}

fn parse_registration(fields: &[&str], path: &Path, line: usize) -> Result<RegistrationDirective> {
    if fields.len() != 6 {
        return Err(Error::Directive {
            path: path.to_path_buf(),
            line,
            message: format!(
                "Registration expected 6 tab-separated fields, got {}",
                fields.len()
            ),
        });
    }
    let pollutant_name = fields[1].to_string();
    let pollutant_id = parse_u32(fields[2], path, line, "OutputPollutantID")?;
    let process_name = fields[3].to_string();
    let process_id = parse_u32(fields[4], path, line, "ProcessID")?;
    let calculator = fields[5].to_string();
    Ok(RegistrationDirective {
        pollutant_name,
        pollutant_id,
        process_name,
        process_id,
        calculator,
        location: DirectiveLocation { line },
    })
}

fn parse_subscribe(fields: &[&str], path: &Path, line: usize) -> Result<SubscribeDirective> {
    if fields.len() != 6 {
        return Err(Error::Directive {
            path: path.to_path_buf(),
            line,
            message: format!(
                "Subscribe expected 6 tab-separated fields, got {}",
                fields.len()
            ),
        });
    }
    let module = fields[1].to_string();
    let process_name = fields[2].to_string();
    let process_id = parse_u32(fields[3], path, line, "ProcessID")?;
    let granularity = Granularity::from_str(fields[4]).map_err(|_| Error::UnknownGranularity {
        path: path.to_path_buf(),
        line,
        value: fields[4].to_string(),
    })?;
    let priority = Priority::parse(fields[5]).ok_or_else(|| Error::UnknownPriority {
        path: path.to_path_buf(),
        line,
        value: fields[5].to_string(),
    })?;
    Ok(SubscribeDirective {
        module,
        process_name,
        process_id,
        granularity,
        priority,
        location: DirectiveLocation { line },
    })
}

fn parse_chain(fields: &[&str], path: &Path, line: usize) -> Result<ChainDirective> {
    if fields.len() != 3 {
        return Err(Error::Directive {
            path: path.to_path_buf(),
            line,
            message: format!(
                "Chain expected 3 tab-separated fields, got {}",
                fields.len()
            ),
        });
    }
    Ok(ChainDirective {
        output: fields[1].to_string(),
        input: fields[2].to_string(),
        location: DirectiveLocation { line },
    })
}

fn parse_u32(s: &str, path: &Path, line: usize, label: &str) -> Result<u32> {
    s.parse::<u32>().map_err(|_| Error::Directive {
        path: path.to_path_buf(),
        line,
        message: format!("{label} '{s}' is not a non-negative integer"),
    })
}

impl CalculatorInfo {
    /// Convenience: empty value for tests / fixtures.
    pub fn empty() -> Self {
        CalculatorInfo {
            registrations: Vec::new(),
            subscribes: Vec::new(),
            chains: Vec::new(),
            source_sha256: String::new(),
        }
    }

    /// Discard the parsed value's hash. Useful when comparing parses by
    /// directive content rather than provenance.
    pub fn without_hash(mut self) -> Self {
        self.source_sha256.clear();
        self
    }

    /// Total directive count — matches the migration plan's headline number.
    pub fn total_directives(&self) -> usize {
        self.registrations.len() + self.subscribes.len() + self.chains.len()
    }
}

/// Tail-rich `Display`-style formatter for tests / debug output.
#[allow(dead_code)]
pub(crate) fn _debug_path(p: PathBuf) -> String {
    p.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> CalculatorInfo {
        parse_calculator_info_str(text, Path::new("<test>")).unwrap()
    }

    #[test]
    fn parses_three_directive_kinds() {
        let text = "// Registration\tOutputPollutantName\t...\n\
                    // Subscribe\tModule\t...\n\
                    // Chain\tOutput\tInput\n\
                    Registration\tCO\t2\tRunning Exhaust\t1\tBaseRateCalculator\n\
                    Subscribe\tBaseRateCalculator\tRunning Exhaust\t1\tMONTH\tEMISSION_CALCULATOR\n\
                    Chain\tHCSpeciationCalculator\tBaseRateCalculator\n";
        let info = parse(text);
        assert_eq!(info.registrations.len(), 1);
        assert_eq!(info.subscribes.len(), 1);
        assert_eq!(info.chains.len(), 1);
        assert_eq!(info.total_directives(), 3);
        let reg = &info.registrations[0];
        assert_eq!(reg.pollutant_id, 2);
        assert_eq!(reg.process_id, 1);
        assert_eq!(reg.calculator, "BaseRateCalculator");
        assert_eq!(reg.location.line, 4);
    }

    #[test]
    fn handles_bom_and_blank_lines() {
        let text = "\u{feff}// header\n\n\
                    Chain\tHCSpeciationCalculator\tBaseRateCalculator\n\n";
        let info = parse(text);
        assert_eq!(info.chains.len(), 1);
        assert_eq!(info.chains[0].location.line, 3);
    }

    #[test]
    fn priority_with_offset() {
        let text = "Subscribe\tEvaporativePermeationCalculator\tEvap Permeation\t11\tMONTH\tEMISSION_CALCULATOR+1\n";
        let info = parse(text);
        let sub = &info.subscribes[0];
        assert_eq!(sub.granularity, Granularity::Month);
        assert_eq!(sub.priority.value(), 11);
        assert_eq!(sub.priority.display(), "EMISSION_CALCULATOR+1");
    }

    #[test]
    fn unknown_tag_is_error() {
        let err = parse_calculator_info_str("// header\nBogus\tfoo\tbar\n", Path::new("<test>"))
            .unwrap_err();
        match err {
            Error::Directive { line, message, .. } => {
                assert_eq!(line, 2);
                assert!(message.contains("unknown directive tag 'Bogus'"));
            }
            other => panic!("expected Directive error, got {other:?}"),
        }
    }

    #[test]
    fn wrong_column_count_is_error() {
        let err = parse_calculator_info_str(
            "Registration\tCO\t2\tRunning Exhaust\n",
            Path::new("<test>"),
        )
        .unwrap_err();
        match err {
            Error::Directive { line, message, .. } => {
                assert_eq!(line, 1);
                assert!(message.contains("expected 6 tab-separated fields"));
            }
            other => panic!("expected Directive error, got {other:?}"),
        }
    }

    #[test]
    fn unknown_granularity_is_error() {
        let err = parse_calculator_info_str(
            "Subscribe\tX\tRunning Exhaust\t1\tDECADE\tEMISSION_CALCULATOR\n",
            Path::new("<test>"),
        )
        .unwrap_err();
        match err {
            Error::UnknownGranularity { value, .. } => assert_eq!(value, "DECADE"),
            other => panic!("expected UnknownGranularity, got {other:?}"),
        }
    }

    #[test]
    fn sha256_is_deterministic() {
        let a = parse_calculator_info_str("// hi\n", Path::new("<a>")).unwrap();
        let b = parse_calculator_info_str("// hi\n", Path::new("<b>")).unwrap();
        assert_eq!(a.source_sha256, b.source_sha256);
        // Sanity: changing one byte changes the hash.
        let c = parse_calculator_info_str("// bye\n", Path::new("<a>")).unwrap();
        assert_ne!(a.source_sha256, c.source_sha256);
    }
}
