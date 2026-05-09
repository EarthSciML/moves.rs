//! Source-category packet parser (`rdnrsrc.f`).
//!
//! Task 97. Parses the optional `/SOURCE CATEGORY/` packet from the
//! options file. Each line lists a 10-character SCC selector that
//! either matches an equipment code exactly, or — when its trailing
//! digits are zeros — matches by 4-digit (`XXXX000000`) or 7-digit
//! (`XXXXXXX000`) prefix. If the packet is absent the simulation is
//! "all categories"; the Rust port models that with [`SourceCategorySelection::AllSources`].
//!
//! # Format
//!
//! ```text
//! /SOURCE CATEGORY/
//! Source             : 2270001000
//! Source             : 2265000000
//! /END/
//! ```
//!
//! # Fortran source
//!
//! Ports `rdnrsrc.f` (191 lines).

use crate::{Error, Result};
use std::io::BufRead;
use std::path::PathBuf;

/// One SCC selector from the packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSelector {
    /// 10-character SCC code (digits, may have trailing zeros).
    pub scc: String,
}

impl SourceSelector {
    /// Whether `equipment_code` matches this selector under the
    /// rules of `rdnrsrc.f` (exact / 4-digit / 7-digit prefix).
    pub fn matches(&self, equipment_code: &str) -> bool {
        if self.scc.len() != 10 || equipment_code.len() != 10 {
            return self.scc == equipment_code;
        }
        if self.scc == equipment_code {
            return true;
        }
        if self.scc.ends_with("000000") {
            return self.scc[..4] == equipment_code[..4];
        }
        if self.scc.ends_with("000") {
            return self.scc[..7] == equipment_code[..7];
        }
        false
    }
}

/// Outcome of parsing a `/SOURCE CATEGORY/` packet.
#[derive(Debug, Clone)]
pub enum SourceCategorySelection {
    /// Packet absent — all sources are active.
    AllSources,
    /// Explicit list of SCC selectors.
    Selected(Vec<SourceSelector>),
}

impl SourceCategorySelection {
    /// Whether `equipment_code` is selected.
    pub fn includes(&self, equipment_code: &str) -> bool {
        match self {
            Self::AllSources => true,
            Self::Selected(items) => items.iter().any(|s| s.matches(equipment_code)),
        }
    }
}

/// Parse a `/SOURCE CATEGORY/` packet.
pub fn read_source_category<R: BufRead>(reader: R) -> Result<SourceCategorySelection> {
    let path = PathBuf::from(".OPT");
    let mut in_packet = false;
    let mut found_packet = false;
    let mut selectors = Vec::new();
    let mut line_num = 0;

    for line_result in reader.lines() {
        line_num += 1;
        let line = line_result.map_err(|e| Error::Io {
            path: path.clone(),
            source: e,
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("/SOURCE CATEGORY/") {
            in_packet = true;
            found_packet = true;
            continue;
        }
        if upper.starts_with("/END/") {
            in_packet = false;
            continue;
        }
        if !in_packet {
            continue;
        }
        let value = extract_value(&line);
        if value.is_empty() {
            continue;
        }
        if value.len() != 10 || !value.chars().all(|c| c.is_ascii_digit()) {
            return Err(Error::Parse {
                file: path,
                line: line_num,
                message: format!(
                    "SCC selector must be 10 digits, got {:?} ({} chars)",
                    value,
                    value.len()
                ),
            });
        }
        selectors.push(SourceSelector { scc: value });
    }

    if !found_packet {
        return Ok(SourceCategorySelection::AllSources);
    }
    Ok(SourceCategorySelection::Selected(selectors))
}

fn extract_value(line: &str) -> String {
    if let Some(idx) = line.find(':') {
        line[idx + 1..].trim().to_string()
    } else if line.len() > 20 {
        line[20..].trim().to_string()
    } else {
        line.trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_packet_is_all() {
        let sel = read_source_category(b"" as &[u8]).unwrap();
        assert!(matches!(sel, SourceCategorySelection::AllSources));
        assert!(sel.includes("2270001000"));
    }

    #[test]
    fn exact_match_only() {
        let input = "\
/SOURCE CATEGORY/
Source             : 2270001000
/END/
";
        let sel = read_source_category(input.as_bytes()).unwrap();
        assert!(sel.includes("2270001000"));
        assert!(!sel.includes("2270002000"));
    }

    #[test]
    fn prefix_matches() {
        let input = "\
/SOURCE CATEGORY/
Source             : 2265000000
Source             : 2270001000
/END/
";
        let sel = read_source_category(input.as_bytes()).unwrap();
        // 4-digit prefix 2265 matches anything starting with 2265
        assert!(sel.includes("2265001000"));
        assert!(sel.includes("2265010050"));
        // exact match
        assert!(sel.includes("2270001000"));
        // doesn't match 2270002000 (which is not a prefix-zero entry)
        assert!(!sel.includes("2270002000"));
    }

    #[test]
    fn rejects_non_numeric_selector() {
        let input = "\
/SOURCE CATEGORY/
Source             : abc
/END/
";
        let err = read_source_category(input.as_bytes()).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("10 digits")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
