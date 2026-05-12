//! TOML configuration schema for [`crate::expander::MacroExpander`].
//!
//! The CLI binary `moves-sql-expand` reads a configuration file in this
//! format, registers each [`DataSet`] and [`CsvSet`] with a fresh
//! [`crate::MacroExpander`], and applies [`ExpandConfig::enabled_sections`]
//! and [`ExpandConfig::replacements`] to the script through
//! [`crate::sections::process_sections`].
//!
//! # Example
//!
//! ```toml
//! enabled_sections = ["WithRegClassID", "Process2", "Inventory"]
//!
//! [replacements]
//! "##context.year##" = "2030"
//! "##context.iterLocation.countyRecordID##" = "27137"
//!
//! [[data_sets]]
//! prefix = "mya."
//! sql_id = "select yearID, modelYearID, ageID from RunSpecModelYearAge"
//! columns = ["yearID", "modelYearID", "ageID"]
//! rows = [
//!   ["2030", "2025", "5"],
//!   ["2030", "2030", "0"],
//! ]
//!
//! [[csv_sets]]
//! sql_id = "select sourceTypeID from RunSpecSourceType"
//! column_name = "sourceTypeID"
//! values = ["21", "31"]
//! max_length = 5000
//! should_add_quotes = false
//! use_default_value_in_data = false
//! default_value = "0"
//! ```

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::expander::MacroExpander;

/// Top-level configuration consumed by the `moves-sql-expand` CLI.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpandConfig {
    /// Section names the runtime would mark as enabled for this bundle.
    /// Matched case-insensitively against `-- Section <name>` markers in
    /// the SQL script.
    #[serde(default)]
    pub enabled_sections: Vec<String>,

    /// `##context.*##`-style textual replacements applied to non-marker
    /// lines after macro expansion. Keys are inserted as-is (Java does
    /// case-insensitive substring matching on the key).
    #[serde(default)]
    pub replacements: BTreeMap<String, String>,

    /// Multi-column macro value sets. Each one fans out into `columns.len()`
    /// macros of the form `##macro.{prefix}{column}##`.
    #[serde(default)]
    pub data_sets: Vec<DataSet>,

    /// CSV-style value sets. Each one produces two macros:
    /// `##macro.csv.{column_name}##` (chunked, one row per `max_length`-
    /// bounded chunk) and `##macro.csv.all.{column_name}##` (one row with
    /// the full list).
    #[serde(default)]
    pub csv_sets: Vec<CsvSet>,
}

/// Multi-column data set.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataSet {
    /// Macro-name prefix. Use the empty string for no prefix.
    #[serde(default)]
    pub prefix: String,
    /// Arbitrary string used only to derive the set's identity key.
    /// Java passes the SQL statement text; downstream tests can use any
    /// stable string.
    pub sql_id: String,
    /// Column names, ordered. Produce macros `##macro.{prefix}{column}##`.
    pub columns: Vec<String>,
    /// Rows, in macro-expansion order (Java's `PermutationCreator`
    /// increments the first-added dimension fastest, so the row ordering
    /// here directly maps to expansion order).
    pub rows: Vec<Vec<String>>,
}

/// CSV-style aggregation set.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsvSet {
    /// Identity key (Java passes the SQL statement text).
    pub sql_id: String,
    /// Column name, used in the macro suffix.
    pub column_name: String,
    /// Raw column values (unsorted, possibly duplicated). The expander sorts
    /// and dedupes them via `BTreeSet<String>` (Java's `TreeSet<String>`).
    pub values: Vec<String>,
    /// Maximum chunk length in characters. `0` means no chunking — every
    /// value joins into one row.
    #[serde(default = "default_max_length")]
    pub max_length: usize,
    /// Wrap each value with `'…'` (MySQL-escaped). Use `true` for textual
    /// columns, `false` for numeric IDs.
    #[serde(default)]
    pub should_add_quotes: bool,
    /// Keep `default_value` in the data even when real rows are present.
    #[serde(default)]
    pub use_default_value_in_data: bool,
    /// Value to emit when the value list collapses to empty (prevents
    /// zero-element SQL `IN ()` syntax errors). `null`/missing = no default.
    /// An empty string with `should_add_quotes = false` is treated as
    /// "no default", matching the Java `hasDefaultValue` predicate.
    #[serde(default)]
    pub default_value: Option<String>,
}

fn default_max_length() -> usize {
    5000
}

impl ExpandConfig {
    /// Load and parse a TOML configuration file.
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&contents).map_err(|source| Error::Config {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Apply the configured value sets to a fresh [`MacroExpander`].
    ///
    /// Order: [`Self::data_sets`] first, then [`Self::csv_sets`]. The
    /// order is observable via [`crate::MacroExpander::expand_and_add`]'s
    /// cartesian-product expansion — the first-added set's row index
    /// cycles fastest.
    pub fn build_expander(&self) -> Result<MacroExpander> {
        let mut m = MacroExpander::new();
        for set in &self.data_sets {
            let columns: Vec<&str> = set.columns.iter().map(String::as_str).collect();
            m.add_data(&set.prefix, &set.sql_id, &columns, &set.rows)?;
        }
        for set in &self.csv_sets {
            let values: Vec<&str> = set.values.iter().map(String::as_str).collect();
            m.add_csv_data(
                &set.sql_id,
                &set.column_name,
                &values,
                set.max_length,
                set.should_add_quotes,
                set.use_default_value_in_data,
                set.default_value.as_deref(),
            );
        }
        m.compile();
        Ok(m)
    }

    /// Materialise the replacement map as a vector of `(key, value)` pairs
    /// in the order [`crate::do_replacements`] expects. Sorted by key for
    /// determinism (BTreeMap's natural order).
    pub fn replacement_pairs(&self) -> Vec<(String, String)> {
        self.replacements
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let toml = "";
        let cfg: ExpandConfig = toml::from_str(toml).unwrap();
        assert!(cfg.enabled_sections.is_empty());
        assert!(cfg.replacements.is_empty());
        assert!(cfg.data_sets.is_empty());
        assert!(cfg.csv_sets.is_empty());
    }

    #[test]
    fn parses_full_config() {
        let toml = r###"
enabled_sections = ["WithRegClassID", "Process2"]

[replacements]
"##context.year##" = "2030"

[[data_sets]]
prefix = "mya."
sql_id = "rsp_mya"
columns = ["yearID", "modelYearID", "ageID"]
rows = [
  ["2030", "2025", "5"],
  ["2030", "2030", "0"],
]

[[csv_sets]]
sql_id = "rsp_st"
column_name = "sourceTypeID"
values = ["21", "31"]
max_length = 5000
should_add_quotes = false
use_default_value_in_data = false
default_value = "0"
"###;
        let cfg: ExpandConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.enabled_sections, vec!["WithRegClassID", "Process2"]);
        assert_eq!(cfg.replacements.len(), 1);
        assert_eq!(cfg.data_sets.len(), 1);
        assert_eq!(cfg.data_sets[0].prefix, "mya.");
        assert_eq!(cfg.data_sets[0].columns.len(), 3);
        assert_eq!(cfg.data_sets[0].rows.len(), 2);
        assert_eq!(cfg.csv_sets.len(), 1);
        assert_eq!(cfg.csv_sets[0].column_name, "sourceTypeID");
        assert_eq!(cfg.csv_sets[0].values, vec!["21", "31"]);
        assert_eq!(cfg.csv_sets[0].default_value.as_deref(), Some("0"));
    }

    #[test]
    fn rejects_unknown_fields() {
        let toml = "not_a_field = 1\n";
        let err = toml::from_str::<ExpandConfig>(toml).unwrap_err();
        assert!(err.to_string().contains("not_a_field"));
    }

    #[test]
    fn build_expander_populates_sets_in_order() {
        let toml = r#"
[[data_sets]]
sql_id = "a"
columns = ["x"]
rows = [["1"], ["2"]]

[[csv_sets]]
sql_id = "b"
column_name = "y"
values = ["3", "4"]
"#;
        let cfg: ExpandConfig = toml::from_str(toml).unwrap();
        let m = cfg.build_expander().unwrap();
        // 1 data set + 2 CSV sets (csv chunked + csv all) = 3.
        assert_eq!(m.len(), 3);
    }

    #[test]
    fn replacement_pairs_are_sorted_for_determinism() {
        let toml = r###"
[replacements]
"##b##" = "B"
"##a##" = "A"
"##c##" = "C"
"###;
        let cfg: ExpandConfig = toml::from_str(toml).unwrap();
        let pairs = cfg.replacement_pairs();
        assert_eq!(
            pairs,
            vec![
                ("##a##".to_string(), "A".to_string()),
                ("##b##".to_string(), "B".to_string()),
                ("##c##".to_string(), "C".to_string()),
            ]
        );
    }
}
