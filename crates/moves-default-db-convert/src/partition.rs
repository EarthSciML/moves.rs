//! Resolve a [`PartitionStrategy`] into the partition columns and file-path
//! template the writer uses.
//!
//! The strategies live in the audit (`tables.json`); the audit names the
//! strategy but not which column drives it. This module makes that decision
//! from the table's primary-key columns using the rules documented in
//! `characterization/default-db-schema/partitioning-plan.md`:
//!
//! * `county` → first of `countyID`, `zoneID`, `stateID` in the PK; file
//!   path component is `county=`, `zone=`, or `state=` accordingly so the
//!   reader can predicate-push by the actual column name.
//! * `year_x_county` → outer partition `year=` from `yearID`, inner
//!   partition by the same county/zone/state rule.
//! * `year` → `year=` from `yearID`.
//! * `model_year` → `modelYear=` from `modelYearID`.
//! * `monolithic` / `schema_only` → no partition columns.

use crate::error::{Error, Result};
use crate::plan::{PartitionStrategy, TableEntry};

/// Candidate column names for a geographic partition, in preference order.
/// `countyID` > `zoneID` > `stateID` because the partitioning plan treats
/// `countyID` as the canonical axis; `zoneID` collapses 1:1 to county at the
/// default scale and the reader joins via the `Zone` dimension when needed.
const COUNTY_LIKE: &[&str] = &["countyID", "zoneID", "stateID"];

/// Resolved partition specification for a single table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpec {
    pub strategy: PartitionStrategy,
    /// Ordered list of (column-name, path-label) pairs. Empty for
    /// monolithic and schema-only strategies. Path-label is the prefix
    /// that appears in directory components (`county=42/...`).
    pub columns: Vec<PartitionColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionColumn {
    pub column: String,
    pub label: String,
}

impl PartitionSpec {
    pub fn is_partitioned(&self) -> bool {
        !self.columns.is_empty()
    }
}

/// Resolve the partition spec for a table given its audit entry.
pub fn resolve(table: &TableEntry) -> Result<PartitionSpec> {
    use PartitionStrategy::*;
    let strategy = table.partition.strategy;
    match strategy {
        Monolithic | SchemaOnly => Ok(PartitionSpec {
            strategy,
            columns: Vec::new(),
        }),
        County => {
            let geo = pick_county_like(&table.primary_key, &table.name, strategy)?;
            Ok(PartitionSpec {
                strategy,
                columns: vec![geo],
            })
        }
        Year => {
            let year = pick_named(&table.primary_key, "yearID", "year", &table.name, strategy)?;
            Ok(PartitionSpec {
                strategy,
                columns: vec![year],
            })
        }
        YearXCounty => {
            let year = pick_named(&table.primary_key, "yearID", "year", &table.name, strategy)?;
            let geo = pick_county_like(&table.primary_key, &table.name, strategy)?;
            Ok(PartitionSpec {
                strategy,
                columns: vec![year, geo],
            })
        }
        ModelYear => {
            let my = pick_named(
                &table.primary_key,
                "modelYearID",
                "modelYear",
                &table.name,
                strategy,
            )?;
            Ok(PartitionSpec {
                strategy,
                columns: vec![my],
            })
        }
        Unknown => Err(Error::Plan(format!(
            "table '{}' has unrecognised partition strategy",
            table.name
        ))),
    }
}

fn pick_county_like(
    pk: &[String],
    table: &str,
    strategy: PartitionStrategy,
) -> Result<PartitionColumn> {
    for candidate in COUNTY_LIKE {
        if let Some(matched) = pk.iter().find(|c| c.eq_ignore_ascii_case(candidate)) {
            let label = match candidate.to_ascii_lowercase().as_str() {
                "countyid" => "county",
                "zoneid" => "zone",
                "stateid" => "state",
                _ => unreachable!(),
            };
            return Ok(PartitionColumn {
                column: matched.clone(),
                label: label.to_string(),
            });
        }
    }
    Err(Error::NoPartitionColumn {
        table: table.to_string(),
        strategy: strategy.as_str().to_string(),
        pk: pk.to_vec(),
    })
}

fn pick_named(
    pk: &[String],
    needle: &str,
    label: &str,
    table: &str,
    strategy: PartitionStrategy,
) -> Result<PartitionColumn> {
    pk.iter()
        .find(|c| c.eq_ignore_ascii_case(needle))
        .map(|matched| PartitionColumn {
            column: matched.clone(),
            label: label.to_string(),
        })
        .ok_or_else(|| Error::NoPartitionColumn {
            table: table.to_string(),
            strategy: strategy.as_str().to_string(),
            pk: pk.to_vec(),
        })
}

/// Render a relative path for a partition value sequence. `parts` carries the
/// (label, value) pairs in partition order; for a monolithic table `parts` is
/// empty and the path is the table name only.
///
/// Monolithic: `<table>.parquet`
/// Partitioned: `<table>/<label>=<v>/<label>=<v>/part.parquet`
pub fn render_path(table: &str, parts: &[(String, String)]) -> String {
    if parts.is_empty() {
        return format!("{table}.parquet");
    }
    let mut s = String::from(table);
    for (label, value) in parts {
        s.push('/');
        s.push_str(label);
        s.push('=');
        s.push_str(&sanitize_value(value));
    }
    s.push_str("/part.parquet");
    s
}

/// Sanitize a partition value into a path-safe segment. `null` becomes the
/// literal `__NULL__`; other characters that would confuse the layout are
/// `_`-escaped.
fn sanitize_value(v: &str) -> String {
    if v.is_empty() {
        return "__EMPTY__".to_string();
    }
    let mut out = String::with_capacity(v.len());
    for ch in v.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{ColumnEntry, PartitionInfo, TableEntry};

    fn entry(name: &str, pk: &[&str], strategy: PartitionStrategy) -> TableEntry {
        TableEntry {
            name: name.to_string(),
            primary_key: pk.iter().map(|s| s.to_string()).collect(),
            columns: pk
                .iter()
                .map(|c| ColumnEntry {
                    name: c.to_string(),
                    ty: "int".to_string(),
                })
                .collect(),
            indexes: Vec::new(),
            size_bucket: "small".to_string(),
            partition: PartitionInfo {
                strategy,
                rationale: String::new(),
            },
        }
    }

    #[test]
    fn monolithic_has_no_columns() {
        let t = entry("Year", &["yearID"], PartitionStrategy::Monolithic);
        let spec = resolve(&t).unwrap();
        assert!(!spec.is_partitioned());
        assert_eq!(render_path("Year", &[]), "Year.parquet");
    }

    #[test]
    fn county_prefers_county_id() {
        let t = entry(
            "fuelUsageFraction",
            &["countyID", "fuelYearID", "modelYearGroupID"],
            PartitionStrategy::County,
        );
        let spec = resolve(&t).unwrap();
        assert_eq!(spec.columns.len(), 1);
        assert_eq!(spec.columns[0].column, "countyID");
        assert_eq!(spec.columns[0].label, "county");
    }

    #[test]
    fn county_falls_back_to_zone_id() {
        let t = entry(
            "AverageTankGasoline",
            &["zoneID", "fuelTypeID"],
            PartitionStrategy::County,
        );
        let spec = resolve(&t).unwrap();
        assert_eq!(spec.columns[0].column, "zoneID");
        assert_eq!(spec.columns[0].label, "zone");
    }

    #[test]
    fn county_falls_back_to_state_id() {
        let t = entry(
            "nrMonthAllocation",
            &["SCC", "stateID", "monthID"],
            PartitionStrategy::County,
        );
        let spec = resolve(&t).unwrap();
        assert_eq!(spec.columns[0].column, "stateID");
        assert_eq!(spec.columns[0].label, "state");
    }

    #[test]
    fn county_errors_without_geo_pk() {
        let t = entry("Bogus", &["a", "b"], PartitionStrategy::County);
        assert!(matches!(
            resolve(&t).unwrap_err(),
            Error::NoPartitionColumn { .. }
        ));
    }

    #[test]
    fn year_x_county_combines_year_and_geo() {
        let t = entry(
            "IMCoverage",
            &["polProcessID", "countyID", "yearID", "sourceTypeID"],
            PartitionStrategy::YearXCounty,
        );
        let spec = resolve(&t).unwrap();
        assert_eq!(spec.columns.len(), 2);
        assert_eq!(spec.columns[0].column, "yearID");
        assert_eq!(spec.columns[0].label, "year");
        assert_eq!(spec.columns[1].column, "countyID");
        assert_eq!(spec.columns[1].label, "county");
    }

    #[test]
    fn model_year_resolves() {
        let t = entry(
            "nrEmissionRate",
            &["SCC", "modelYearID", "engTechID"],
            PartitionStrategy::ModelYear,
        );
        let spec = resolve(&t).unwrap();
        assert_eq!(spec.columns[0].column, "modelYearID");
        assert_eq!(spec.columns[0].label, "modelYear");
    }

    #[test]
    fn unknown_strategy_is_an_error() {
        let t = entry("Future", &["x"], PartitionStrategy::Unknown);
        assert!(matches!(resolve(&t).unwrap_err(), Error::Plan(_)));
    }

    #[test]
    fn render_path_partitioned() {
        assert_eq!(
            render_path(
                "IMCoverage",
                &[
                    ("year".to_string(), "2025".to_string()),
                    ("county".to_string(), "17031".to_string()),
                ]
            ),
            "IMCoverage/year=2025/county=17031/part.parquet"
        );
    }

    #[test]
    fn render_path_sanitises_weird_values() {
        assert_eq!(
            render_path("X", &[("label".to_string(), "a/b c".to_string())]),
            "X/label=a_b_c/part.parquet"
        );
        assert_eq!(
            render_path("X", &[("label".to_string(), "".to_string())]),
            "X/label=__EMPTY__/part.parquet"
        );
    }

    #[test]
    fn case_insensitive_pk_match() {
        let t = entry(
            "lower",
            &["YEARID", "COUNTYID"],
            PartitionStrategy::YearXCounty,
        );
        let spec = resolve(&t).unwrap();
        assert_eq!(spec.columns[0].column, "YEARID");
        assert_eq!(spec.columns[0].label, "year");
        assert_eq!(spec.columns[1].column, "COUNTYID");
    }
}
