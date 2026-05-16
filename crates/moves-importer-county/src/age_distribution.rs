//! `AgeDistributionImporter` — ports
//! `gov/epa/otaq/moves/master/implementation/importers/AgeDistributionImporter.java`.
//!
//! Single table `SourceTypeAgeDistribution`:
//!
//! | column | filter |
//! |---|---|
//! | `sourceTypeID` | [`Filter::SourceType`] |
//! | `yearID` | [`Filter::Year`] |
//! | `ageID` | [`Filter::Age`] |
//! | `ageFraction` | [`Filter::NonNegative`] |
//!
//! The cross-row invariant from MOVES: for every (sourceTypeID,
//! yearID) tuple, `ageFraction` over ages 0..30 must sum to 1.0
//! (within 4-decimal rounding). The Java code does not put this in a
//! single SQL file but the
//! `gov.epa.otaq.moves.master.framework.RunSpecSectionStatus`
//! pipeline picks it up at run time when the `AgeDistribution` block
//! is consumed; we surface it at validation time instead.

use arrow::array::{Array, Float64Array, Int64Array};
use moves_importer::{
    ColumnDescriptor, Filter, ImportedTable, Importer, TableDescriptor, ValidationContext,
    ValidationMessage,
};

const COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
    ColumnDescriptor::new("yearID", Filter::Year),
    ColumnDescriptor::new("ageID", Filter::Age),
    ColumnDescriptor::new("ageFraction", Filter::NonNegative),
];

const TABLE: TableDescriptor = TableDescriptor {
    name: "SourceTypeAgeDistribution",
    columns: COLUMNS,
    primary_key: &["sourceTypeID", "yearID", "ageID"],
};

const TABLES: &[TableDescriptor] = &[TABLE];

/// Source-type age-distribution importer.
#[derive(Debug, Default)]
pub struct AgeDistributionImporter;

impl Importer for AgeDistributionImporter {
    fn name(&self) -> &'static str {
        "Age Distribution"
    }
    fn xml_node_type(&self) -> &'static str {
        "agedistribution"
    }
    fn tables(&self) -> &'static [TableDescriptor] {
        TABLES
    }

    fn validate_imported(
        &self,
        tables: &[ImportedTable<'_>],
        _ctx: &ValidationContext<'_>,
    ) -> Vec<ValidationMessage> {
        let imported = &tables[0];
        let batch = &imported.batch;
        let source_type = batch
            .column_by_name("sourceTypeID")
            .expect("descriptor lists sourceTypeID")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sourceTypeID is Int64");
        let year = batch
            .column_by_name("yearID")
            .expect("descriptor lists yearID")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("yearID is Int64");
        let fraction = batch
            .column_by_name("ageFraction")
            .expect("descriptor lists ageFraction")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("ageFraction is Float64");

        let mut sums: std::collections::BTreeMap<(i64, i64), f64> =
            std::collections::BTreeMap::new();
        for row in 0..batch.num_rows() {
            if source_type.is_null(row) || year.is_null(row) || fraction.is_null(row) {
                continue;
            }
            let s = source_type.value(row);
            let y = year.value(row);
            let f = fraction.value(row);
            *sums.entry((s, y)).or_insert(0.0) += f;
        }

        let mut out = Vec::new();
        for ((source_type_id, year_id), sum) in sums {
            let rounded = (sum * 10_000.0).round() / 10_000.0;
            if (rounded - 1.0).abs() > f64::EPSILON {
                out.push(ValidationMessage::error(
                    "SourceTypeAgeDistribution",
                    Some("ageFraction"),
                    None,
                    format!(
                        "sourceTypeID {source_type_id} yearID {year_id} ageFraction sums to {rounded:.4}, expected 1.0000"
                    ),
                ));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn batch(rows: &[(i64, i64, i64, f64)]) -> arrow::record_batch::RecordBatch {
        use arrow::array::{Float64Builder, Int64Builder};
        let mut s = Int64Builder::new();
        let mut y = Int64Builder::new();
        let mut a = Int64Builder::new();
        let mut f = Float64Builder::new();
        for &(source, year, age, frac) in rows {
            s.append_value(source);
            y.append_value(year);
            a.append_value(age);
            f.append_value(frac);
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(s.finish()),
            Arc::new(y.finish()),
            Arc::new(a.finish()),
            Arc::new(f.finish()),
        ];
        moves_importer::writer::build_record_batch_from_columns(&TABLE, cols).unwrap()
    }

    #[test]
    fn fractions_summing_to_one_per_source_year_pass() {
        // (sourceTypeID=21, yearID=2020) — three buckets that sum to 1.
        // (sourceTypeID=21, yearID=2021) — same.
        let rows: Vec<(i64, i64, i64, f64)> = vec![
            (21, 2020, 0, 0.50),
            (21, 2020, 1, 0.30),
            (21, 2020, 2, 0.20),
            (21, 2021, 0, 0.40),
            (21, 2021, 1, 0.40),
            (21, 2021, 2, 0.20),
        ];
        let b = batch(&rows);
        let imp = AgeDistributionImporter;
        let imported = ImportedTable::new(&imp.tables()[0], PathBuf::from("test"), b);
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&[imported], &ctx);
        assert!(msgs.is_empty(), "got: {msgs:?}");
    }

    #[test]
    fn off_balance_tuple_emits_one_error_per_tuple() {
        let rows: Vec<(i64, i64, i64, f64)> = vec![
            (21, 2020, 0, 0.50),
            (21, 2020, 1, 0.30),
            // (21, 2020) sums to 0.80 — off
            (21, 2021, 0, 1.00),
            // (21, 2021) sums to 1.00 — ok
            (32, 2020, 0, 0.60),
            // (32, 2020) sums to 0.60 — off
        ];
        let b = batch(&rows);
        let imp = AgeDistributionImporter;
        let imported = ImportedTable::new(&imp.tables()[0], PathBuf::from("test"), b);
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&[imported], &ctx);
        let errors: Vec<_> = msgs.iter().filter(|m| m.is_error()).collect();
        assert_eq!(errors.len(), 2);
        assert!(errors
            .iter()
            .all(|m| m.column == Some("ageFraction") && m.row.is_none()));
    }
}
