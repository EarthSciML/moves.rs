//! `StartsImporter` — ports
//! `gov/epa/otaq/moves/master/implementation/importers/StartsImporter.java`.
//!
//! County-scale start-exhaust activity tables. These feed
//! `CriteriaStartCalculator` (and downstream crankcase calculators) with
//! county-specific start activity instead of the national default.
//!
//! ## Tables
//!
//! | Table | Columns | Purpose |
//! |---|---|---|
//! | `startsPerDay` | dayID, sourceTypeID, startsPerDay | Total daily starts by day/source type |
//! | `startsHourFraction` | dayID, hourID, sourceTypeID, allocationFraction | Hour distribution |
//! | `startsMonthAdjust` | monthID, sourceTypeID, monthAdjustment | Monthly adjustment multipliers |
//! | `startsAgeAdjustment` | sourceTypeID, ageID, ageAdjustment | Age-distribution multipliers |
//! | `startsOpModeDistribution` | dayID, hourID, sourceTypeID, ageID, opModeID, opModeFraction | Op-mode distribution |
//!
//! Cross-row invariants (from `database/StartsImporter.sql`):
//! - `allocationFraction` sums to 1.0 per `(dayID, sourceTypeID)` in `startsHourFraction`.

use arrow::array::{Array, Float64Array, Int64Array};
use moves_importer::{
    ColumnDescriptor, Filter, ImportedTable, Importer, TableDescriptor, ValidationContext,
    ValidationMessage,
};

// ---- startsPerDay ----------------------------------------------------------

const STARTS_PER_DAY_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("dayID", Filter::Day),
    ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
    ColumnDescriptor::new("startsPerDay", Filter::NonNegative),
];

const STARTS_PER_DAY_TABLE: TableDescriptor = TableDescriptor {
    name: "startsPerDay",
    columns: STARTS_PER_DAY_COLUMNS,
    primary_key: &["sourceTypeID", "dayID"],
};

// ---- startsHourFraction ----------------------------------------------------

const STARTS_HOUR_FRACTION_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("dayID", Filter::Day),
    ColumnDescriptor::new("hourID", Filter::Hour),
    ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
    ColumnDescriptor::new("allocationFraction", Filter::NonNegative),
];

const STARTS_HOUR_FRACTION_TABLE: TableDescriptor = TableDescriptor {
    name: "startsHourFraction",
    columns: STARTS_HOUR_FRACTION_COLUMNS,
    primary_key: &["dayID", "hourID", "sourceTypeID"],
};

// ---- startsMonthAdjust -----------------------------------------------------

const STARTS_MONTH_ADJUST_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("monthID", Filter::Month),
    ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
    ColumnDescriptor::new("monthAdjustment", Filter::NonNegative),
];

const STARTS_MONTH_ADJUST_TABLE: TableDescriptor = TableDescriptor {
    name: "startsMonthAdjust",
    columns: STARTS_MONTH_ADJUST_COLUMNS,
    primary_key: &["monthID", "sourceTypeID"],
};

// ---- startsAgeAdjustment ---------------------------------------------------

const STARTS_AGE_ADJUSTMENT_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
    ColumnDescriptor::new("ageID", Filter::Age),
    ColumnDescriptor::new("ageAdjustment", Filter::NonNegative),
];

const STARTS_AGE_ADJUSTMENT_TABLE: TableDescriptor = TableDescriptor {
    name: "startsAgeAdjustment",
    columns: STARTS_AGE_ADJUSTMENT_COLUMNS,
    primary_key: &["sourceTypeID", "ageID"],
};

// ---- startsOpModeDistribution ----------------------------------------------

const STARTS_OP_MODE_DISTRIBUTION_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("dayID", Filter::Day),
    ColumnDescriptor::new("hourID", Filter::Hour),
    ColumnDescriptor::new("sourceTypeID", Filter::SourceType),
    ColumnDescriptor::new("ageID", Filter::Age),
    ColumnDescriptor::new("opModeID", Filter::NonNegative),
    ColumnDescriptor::new("opModeFraction", Filter::NonNegative),
];

const STARTS_OP_MODE_DISTRIBUTION_TABLE: TableDescriptor = TableDescriptor {
    name: "startsOpModeDistribution",
    columns: STARTS_OP_MODE_DISTRIBUTION_COLUMNS,
    primary_key: &["dayID", "hourID", "sourceTypeID", "ageID", "opModeID"],
};

// ---- all tables ------------------------------------------------------------

const TABLES: &[TableDescriptor] = &[
    STARTS_PER_DAY_TABLE,
    STARTS_HOUR_FRACTION_TABLE,
    STARTS_MONTH_ADJUST_TABLE,
    STARTS_AGE_ADJUSTMENT_TABLE,
    STARTS_OP_MODE_DISTRIBUTION_TABLE,
];

/// County-scale start-exhaust activity importer.
///
/// Ports `StartsImporter.java` for county-domain runs. Provides the
/// `startsPerDay` and related distribution tables that `AdjustStarts.sql`
/// uses to synthesise the `Starts` activity table.
#[derive(Debug, Default)]
pub struct StartsImporter;

impl Importer for StartsImporter {
    fn name(&self) -> &'static str {
        "Starts"
    }

    fn xml_node_type(&self) -> &'static str {
        "starts"
    }

    fn tables(&self) -> &'static [TableDescriptor] {
        TABLES
    }

    fn validate_imported(
        &self,
        tables: &[ImportedTable<'_>],
        _ctx: &ValidationContext<'_>,
    ) -> Vec<ValidationMessage> {
        let mut out = Vec::new();

        // -- startsHourFraction: allocationFraction sums to 1.0 per (dayID, sourceTypeID) --
        let hf = tables
            .iter()
            .find(|t| t.descriptor.name == "startsHourFraction");
        if let Some(hf) = hf {
            let batch = &hf.batch;
            if let (Some(day_col), Some(st_col), Some(frac_col)) = (
                batch.column_by_name("dayID"),
                batch.column_by_name("sourceTypeID"),
                batch.column_by_name("allocationFraction"),
            ) {
                if let (Some(day), Some(st), Some(frac)) = (
                    day_col.as_any().downcast_ref::<Int64Array>(),
                    st_col.as_any().downcast_ref::<Int64Array>(),
                    frac_col.as_any().downcast_ref::<Float64Array>(),
                ) {
                    let mut sums: std::collections::BTreeMap<(i64, i64), f64> =
                        std::collections::BTreeMap::new();
                    for i in 0..batch.num_rows() {
                        if day.is_null(i) || st.is_null(i) || frac.is_null(i) {
                            continue;
                        }
                        *sums.entry((day.value(i), st.value(i))).or_insert(0.0) += frac.value(i);
                    }
                    for ((d, s), sum) in &sums {
                        let rounded = (sum * 10_000.0).round() / 10_000.0;
                        if (rounded - 1.0).abs() > f64::EPSILON {
                            out.push(ValidationMessage::error(
                                "startsHourFraction",
                                Some("allocationFraction"),
                                None,
                                format!(
                                    "total allocationFraction for day {d}, sourceType {s} \
                                     should be 1 but instead is {rounded:.4}"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Builder, Int64Builder};
    use std::path::PathBuf;
    use std::sync::Arc;

    fn hf_batch(rows: &[(i64, i64, i64, f64)]) -> arrow::record_batch::RecordBatch {
        // (dayID, hourID, sourceTypeID, allocationFraction)
        let mut d = Int64Builder::new();
        let mut h = Int64Builder::new();
        let mut s = Int64Builder::new();
        let mut f = Float64Builder::new();
        for &(day, hour, src, frac) in rows {
            d.append_value(day);
            h.append_value(hour);
            s.append_value(src);
            f.append_value(frac);
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(d.finish()),
            Arc::new(h.finish()),
            Arc::new(s.finish()),
            Arc::new(f.finish()),
        ];
        moves_importer::writer::build_record_batch_from_columns(&STARTS_HOUR_FRACTION_TABLE, cols)
            .unwrap()
    }

    #[test]
    fn hour_fractions_summing_to_one_pass() {
        // day=5, sourceType=21: three hours summing to 1.0
        let b = hf_batch(&[(5, 6, 21, 0.30), (5, 7, 21, 0.40), (5, 8, 21, 0.30)]);
        let imp = StartsImporter;
        let tables: Vec<ImportedTable<'_>> = imp
            .tables()
            .iter()
            .map(|td| {
                if td.name == "startsHourFraction" {
                    ImportedTable::new(td, PathBuf::from("test"), b.clone())
                } else {
                    ImportedTable::new(
                        td,
                        PathBuf::from("test"),
                        arrow::record_batch::RecordBatch::new_empty(
                            arrow::datatypes::Schema::empty().into(),
                        ),
                    )
                }
            })
            .collect();
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&tables, &ctx);
        assert!(msgs.is_empty(), "got: {msgs:?}");
    }

    #[test]
    fn hour_fractions_not_summing_to_one_are_flagged() {
        // day=5, sourceType=21: sums to 0.5 only
        let b = hf_batch(&[(5, 6, 21, 0.30), (5, 7, 21, 0.20)]);
        let imp = StartsImporter;
        let tables: Vec<ImportedTable<'_>> = imp
            .tables()
            .iter()
            .map(|td| {
                if td.name == "startsHourFraction" {
                    ImportedTable::new(td, PathBuf::from("test"), b.clone())
                } else {
                    ImportedTable::new(
                        td,
                        PathBuf::from("test"),
                        arrow::record_batch::RecordBatch::new_empty(
                            arrow::datatypes::Schema::empty().into(),
                        ),
                    )
                }
            })
            .collect();
        let ctx = ValidationContext::without_default_db();
        let msgs = imp.validate_imported(&tables, &ctx);
        assert!(msgs.iter().any(|m| m.is_error()), "expected error");
    }
}
