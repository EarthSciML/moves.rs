//! `HotellingImporter` — ports
//! `gov/epa/otaq/moves/master/implementation/importers/HotellingImporter.java`.
//!
//! Five tables for county- and project-domain hotelling (extended-idle /
//! auxiliary-power exhaust) activity.
//!
//! County domain uses all five tables; project domain uses only
//! `hotellingActivityDistribution`.
//!
//! ## Tables
//!
//! | Table | Columns | Purpose |
//! |---|---|---|
//! | `hotellingHoursPerDay` | yearID, zoneID, dayID, hotellingHoursPerDay | Daily hotelling hours by zone/day/year |
//! | `hotellingHourFraction` | zoneID, dayID, hourID, hourFraction | Per-hour distribution of hotelling |
//! | `hotellingAgeFraction` | zoneID, ageID, ageFraction | Age-category distribution |
//! | `hotellingMonthAdjust` | zoneID, monthID, monthAdjustment | Monthly adjustment multipliers |
//! | `hotellingActivityDistribution` | zoneID, fuelTypeID, beginModelYearID, endModelYearID, opModeID, opModeFraction | Op-mode fractions |
//!
//! ## Cross-row invariants (from `database/HotellingImporter.sql`)
//!
//! - `hotellingHourFraction.hourFraction` sums to 1.0 per `(zoneID, dayID)`.
//! - `hotellingAgeFraction.ageFraction` sums to 1.0 per `zoneID`.
//! - `hotellingActivityDistribution.opModeID` must be in `{200, 201, 203, 204}`.
//! - No electricity (`fuelTypeID = 9`) with extended idle (`opModeID = 200`) and non-zero fraction.
//! - No electricity with diesel APU (`opModeID = 201`) and non-zero fraction.
//! - No CNG (`fuelTypeID = 3`) with diesel APU and non-zero fraction.
//! - `beginModelYearID ≤ endModelYearID`.
//! - `opModeFraction ∈ [0, 1]`.

use arrow::array::{Array, Float64Array, Int64Array};
use moves_importer::{
    ColumnDescriptor, Filter, ImportedTable, Importer, TableDescriptor, ValidationContext,
    ValidationMessage,
};

// ---- hotellingHoursPerDay --------------------------------------------------

const HOTELLING_HOURS_PER_DAY_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("yearID", Filter::Year),
    ColumnDescriptor::new("zoneID", Filter::Zone),
    ColumnDescriptor::new("dayID", Filter::Day),
    ColumnDescriptor::new("hotellingHoursPerDay", Filter::NonNegative),
];

const HOTELLING_HOURS_PER_DAY_TABLE: TableDescriptor = TableDescriptor {
    name: "hotellingHoursPerDay",
    columns: HOTELLING_HOURS_PER_DAY_COLUMNS,
    primary_key: &["yearID", "zoneID", "dayID"],
};

// ---- hotellingHourFraction -------------------------------------------------

const HOTELLING_HOUR_FRACTION_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("zoneID", Filter::Zone),
    ColumnDescriptor::new("dayID", Filter::Day),
    ColumnDescriptor::new("hourID", Filter::Hour),
    ColumnDescriptor::new("hourFraction", Filter::NonNegative),
];

const HOTELLING_HOUR_FRACTION_TABLE: TableDescriptor = TableDescriptor {
    name: "hotellingHourFraction",
    columns: HOTELLING_HOUR_FRACTION_COLUMNS,
    primary_key: &["zoneID", "dayID", "hourID"],
};

// ---- hotellingAgeFraction --------------------------------------------------

const HOTELLING_AGE_FRACTION_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("zoneID", Filter::Zone),
    ColumnDescriptor::new("ageID", Filter::Age),
    ColumnDescriptor::new("ageFraction", Filter::NonNegative),
];

const HOTELLING_AGE_FRACTION_TABLE: TableDescriptor = TableDescriptor {
    name: "hotellingAgeFraction",
    columns: HOTELLING_AGE_FRACTION_COLUMNS,
    primary_key: &["zoneID", "ageID"],
};

// ---- hotellingMonthAdjust --------------------------------------------------

const HOTELLING_MONTH_ADJUST_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("zoneID", Filter::Zone),
    ColumnDescriptor::new("monthID", Filter::Month),
    ColumnDescriptor::new("monthAdjustment", Filter::NonNegativeDefault1),
];

const HOTELLING_MONTH_ADJUST_TABLE: TableDescriptor = TableDescriptor {
    name: "hotellingMonthAdjust",
    columns: HOTELLING_MONTH_ADJUST_COLUMNS,
    primary_key: &["zoneID", "monthID"],
};

// ---- hotellingActivityDistribution -----------------------------------------

const HOTELLING_ACTIVITY_DISTRIBUTION_COLUMNS: &[ColumnDescriptor] = &[
    ColumnDescriptor::new("zoneID", Filter::Zone),
    ColumnDescriptor::new("fuelTypeID", Filter::FuelType),
    ColumnDescriptor::new("beginModelYearID", Filter::ModelYear),
    ColumnDescriptor::new("endModelYearID", Filter::ModelYear),
    ColumnDescriptor::new("opModeID", Filter::NonNegative),
    ColumnDescriptor::new("opModeFraction", Filter::ZeroToOne),
];

const HOTELLING_ACTIVITY_DISTRIBUTION_TABLE: TableDescriptor = TableDescriptor {
    name: "hotellingActivityDistribution",
    columns: HOTELLING_ACTIVITY_DISTRIBUTION_COLUMNS,
    primary_key: &[
        "zoneID",
        "fuelTypeID",
        "beginModelYearID",
        "endModelYearID",
        "opModeID",
    ],
};

// ---- all tables ------------------------------------------------------------

const TABLES: &[TableDescriptor] = &[
    HOTELLING_HOURS_PER_DAY_TABLE,
    HOTELLING_HOUR_FRACTION_TABLE,
    HOTELLING_AGE_FRACTION_TABLE,
    HOTELLING_MONTH_ADJUST_TABLE,
    HOTELLING_ACTIVITY_DISTRIBUTION_TABLE,
];

/// County-scale hotelling (extended-idle / APU) activity importer.
///
/// Ports `HotellingImporter.java` for county-domain runs.
#[derive(Debug, Default)]
pub struct HotellingImporter;

impl Importer for HotellingImporter {
    fn name(&self) -> &'static str {
        "Hotelling Activity"
    }

    fn xml_node_type(&self) -> &'static str {
        "hotelling"
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

        // Find each table by name.
        let find = |name: &str| tables.iter().find(|t| t.descriptor.name == name);

        // -- hotellingHourFraction: hourFraction sums to 1.0 per (zoneID, dayID) --
        if let Some(hf) = find("hotellingHourFraction") {
            let batch = &hf.batch;
            if let (Some(zone_col), Some(day_col), Some(frac_col)) = (
                batch.column_by_name("zoneID"),
                batch.column_by_name("dayID"),
                batch.column_by_name("hourFraction"),
            ) {
                if let (Some(zone), Some(day), Some(frac)) = (
                    zone_col.as_any().downcast_ref::<Int64Array>(),
                    day_col.as_any().downcast_ref::<Int64Array>(),
                    frac_col.as_any().downcast_ref::<Float64Array>(),
                ) {
                    let mut sums: std::collections::BTreeMap<(i64, i64), f64> =
                        std::collections::BTreeMap::new();
                    for i in 0..batch.num_rows() {
                        if zone.is_null(i) || day.is_null(i) || frac.is_null(i) {
                            continue;
                        }
                        *sums.entry((zone.value(i), day.value(i))).or_insert(0.0) += frac.value(i);
                    }
                    for ((z, d), sum) in &sums {
                        let rounded = (sum * 10_000.0).round() / 10_000.0;
                        if (rounded - 1.0).abs() > f64::EPSILON {
                            out.push(ValidationMessage::error(
                                "hotellingHourFraction",
                                Some("hourFraction"),
                                None,
                                format!(
                                    "total hourFraction for zone {z}, day {d} should be 1 but \
                                     instead is {rounded:.4}"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        // -- hotellingAgeFraction: ageFraction sums to 1.0 per zoneID --
        if let Some(af) = find("hotellingAgeFraction") {
            let batch = &af.batch;
            if let (Some(zone_col), Some(frac_col)) = (
                batch.column_by_name("zoneID"),
                batch.column_by_name("ageFraction"),
            ) {
                if let (Some(zone), Some(frac)) = (
                    zone_col.as_any().downcast_ref::<Int64Array>(),
                    frac_col.as_any().downcast_ref::<Float64Array>(),
                ) {
                    let mut sums: std::collections::BTreeMap<i64, f64> =
                        std::collections::BTreeMap::new();
                    for i in 0..batch.num_rows() {
                        if zone.is_null(i) || frac.is_null(i) {
                            continue;
                        }
                        *sums.entry(zone.value(i)).or_insert(0.0) += frac.value(i);
                    }
                    for (z, sum) in &sums {
                        let rounded = (sum * 10_000.0).round() / 10_000.0;
                        if (rounded - 1.0).abs() > f64::EPSILON {
                            out.push(ValidationMessage::error(
                                "hotellingAgeFraction",
                                Some("ageFraction"),
                                None,
                                format!(
                                    "total ageFraction for zone {z} should be 1 but instead is \
                                     {rounded:.4}"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        // -- hotellingActivityDistribution cross-row checks --
        if let Some(had) = find("hotellingActivityDistribution") {
            let batch = &had.batch;
            let fuel_col = batch
                .column_by_name("fuelTypeID")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>().map(|_| c));
            // opModeID uses Filter::NonNegative → Float64 storage; read and cast to i64.
            let op_mode_col = batch
                .column_by_name("opModeID")
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>().map(|_| c));
            let frac_col = batch
                .column_by_name("opModeFraction")
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>().map(|_| c));
            let begin_col = batch
                .column_by_name("beginModelYearID")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>().map(|_| c));
            let end_col = batch
                .column_by_name("endModelYearID")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>().map(|_| c));

            if let (Some(fuel_c), Some(op_mode_c), Some(frac_c), Some(begin_c), Some(end_c)) =
                (fuel_col, op_mode_col, frac_col, begin_col, end_col)
            {
                let fuel = fuel_c.as_any().downcast_ref::<Int64Array>().unwrap();
                let op_mode_f = op_mode_c.as_any().downcast_ref::<Float64Array>().unwrap();
                let frac = frac_c.as_any().downcast_ref::<Float64Array>().unwrap();
                let begin = begin_c.as_any().downcast_ref::<Int64Array>().unwrap();
                let end = end_c.as_any().downcast_ref::<Int64Array>().unwrap();

                for i in 0..batch.num_rows() {
                    if op_mode_f.is_null(i) {
                        continue;
                    }
                    let op = op_mode_f.value(i) as i64;
                    let ft = if fuel.is_null(i) { -1 } else { fuel.value(i) };
                    let fr = if frac.is_null(i) { 0.0 } else { frac.value(i) };
                    let bmy = if begin.is_null(i) { 0 } else { begin.value(i) };
                    let emy = if end.is_null(i) { 0 } else { end.value(i) };

                    // opModeID must be in {200, 201, 203, 204}
                    if !matches!(op, 200 | 201 | 203 | 204) {
                        out.push(ValidationMessage::error(
                            "hotellingActivityDistribution",
                            Some("opModeID"),
                            Some(i),
                            format!(
                                "Unknown opModeID ({op}). Hotelling operating modes are \
                                 200, 201, 203, and 204."
                            ),
                        ));
                    }

                    // No electricity (9) with extended idle (200) and non-zero fraction
                    if ft == 9 && op == 200 && fr != 0.0 {
                        out.push(ValidationMessage::error(
                            "hotellingActivityDistribution",
                            Some("opModeFraction"),
                            Some(i),
                            "Cannot use a non-zero opModeFraction for electricity \
                             (fuelTypeID 9) and extended idle (opModeID 200)"
                                .to_owned(),
                        ));
                    }

                    // No electricity with diesel APU (201)
                    if ft == 9 && op == 201 && fr != 0.0 {
                        out.push(ValidationMessage::error(
                            "hotellingActivityDistribution",
                            Some("opModeFraction"),
                            Some(i),
                            "Cannot use a non-zero opModeFraction for electricity \
                             (fuelTypeID 9) and diesel APU usage (opModeID 201)"
                                .to_owned(),
                        ));
                    }

                    // No CNG (3) with diesel APU (201)
                    if ft == 3 && op == 201 && fr != 0.0 {
                        out.push(ValidationMessage::error(
                            "hotellingActivityDistribution",
                            Some("opModeFraction"),
                            Some(i),
                            "Cannot use a non-zero opModeFraction for CNG \
                             (fuelTypeID 3) and diesel APU usage (opModeID 201)"
                                .to_owned(),
                        ));
                    }

                    // beginModelYearID <= endModelYearID
                    if bmy > emy {
                        out.push(ValidationMessage::error(
                            "hotellingActivityDistribution",
                            Some("beginModelYearID"),
                            Some(i),
                            format!("BeginModelYearID ({bmy}) must be <= EndModelYearID ({emy})"),
                        ));
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

    fn had_batch(rows: &[(i64, i64, i64, i64, i64, f64)]) -> arrow::record_batch::RecordBatch {
        // (zoneID, fuelTypeID, beginMY, endMY, opModeID, opModeFraction)
        // opModeID uses Filter::NonNegative → Float64; pass as f64.
        let mut z = Int64Builder::new();
        let mut ft = Int64Builder::new();
        let mut bmy = Int64Builder::new();
        let mut emy = Int64Builder::new();
        let mut op = Float64Builder::new();
        let mut fr = Float64Builder::new();
        for &(zone, fuel, b, e, mode, frac) in rows {
            z.append_value(zone);
            ft.append_value(fuel);
            bmy.append_value(b);
            emy.append_value(e);
            op.append_value(mode as f64);
            fr.append_value(frac);
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(z.finish()),
            Arc::new(ft.finish()),
            Arc::new(bmy.finish()),
            Arc::new(emy.finish()),
            Arc::new(op.finish()),
            Arc::new(fr.finish()),
        ];
        moves_importer::writer::build_record_batch_from_columns(
            &HOTELLING_ACTIVITY_DISTRIBUTION_TABLE,
            cols,
        )
        .unwrap()
    }

    #[test]
    fn valid_activity_distribution_passes() {
        let b = had_batch(&[
            (261610, 2, 1950, 2009, 200, 0.8),
            (261610, 2, 1950, 2009, 201, 0.0),
            (261610, 2, 1950, 2009, 203, 0.0),
            (261610, 2, 1950, 2009, 204, 0.2),
        ]);
        let imp = HotellingImporter;
        let tables: Vec<ImportedTable<'_>> = imp
            .tables()
            .iter()
            .map(|td| {
                if td.name == "hotellingActivityDistribution" {
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
    fn unknown_op_mode_is_flagged() {
        let b = had_batch(&[(261610, 2, 1950, 2009, 205, 1.0)]);
        let imp = HotellingImporter;
        let tables: Vec<ImportedTable<'_>> = imp
            .tables()
            .iter()
            .map(|td| {
                if td.name == "hotellingActivityDistribution" {
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

    #[test]
    fn inverted_model_year_range_is_flagged() {
        let b = had_batch(&[(261610, 2, 2010, 2009, 200, 0.8)]);
        let imp = HotellingImporter;
        let tables: Vec<ImportedTable<'_>> = imp
            .tables()
            .iter()
            .map(|td| {
                if td.name == "hotellingActivityDistribution" {
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
