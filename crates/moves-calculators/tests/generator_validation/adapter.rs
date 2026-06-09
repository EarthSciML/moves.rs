//! Generator compute-core output → `moves_snapshot` table.
//!
//! A generator's [`Generator::execute`](moves_framework::Generator)
//! returns `CalculatorOutput::empty()` until the data plane
//! lands — but the generator's *numeric compute core* is complete and
//! callable today. This module is the contract that bridges the two:
//! it runs a real generator compute core and shapes its output into
//! the [`moves_snapshot::Table`] the canonical-capture diff consumes.
//!
//! When the data plane lands, the per-fixture materialisation feeds
//! each generator its inputs and collects its rows; the row → table
//! conversion here is what it builds to, so the diff
//! ([`super::compare`]) activates with no change to the comparison
//! machinery.
//!
//! # Live-port exercise: `MeteorologyGenerator`
//!
//! The module ports one generator end to end as the worked example
//! and the harness's live-port exercise: [`run_meteorology`] calls
//! the real [`compute_zone_month_hour`] and builds the `ZoneMonthHour`
//! table — the table `MeteorologyGenerator::output_tables` names.
//! Each remaining generator gets an analogous adapter function as the
//! data plane lands; the shape is the same — run the compute core,
//! push one snapshot row per output row.

use moves_snapshot::{ColumnKind, Table, TableBuilder, Value};

use moves_calculators::generators::meteorology::{
    compute_zone_month_hour, ZoneMonthHourInputs, ZoneMonthHourMeteorology,
};

/// The `(zoneID, monthID, hourID)` natural key of one `ZoneMonthHour`
/// row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoneMonthHourKey {
    /// `ZoneMonthHour.zoneID`.
    pub zone_id: i64,
    /// `ZoneMonthHour.monthID`.
    pub month_id: i64,
    /// `ZoneMonthHour.hourID`.
    pub hour_id: i64,
}

/// One `ZoneMonthHour` cell: its key and the raw meteorology inputs
/// `MeteorologyGenerator` reads for that row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneMonthHourCell {
    /// The row's `(zone, month, hour)` key.
    pub key: ZoneMonthHourKey,
    /// The temperature / humidity / pressure inputs.
    pub inputs: ZoneMonthHourInputs,
}

/// The snapshot-table name `MeteorologyGenerator` writes — matches
/// its `Generator::output_tables` entry.
pub const ZONE_MONTH_HOUR_TABLE: &str = "ZoneMonthHour";

/// Build the `ZoneMonthHour` snapshot table from
/// `MeteorologyGenerator` compute-core output: the `(zone, month,
/// hour)` natural key plus the three columns the generator writes
/// (`heatIndex`, `specificHumidity`, `molWaterFraction`).
///
/// # Errors
///
/// Propagates a [`moves_snapshot::Error`] only on a schema-shape bug
/// a duplicate column or a row-width mismatch — neither of which a
/// well-formed call can hit.
pub fn zone_month_hour_table(
    rows: &[(ZoneMonthHourKey, ZoneMonthHourMeteorology)],
) -> Result<Table, moves_snapshot::Error> {
    let mut builder = TableBuilder::new(
        ZONE_MONTH_HOUR_TABLE,
        [
            ("zoneID".to_string(), ColumnKind::Int64),
            ("monthID".to_string(), ColumnKind::Int64),
            ("hourID".to_string(), ColumnKind::Int64),
            ("heatIndex".to_string(), ColumnKind::Float64),
            ("specificHumidity".to_string(), ColumnKind::Float64),
            ("molWaterFraction".to_string(), ColumnKind::Float64),
        ],
    )?
    .with_natural_key(["zoneID", "monthID", "hourID"])?;

    for (key, met) in rows {
        builder.push_row([
            Value::Int64(key.zone_id),
            Value::Int64(key.month_id),
            Value::Int64(key.hour_id),
            Value::Float64(met.heat_index),
            Value::Float64(met.specific_humidity),
            Value::Float64(met.mol_water_fraction),
        ])?;
    }

    builder.build()
}

/// Run `MeteorologyGenerator`'s compute core over `cells` and shape
/// the result into the `ZoneMonthHour` snapshot table.
///
/// This is the harness's live-port exercise: the real
/// [`compute_zone_month_hour`] is invoked once per cell and its
/// output routed into the comparison machinery.
///
/// # Errors
///
/// Propagates a [`moves_snapshot::Error`] from [`zone_month_hour_table`].
pub fn run_meteorology(cells: &[ZoneMonthHourCell]) -> Result<Table, moves_snapshot::Error> {
    let rows: Vec<(ZoneMonthHourKey, ZoneMonthHourMeteorology)> = cells
        .iter()
        .map(|cell| (cell.key, compute_zone_month_hour(cell.inputs)))
        .collect();
    zone_month_hour_table(&rows)
}

/// A small, fixed grid of `ZoneMonthHour` cells spanning the heat-index
/// regimes — below the 78 °F threshold, above it, and a humid hot
/// cell — for the harness's live-port exercise.
pub fn sample_cells() -> Vec<ZoneMonthHourCell> {
    let cell = |zone, month, hour, temperature_f, rel_humidity, pressure| ZoneMonthHourCell {
        key: ZoneMonthHourKey {
            zone_id: zone,
            month_id: month,
            hour_id: hour,
        },
        inputs: ZoneMonthHourInputs {
            temperature_f,
            rel_humidity,
            barometric_pressure_inhg: pressure,
        },
    };
    vec![
        // Cool January morning — below the heat-index threshold.
        cell(260000, 1, 6, 35.0, 70.0, 28.94),
        // Mild April afternoon — still below threshold.
        cell(260000, 4, 14, 62.0, 45.0, 28.94),
        // Hot, humid July afternoon — NWS regression regime.
        cell(260000, 7, 14, 95.0, 65.0, 28.94),
        // Hot, dry July afternoon at high altitude.
        cell(80000, 7, 14, 99.0, 15.0, 24.59),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zone_month_hour_table_has_the_expected_shape() {
        let table = run_meteorology(&sample_cells()).expect("table builds");
        assert_eq!(table.name(), ZONE_MONTH_HOUR_TABLE);
        assert_eq!(table.row_count(), sample_cells().len());
        assert_eq!(table.natural_key(), ["zoneID", "monthID", "hourID"]);
        // Six columns: the three-part key plus the three written columns.
        assert_eq!(table.schema().len(), 6);
        for column in ["heatIndex", "specificHumidity", "molWaterFraction"] {
            assert!(
                table.column_index(column).is_some(),
                "missing generator-written column `{column}`"
            );
        }
    }

    #[test]
    fn meteorology_compute_core_produces_finite_output() {
        for cell in sample_cells() {
            let met = compute_zone_month_hour(cell.inputs);
            assert!(met.heat_index.is_finite(), "heat_index non-finite");
            assert!(
                met.specific_humidity.is_finite(),
                "specific_humidity non-finite"
            );
            assert!(
                met.mol_water_fraction.is_finite(),
                "mol_water_fraction non-finite"
            );
        }
    }

    #[test]
    fn below_threshold_heat_index_is_the_temperature() {
        // The cool-morning cell sits below 78 °F, so heatIndex == temperature.
        let cells = sample_cells();
        let cool = cells[0];
        let met = compute_zone_month_hour(cool.inputs);
        assert_eq!(met.heat_index, cool.inputs.temperature_f);
    }

    #[test]
    fn empty_input_builds_an_empty_table() {
        let table = run_meteorology(&[]).expect("empty table builds");
        assert_eq!(table.row_count(), 0);
        assert_eq!(table.schema().len(), 6);
    }
}
