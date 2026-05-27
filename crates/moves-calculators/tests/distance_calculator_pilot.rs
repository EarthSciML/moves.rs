//! Integration test: `DistanceCalculator::execute` end-to-end pilot (mo-ymv41).
//!
//! Builds a 5-row SHO store, wraps it in a [`CalculatorContext`], calls
//! [`execute`](moves_framework::Calculator::execute), and asserts the returned
//! `DataFrame` has exactly one row with the expected `activity` value.
//!
//! The five SHO rows all share the same vehicle parameters. Four of them
//! reference an off-network link (`roadTypeID = 1`) which the calculator's
//! `doesProcessContext` predicate drops; only the fifth (an urban-unrestricted
//! link, `roadTypeID = 4`) passes through and produces one output row with
//! `activity = 100.0` (distance 100 × fuel-type-activity fraction 1.0).

use moves_calculators::calculators::distance_calculator::{
    CountyRow, DistanceCalculator, HourDayRow, LinkRow, ShoRow, SourceBinDistributionRow,
    SourceBinRow, SourceTypeModelYearRow,
};
use moves_framework::{Calculator, CalculatorContext, DataFrameStore, InMemoryStore, TableRow};

// ── Shared synthetic parameters ───────────────────────────────────────────────

const SOURCE_TYPE_ID: i32 = 21; // passenger cars
const MODEL_YEAR_ID: i32 = 2018;
const YEAR_ID: i32 = 2020;
const AGE_ID: i32 = 2; // YEAR_ID - AGE_ID = MODEL_YEAR_ID
const MONTH_ID: i32 = 7;
const HOUR_DAY_ID: i32 = 85;
const DISTANCE: f64 = 100.0;

const SOURCE_BIN_ID: i64 = 1000;
const REG_CLASS_ID: i32 = 30;
const FUEL_TYPE_ID: i32 = 1;
const POL_PROCESS_ID: i32 = 101; // pollutant 1, process 1 (Running Exhaust)
const SOURCE_TYPE_MODEL_YEAR_ID: i32 = 210_018; // 21 * 10_000 + 2018

const DAY_ID: i32 = 5;
const HOUR_ID: i32 = 8;

const COUNTY_ID: i32 = 26_161;
const STATE_ID: i32 = 26;
const ZONE_ID: i32 = 261_610;

const LINK_OFF_NETWORK: i32 = 5000; // roadTypeID = 1, filtered by doesProcessContext
const LINK_VALID: i32 = 5001; // roadTypeID = 4, produces one output row

// ── Store builder ─────────────────────────────────────────────────────────────

fn build_store() -> InMemoryStore {
    let mut store = InMemoryStore::new();

    // Five SHO rows: four off-network (filtered) + one valid.
    let sho_rows: Vec<ShoRow> = (0..5)
        .map(|i| ShoRow {
            hour_day_id: HOUR_DAY_ID,
            month_id: MONTH_ID,
            year_id: YEAR_ID,
            age_id: AGE_ID,
            link_id: if i < 4 { LINK_OFF_NETWORK } else { LINK_VALID },
            source_type_id: SOURCE_TYPE_ID,
            distance: DISTANCE,
        })
        .collect();
    store.insert("SHO", ShoRow::into_dataframe(sho_rows).unwrap());

    // Two links: off-network and a valid urban-unrestricted link.
    let link_rows = vec![
        LinkRow {
            link_id: LINK_OFF_NETWORK,
            county_id: COUNTY_ID,
            zone_id: ZONE_ID,
            road_type_id: 1,
        },
        LinkRow {
            link_id: LINK_VALID,
            county_id: COUNTY_ID,
            zone_id: ZONE_ID,
            road_type_id: 4,
        },
    ];
    store.insert("Link", LinkRow::into_dataframe(link_rows).unwrap());

    // One county row.
    let county_rows = vec![CountyRow {
        county_id: COUNTY_ID,
        state_id: STATE_ID,
    }];
    store.insert("County", CountyRow::into_dataframe(county_rows).unwrap());

    // One HourDay row.
    let hour_day_rows = vec![HourDayRow {
        hour_day_id: HOUR_DAY_ID,
        day_id: DAY_ID,
        hour_id: HOUR_ID,
    }];
    store.insert(
        "HourDay",
        HourDayRow::into_dataframe(hour_day_rows).unwrap(),
    );

    // One SourceBin row.
    let source_bin_rows = vec![SourceBinRow {
        source_bin_id: SOURCE_BIN_ID,
        reg_class_id: REG_CLASS_ID,
        fuel_type_id: FUEL_TYPE_ID,
    }];
    store.insert(
        "SourceBin",
        SourceBinRow::into_dataframe(source_bin_rows).unwrap(),
    );

    // One SourceBinDistribution row (fraction 1.0 so activity == distance).
    let sbd_rows = vec![SourceBinDistributionRow {
        source_type_model_year_id: SOURCE_TYPE_MODEL_YEAR_ID,
        pol_process_id: POL_PROCESS_ID,
        source_bin_id: SOURCE_BIN_ID,
        source_bin_activity_fraction: 1.0,
    }];
    store.insert(
        "SourceBinDistribution",
        SourceBinDistributionRow::into_dataframe(sbd_rows).unwrap(),
    );

    // One SourceTypeModelYear row.
    let stmy_rows = vec![SourceTypeModelYearRow {
        source_type_model_year_id: SOURCE_TYPE_MODEL_YEAR_ID,
        source_type_id: SOURCE_TYPE_ID,
        model_year_id: MODEL_YEAR_ID,
    }];
    store.insert(
        "SourceTypeModelYear",
        SourceTypeModelYearRow::into_dataframe(stmy_rows).unwrap(),
    );

    store
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn execute_with_five_row_sho_produces_one_output_row() {
    let calc = DistanceCalculator::new();
    let ctx = CalculatorContext::with_tables(build_store());
    let out = calc.execute(&ctx).expect("execute should succeed");
    let df = out.dataframe().expect("output must contain a DataFrame");
    assert_eq!(
        df.height(),
        1,
        "four off-network rows filtered; one valid row produces one output"
    );
}

#[test]
fn execute_activity_matches_distance_times_fraction() {
    let calc = DistanceCalculator::new();
    let ctx = CalculatorContext::with_tables(build_store());
    let out = calc.execute(&ctx).expect("execute should succeed");
    let df = out.dataframe().expect("output must contain a DataFrame");
    let activity = df
        .column("activity")
        .expect("DataFrame must have 'activity' column")
        .f64()
        .expect("'activity' column must be f64")
        .get(0)
        .expect("row 0 must have a value");
    assert!(
        (activity - DISTANCE).abs() < 1e-9,
        "activity {activity} != expected {DISTANCE} (distance × fraction 1.0)"
    );
}

#[test]
fn execute_output_carries_correct_dimension_columns() {
    let calc = DistanceCalculator::new();
    let ctx = CalculatorContext::with_tables(build_store());
    let out = calc.execute(&ctx).expect("execute should succeed");
    let df = out.dataframe().expect("output must contain a DataFrame");

    let col_i32 = |name: &str| -> i32 {
        df.column(name)
            .unwrap_or_else(|_| panic!("column '{name}' missing"))
            .i32()
            .unwrap_or_else(|_| panic!("column '{name}' not i32"))
            .get(0)
            .unwrap_or_else(|| panic!("column '{name}' row 0 is null"))
    };

    assert_eq!(col_i32("yearID"), YEAR_ID);
    assert_eq!(col_i32("monthID"), MONTH_ID);
    assert_eq!(col_i32("dayID"), DAY_ID);
    assert_eq!(col_i32("hourID"), HOUR_ID);
    assert_eq!(col_i32("stateID"), STATE_ID);
    assert_eq!(col_i32("countyID"), COUNTY_ID);
    assert_eq!(col_i32("zoneID"), ZONE_ID);
    assert_eq!(col_i32("linkID"), LINK_VALID);
    assert_eq!(col_i32("regClassID"), REG_CLASS_ID);
    assert_eq!(col_i32("sourceTypeID"), SOURCE_TYPE_ID);
    assert_eq!(col_i32("fuelTypeID"), FUEL_TYPE_ID);
    assert_eq!(col_i32("modelYearID"), MODEL_YEAR_ID);
    assert_eq!(col_i32("roadTypeID"), 4);
}
