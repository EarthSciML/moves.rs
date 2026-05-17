//! Regression baseline for the Fuel Effects Generator (Task 40).
//!
//! Ports the database-independent test methods of
//! `gov/epa/otaq/moves/master/implementation/ghg/FuelEffectsGeneratorTest.java`
//! one-to-one, so the Rust port is pinned against the same assertions the
//! Java suite checked:
//!
//! | This file | Java test method |
//! |-----------|------------------|
//! | `cmp_expression_rewrite_matches_java` | `testRewriteCmpExpressionToIncludeStdDev` |
//! | `integer_set_renders_as_csv` | `testGetCSV` |
//! | `pol_process_ids_filtered_against_done_set` | `testGetPolProcessIDsNotAlreadyDone` |
//! | `general_fuel_ratio_evaluates_mtbe_expression` | `testDoGeneralFuelRatio` |
//!
//! `FuelEffectsGeneratorTest`'s remaining methods — `testLoadFuelModelIDs`,
//! `testLoadParameters`, `testAtDifferenceFraction`,
//! `testCreateComplexModelParameterVariables`, `testDoAirToxicsCalculations`,
//! `testDoCOCalculations`, `testDoHCCalculations`, `testDoNOxCalculations`,
//! `testMTBECalculations`, `testBuildOxyThreshCase` — all open a MariaDB
//! connection and exercise the predictive/complex-model paths. Those paths
//! and the data plane they read from are out of scope for this Phase 3
//! port (see the `fueleffectsgenerator` module docs); end-to-end
//! validation against canonical-MOVES captures is Task 44.

use std::collections::{BTreeMap, BTreeSet};

use moves_calculators::generators::fueleffectsgenerator::{
    do_general_fuel_ratio, get_csv, get_pol_process_ids_not_already_done,
    rewrite_cmp_expression_to_include_std_dev, FuelFormulation, GeneralFuelRatioExpression,
    GeneralFuelRatioInputs, IntegerPair,
};

/// The sixteen fuel-property names `testRewriteCmpExpressionToIncludeStdDev`
/// iterates over (the Java `names` array).
const NAMES: &[&str] = &[
    "Oxygen",
    "Sulfur",
    "RVP",
    "E200",
    "E300",
    "Aromatics",
    "Benzene",
    "Olefins",
    "MTBE",
    "ETBE",
    "TAME",
    "Ethanol",
    "Intercept",
    "Hi",
    "T50",
    "T90",
];

/// Ports `testRewriteCmpExpressionToIncludeStdDev`.
///
/// For each property name the Java test checks three rewrites — a single
/// centered term, the same with a trailing `^2`, and a product of two
/// centered terms — then two standalone cases whose centered value is an
/// `if(...)` call.
#[test]
fn cmp_expression_rewrite_matches_java() {
    for name in NAMES {
        // "(fp.E200-fp_E200.center)" -> "((fp.E200-fp_E200.center)/fp_E200.stddev)"
        let original = format!("(cmp.coeff*(fp.{name}-fp_{name}.center))");
        let expected = format!("(cmp.coeff*((fp.{name}-fp_{name}.center)/fp_{name}.stddev))");
        assert_eq!(
            rewrite_cmp_expression_to_include_std_dev(&original),
            expected,
            "single rewrite failed for {name}"
        );

        let original = format!("(cmp.coeff*(fp.{name}-fp_{name}.center)^2)");
        let expected = format!("(cmp.coeff*((fp.{name}-fp_{name}.center)/fp_{name}.stddev)^2)");
        assert_eq!(
            rewrite_cmp_expression_to_include_std_dev(&original),
            expected,
            "single rewrite with power failed for {name}"
        );

        let original = format!("(cmp.coeff*(fp.{name}-fp_{name}.center)*(fp.E200-fp_E200.center))");
        let expected = format!(
            "(cmp.coeff*((fp.{name}-fp_{name}.center)/fp_{name}.stddev)\
             *((fp.E200-fp_E200.center)/fp_E200.stddev))"
        );
        assert_eq!(
            rewrite_cmp_expression_to_include_std_dev(&original),
            expected,
            "multiple rewrite failed for {name}"
        );
    }

    // The two post-loop assertions: a centered term wrapping an if/and call.
    let original =
        "(cmp.coeff*(if(and(fp.Oxygen>3.5,maxModelYear<=2001),3.5,fp.Oxygen)-fp_Oxygen.center))";
    let expected = "(cmp.coeff*((if(and(fp.Oxygen>3.5,maxModelYear<=2001),3.5,fp.Oxygen)-fp_Oxygen.center)/fp_Oxygen.stddev))";
    assert_eq!(
        rewrite_cmp_expression_to_include_std_dev(original),
        expected,
        "complex rewrite failed"
    );

    let original =
        "(cmp.coeff*(if(and(fp.Oxygen>3.5,maxModelYear<=2001),3.5,fp.Oxygen)-fp_Oxygen.center)^2)";
    let expected = "(cmp.coeff*((if(and(fp.Oxygen>3.5,maxModelYear<=2001),3.5,fp.Oxygen)-fp_Oxygen.center)/fp_Oxygen.stddev)^2)";
    assert_eq!(
        rewrite_cmp_expression_to_include_std_dev(original),
        expected,
        "complex rewrite with power failed"
    );
}

/// Ports `testGetCSV`: an unordered set of integers renders as an
/// ascending comma-separated string.
#[test]
fn integer_set_renders_as_csv() {
    let set: BTreeSet<i32> = [5, 6, 1, 2].into_iter().collect();
    assert_eq!(get_csv(&set), "1,2,5,6");
}

/// Ports `testGetPolProcessIDsNotAlreadyDone`: each call returns the IDs
/// not seen before and folds them into the running set.
#[test]
fn pol_process_ids_filtered_against_done_set() {
    let mut ids_already_done: BTreeSet<i32> = BTreeSet::new();

    let got = get_pol_process_ids_not_already_done("101,102", &mut ids_already_done);
    assert_eq!(got.as_deref(), Some("101,102"));

    // Nothing unique on the second pass — Java returns null.
    let got = get_pol_process_ids_not_already_done("101,102", &mut ids_already_done);
    assert_eq!(got, None);

    let got = get_pol_process_ids_not_already_done("201,202,101,103", &mut ids_already_done);
    assert_eq!(got.as_deref(), Some("201,202,103"));
}

/// The fuel formulation `MTBEFuelHandler` inserts in `testDoGeneralFuelRatio`.
///
/// The Java handler picks `testFuelID = 1 + max(fuelFormulationID)`; the
/// exact value is immaterial to the arithmetic, so this port fixes it at
/// `100`. The property columns are the literal values from the Java
/// `insert into fuelFormulation` statement.
fn mtbe_test_fuel() -> FuelFormulation {
    FuelFormulation {
        fuel_formulation_id: 100,
        fuel_subtype_id: 10,
        vol_to_wt_percent_oxy: 3.529_924,
        sulfur_level: 215.96,
        rvp: 8.7,
        e200: 50.175_62,
        e300: 82.612_2,
        aromatic_content: 34.336_8,
        olefin_content: 7.094_4,
        benzene_content: 1.4,
        mtbe_volume: 10.0,
        ..FuelFormulation::default()
    }
}

/// Ports `testDoGeneralFuelRatio`.
///
/// The Java test inserts a single `generalFuelRatioExpression` row for
/// fuel type 1 / `polProcessID = -101` whose ratio expression is
/// `MTBEVolume+7` and whose GPA expression is `MTBEVolume*2`, runs
/// `doGeneralFuelRatio` against a fuel with `MTBEVolume = 10`, and asserts
/// the resulting `generalFuelRatio` row carries `fuelEffectRatio = 17.0`
/// and `fuelEffectRatioGPA = 20.0`.
///
/// `MTBEVolume = 10` is exactly representable, so `10 + 7` and `10 * 2`
/// are exact in `f64`; the Java test's truncation to four decimals
/// (`((int)(ratio*10000))/10000.0`) is therefore unnecessary here.
#[test]
fn general_fuel_ratio_evaluates_mtbe_expression() {
    let expression = GeneralFuelRatioExpression::new(
        1,    // fuelTypeID
        -101, // polProcessID
        1960, // minModelYearID
        2060, // maxModelYearID
        0,    // minAgeID
        30,   // maxAgeID
        0,    // sourceTypeID
        "MTBEVolume+7",
        "MTBEVolume*2",
    );
    let fuel = mtbe_test_fuel();
    let inputs = GeneralFuelRatioInputs {
        expressions: vec![expression],
        // getFuelFormulations(1) -> the test fuel.
        formulations_by_fuel_type: BTreeMap::from([(1, vec![fuel.clone()])]),
        // getFuelSupplyFormulations(1) -> the test fuel is in the supply.
        supplied_by_fuel_type: BTreeMap::from([(1, BTreeSet::from([fuel.fuel_formulation_id]))]),
        // GeneralFuelRatio starts empty.
        already_ratioed: BTreeSet::new(),
    };

    let rows = do_general_fuel_ratio(&inputs).expect("general fuel ratio evaluates");

    assert_eq!(rows.len(), 1, "exactly one generalFuelRatio row");
    let row = &rows[0];
    assert_eq!(row.fuel_type_id, 1);
    assert_eq!(row.fuel_formulation_id, 100);
    assert_eq!(row.pol_process_id, -101);
    // fuelEffectRatio = MTBEVolume + 7 = 10 + 7.
    assert_eq!(
        row.fuel_effect_ratio, 17.0,
        "fuelEffectRatio does not match"
    );
    // fuelEffectRatioGPA = MTBEVolume * 2 = 10 * 2.
    assert_eq!(
        row.fuel_effect_ratio_gpa, 20.0,
        "fuelEffectRatioGPA does not match"
    );

    // Running it again with the row recorded yields nothing new — the
    // Java test's "2nd General run with logic to skip existing ratios".
    let inputs_second_pass = GeneralFuelRatioInputs {
        already_ratioed: BTreeSet::from([IntegerPair::new(
            Some(row.fuel_formulation_id),
            Some(row.pol_process_id),
        )]),
        ..inputs
    };
    let rows = do_general_fuel_ratio(&inputs_second_pass).expect("second pass evaluates");
    assert!(rows.is_empty(), "already-ratioed formulation is skipped");
}
